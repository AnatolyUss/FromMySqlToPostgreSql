<?php
/*
 * This file is a part of "FromMySqlToPostgreSql" - the database migration tool.
 * 
 * Copyright 2015 Anatoly Khaytovich <anatolyuss@gmail.com>
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program (please see the "LICENSE.md" file).  
 * If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

/**
 * This class performs structure and data migration from MySql database to PostgreSql database.
 * 
 * @author Anatoly Khaytovich
 */
class FromMySqlToPostgreSql
{
    /**
     * A \PDO instance, connected to MySql server.
     * 
     * @var \PDO
     */
    private $mysql;
    
    /**
     * A \PDO instance, connected to PostgreSql server.
     * 
     * @var \PDO
     */
    private $pgsql;
    
    /**
     * Encoding of target (PostgreSql) server.
     * 
     * @var string
     */
    private $strEncoding;
    
    /**
     * A shema name.
     * 
     * @var string
     */
    private $strSchema;
    
    /**
     * MySql connection string.
     * 
     * @var string
     */
    private $strSourceConString;
    
    /**
     * PostgreSql connection string.
     * 
     * @var string
     */
    private $strTargetConString;
    
    /**
     * A name of MySql database, that will be migrated.
     *  
     * @var string
     */
    private $strMySqlDbName;
    
    /**
     * An array of MySql tables, that need to be migrated.
     * 
     * @var array
     */
    private $arrTablesToMigrate;
    
    /**
     * Path to errors log file.
     * 
     * @var string
     */
    private $strWriteErrorLogTo;
    
    /**
     * Path to temporary directory.
     * 
     * @var string
     */
    private $strTemporaryDirectory;
    
    /**
     * Path to summary report file.
     * 
     * @var string
     */
    private $strWriteSummaryReportTo;
    
    /**
     * Summary report array.
     * 
     * @var array
     */
    private $arrSummaryReport;
    
    /**
     * Path to common logs file.
     * 
     * @var string
     */
    private $strWriteCommonLogTo;
    
    /**
     * File pointer of "error-log" file.
     * 
     * @var resource
     */
    private $resourceErrorLog;
    
    /**
     * File pointer to "common-log" file.
     * 
     * @var resource
     */
    private $resourceCommonLog;
    
    /**
     * Extract database name from given query-string.
     * 
     * @param  string $strConString
     * @return string
     */
    private function extractDbName($strConString)
    {
        $strRetVal  = '';
        $arrParams  = explode(',', $strConString);
        $arrParams2 = explode(';', $arrParams[0]);
        
        foreach ($arrParams2 as $strPair) {
            $arrPair = explode('=', $strPair);
            
            if ('dbname' == $arrPair[0]) {
                $strRetVal = $arrPair[1];
                unset($strPair);
                break;
            }
            unset($strPair);
        }
        unset($arrParams, $arrParams2);
        return $strRetVal;
    }
    
    /**
     * Constructor.
     * 
     * @param array $arrConfig
     */
    public function __construct(array $arrConfig)
    {
        if (!isset($arrConfig['source'])) {
            echo PHP_EOL, '-- Cannot perform a migration due to missing source database (MySql) connection string.', PHP_EOL, 
                 '-- Please, specify source database (MySql) connection string, and run the tool again.', PHP_EOL;
            
            exit;
        }
        
        if (!isset($arrConfig['target'])) {
            echo PHP_EOL, '-- Cannot perform a migration due to missing target database (PostgreSql) connection string.', PHP_EOL, 
                 '-- Please, specify target database (PostgreSql) connection string, and run the tool again.', PHP_EOL;
            
            exit;
        }
        
        if (!isset($arrConfig['temporary_directory'])) {
            echo PHP_EOL, '-- Cannot perform a migration due to undefined "temporary_directory".', PHP_EOL,
                 '-- Please, define "temporary_directory", and run the tool again.', PHP_EOL;
            exit;
        }

        if (!file_exists($arrConfig['temporary_directory'])) {
            @mkdir($arrConfig['temporary_directory'], 0750, true);
            if (!file_exists($arrConfig['temporary_directory'])) {
                echo PHP_EOL, '-- Cannot perform a migration due to impossibility to create "temporary_directory" : ' . $arrConfig['temporary_directory'], PHP_EOL;
                exit;
            }
        }

        $this->arrTablesToMigrate      = [];
        $this->arrSummaryReport        = [];
        $this->strTemporaryDirectory   = $arrConfig['temporary_directory'];
        $this->strWriteCommonLogTo     = isset($arrConfig['write_common_log_to']) ? $arrConfig['write_common_log_to'] : '';
        $this->strWriteSummaryReportTo = isset($arrConfig['write_summary_report_to']) ? $arrConfig['write_summary_report_to'] : '';
        $this->strWriteErrorLogTo      = isset($arrConfig['write_error_log_to']) ? $arrConfig['write_error_log_to'] : '';
        $this->strEncoding             = isset($arrConfig['encoding']) ? $arrConfig['encoding'] : 'UTF-8';
        $this->strSourceConString      = $arrConfig['source'];
        $this->strTargetConString      = $arrConfig['target'];
        $this->mysql                   = null;
        $this->pgsql                   = null;
        $this->strMySqlDbName          = $this->extractDbName($this->strSourceConString);
        $this->strSchema               = $this->strMySqlDbName;
        
        if (!empty($this->strWriteErrorLogTo)) {
            $this->resourceErrorLog = fopen($this->strWriteErrorLogTo, 'a');
        }
        
        if (!empty($this->strWriteCommonLogTo)) {
            $this->resourceCommonLog = fopen($this->strWriteCommonLogTo, 'a');
        }
    }
    
    /**
     * Destructor.
     */
    public function __destruct()
    {
        $this->mysql = null;
        $this->pgsql = null;
        
        if (is_resource($this->resourceErrorLog)) {
            fclose($this->resourceErrorLog);
        }
        
        if (is_resource($this->resourceCommonLog)) {
            fclose($this->resourceCommonLog);
        }
    }
    
    /**
     * Check if both servers are connected.
     * If not, then create connections.
     * 
     * @param  void
     * @return void
     */
    private function connect()
    {
        if (empty($this->mysql)) {
            $arrSrcInput = explode(',', $this->strSourceConString);
            $this->mysql = new \PDO($arrSrcInput[0], $arrSrcInput[1], $arrSrcInput[2]);
            $this->mysql->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);
            $this->mysql->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        }
        
        if (empty($this->pgsql)) {
            $arrDestInput = explode(',', $this->strTargetConString);
            $this->pgsql  = new \PDO($arrDestInput[0], $arrDestInput[1], $arrDestInput[2]);
            $this->pgsql->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);
            $this->pgsql->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        }
    }
    
    /**
     * Outputs given log.
     * Writes given string to the "common logs" file.
     * 
     * @param  string $strLog
     * @param  bool   $boolIsError
     * @return void
     */
    private function log($strLog, $boolIsError = false)
    {
        if (!$boolIsError) {
            echo $strLog;
        }
        
        if (!empty($this->strWriteCommonLogTo)) {
            if (is_resource($this->resourceCommonLog)) {
                fwrite($this->resourceCommonLog, $strLog);
            } else {
                $this->resourceCommonLog = fopen($this->strWriteCommonLogTo, 'a');
                
                if (is_resource($this->resourceCommonLog)) {
                    fwrite($this->resourceCommonLog, $strLog);
                }
            }
        }
    }
    
    /**
     * Writes a ditailed error message to the log file, if specified.
     * 
     * @param  \PDOException $e
     * @param  string $strMessage
     * @return void
     */
    private function generateError(\PDOException $e, $strMessage)
    {
        $strError = PHP_EOL . "\t-- " . $strMessage . PHP_EOL
                  . "\t-- PDOException code: " . $e->getCode() . PHP_EOL
                  . "\t-- File: " . $e->getFile() . PHP_EOL
                  . "\t-- Line: " . $e->getLine() . PHP_EOL
                  . "\t-- Message: " . $e->getMessage() 
                  . PHP_EOL
                  . "\t-------------------------------------------------------"
                  . PHP_EOL . PHP_EOL;
        
        $this->log($strError, true);
        
        if (!empty($this->strWriteErrorLogTo)) {
            if (is_resource($this->resourceErrorLog)) {
                fwrite($this->resourceErrorLog, $strError);
            } else {
                $this->resourceErrorLog = fopen($this->strWriteErrorLogTo, 'a');
                
                if (is_resource($this->resourceErrorLog)) {
                    fwrite($this->resourceErrorLog, $strError);
                }
            }
        }
        unset($strError);
    }
    
    /**
     * Load MySql tables, that need to be migrated into an array.
     * 
     * @param  void
     * @return bool
     */
    private function loadTablesToMigrate()
    {
        $boolRetVal = false;
        
        try {
            $this->connect();
            $sql                      = 'SHOW TABLES FROM `' . $this->strMySqlDbName . '`;';
            $stmt                     = $this->mysql->query($sql);
            $this->arrTablesToMigrate = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            $boolRetVal               = true;
            unset($sql, $stmt);
            
        } catch (\PDOException $e) {
            $this->generateError($e, __METHOD__ . PHP_EOL . "\t" . '-- Cannot load tables from source (MySql) database...');
        }
        return $boolRetVal;
    }
    
    /**
     * Create a new database schema.
     * Insure a uniqueness of a new schema name.
     * 
     * @param  void
     * @return bool
     */
    private function createSchema()
    {
        $boolRetVal = false;
        
        try {
            $this->connect();
            
            for ($i = 1; true; $i++) {
                $this->strSchema .= $i;
                $sql              = "SELECT schema_name FROM information_schema.schemata "
                                  . "WHERE schema_name = '" . $this->strSchema . "';";
                
                $stmt       = $this->pgsql->query($sql);
                $arrSchemas = $stmt->fetchAll(\PDO::FETCH_ASSOC);
                
                if (empty($arrSchemas)) {
                    unset($sql, $arrSchemas, $stmt);
                    break;
                } else {
                    unset($sql, $arrSchemas, $stmt);
                }
            }
            
            $sql        = 'CREATE SCHEMA ' . $this->strSchema . ';';
            $stmt       = $this->pgsql->query($sql);
            $boolRetVal = true;
            unset($sql, $stmt);
            
        } catch (\PDOException $e) {
            $this->generateError($e, __METHOD__ . PHP_EOL . "\t" . '-- Cannot create a new schema...');
        }
        return $boolRetVal;
    }
    
    /**
     * Migrate structure of a single table to PostgreSql server.
     * 
     * @param  string $strTableName
     * @return bool
     */
    private function createTable($strTableName)
    {
        $boolRetVal = false;
        
        try {
            $this->log(PHP_EOL . '-- Currently processing table: ' . $strTableName . '...' . PHP_EOL);
            $this->connect();
            
            $sql        = 'SHOW COLUMNS FROM `' . $strTableName . '`;';
            $stmt       = $this->mysql->query($sql);
            $arrColumns = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            unset($sql, $stmt);
            
            $strSqlCreateTable = 'CREATE TABLE ' . $this->strSchema . '."' . $strTableName . '"(';
            
            foreach ($arrColumns as $arrColumn) {
                $strSqlCreateTable .= '"' . $arrColumn['Field'] . '" ' . \MapDataTypes::map($arrColumn['Type']) . ',';
                unset($arrColumn);
            }
            
            $strSqlCreateTable = substr($strSqlCreateTable, 0, -1) . ');';
            $stmt              = $this->pgsql->query($strSqlCreateTable);
            $boolRetVal        = true;
            
            unset($strSqlCreateTable, $stmt, $arrColumns);
            $this->log("\t-- Table " . $this->strSchema . '."' . $strTableName . '" ' . 'is created.' . PHP_EOL);
            
        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Cannot create table ' . $this->strSchema . '."' .  $strTableName .  '".';
            $this->generateError($e, $strMsg);
            unset($strMsg);
        }
        return $boolRetVal;
    }
    
    /**
     * Sanitize an input value.
     * 
     * @param  string $strValue
     * @return string
     */
    private function sanitizeValue($strValue)
    {
        switch ($strValue) {
            case '0000-00-00 00:00:00':
                return '1970-01-01 00:00:00';
                
            case '0000-00-00':
                return '1970-01-01';
                
            default:
                return $strValue;
        }
    }
    
    /**
     * Populates given table using "prepared statments".
     * 
     * @param  array  &$arrRows
     * @param  string  $strTableName
     * @param  int     $intStartInsertionsFromIndex
     * @param  int     $intTotalRowsToInsert
     * @param  int     $intRowsInserted
     * @return int
     */
    private function populateTableByPrepStmt(
        array &$arrRows,
        $strTableName,
        $intStartInsertionsFromIndex = 0,
        $intTotalRowsToInsert        = 0,
        $intRowsInserted             = 0
    ) {
        while ($intStartInsertionsFromIndex < $intTotalRowsToInsert) {
            $intRowsInserted = $this->populateTableByPrepStmtWorker(
                $arrRows, 
                $strTableName,
                $intStartInsertionsFromIndex, 
                $intRowsInserted
            );
        }
        
        return $intRowsInserted;
    }
    
    /**
     * Populates given table using "prepared statments" (worker).
     * 
     * @param  array  &$arrRows
     * @param  string  $strTableName
     * @param  int    &$intStartInsertionsFromIndex
     * @param  int     $intRowsInserted
     * @return int
     */
    private function populateTableByPrepStmtWorker(
        array &$arrRows,
        $strTableName,
        &$intStartInsertionsFromIndex,
        $intRowsInserted
    ) {
        try {
            $this->connect();
            $strColumns = '(';
            $strValues  = '(';
            $strInsert  = 'INSERT INTO ' . $this->strSchema . '."' . $strTableName . '" ';
            
            foreach ($arrRows[$intStartInsertionsFromIndex] as $strColumn => $unusedValue) {
                $strColumns .= '"'  . $strColumn  . '",';
                $strValues  .= ' :' . $strColumn  . ',';
                unset($strColumn, $unusedValue);
            }
            
            $strColumns      = substr($strColumns, 0, -1) . ') ';
            $strValues       = substr($strValues, 0, -1)  . ');';
            $strInsert      .= $strColumns . ' VALUES' . $strValues;
            $stmtInsert      = $this->pgsql->prepare($strInsert);
            $arrRowsPortion  = array_slice($arrRows, $intStartInsertionsFromIndex);
            
            foreach ($arrRowsPortion as $arrRow) {
                foreach ($arrRow as $strColumn => $value) {
                    if (is_null($value)) {
                        $stmtInsert->bindValue(':' . $strColumn, $value, \PDO::PARAM_NULL);
                    } elseif (is_bool($value)) {
                        $stmtInsert->bindValue(':' . $strColumn, $value, \PDO::PARAM_BOOL);
                    } elseif (is_numeric($value)) {
                        $stmtInsert->bindValue(':' . $strColumn, $value, \PDO::PARAM_INT);
                    } elseif (is_resource($value)) {
                        $stmtInsert->bindValue(':' . $strColumn, $value, \PDO::PARAM_LOB);
                    } else {
                        $strFiltered = $value;
                        $strFiltered = '0000-00-00 00:00:00' == $strFiltered 
                                     ? '1970-01-01 00:00:00'
                                     : str_replace("'", "''", $strFiltered);
                        
                        if (mb_check_encoding($strFiltered, $this->strEncoding)) {
                            $stmtInsert->bindValue(':' . $strColumn, $strFiltered, \PDO::PARAM_STR);
                        } else {
                            $strFiltered = mb_convert_encoding($strFiltered, $this->strEncoding);
                            $stmtInsert->bindValue(':' . $strColumn, $strFiltered, \PDO::PARAM_STR);
                            
                            if (!mb_check_encoding($strFiltered, $this->strEncoding)) {
                                unset($strColumn, $value);
                                continue;
                            }
                        }
                    }
                    
                    unset($strColumn, $value);
                }
                
                $intStartInsertionsFromIndex++;
                
                if ($stmtInsert->execute()) {
                    $intRowsInserted++;
                    echo "\t-- For now inserted: $intRowsInserted rows\r";
                } else {
                    return $intRowsInserted;
                }
                unset($arrRow);
            }
            
            unset($stmtInsert, $strInsert, $strColumns, $strValues);
            
        } catch (\PDOException $e) {
             $strMsg = __METHOD__ . PHP_EOL;
            
            $this->generateError($e, $strMsg);
            unset($strMsg);
            return $intRowsInserted;
        }
        
        return $intRowsInserted;
    }
    
    /**
     * Populate given table using "PostgreSql COPY".
     * 
     * @param  string $strTableName
     * @return int
     */
    private function populateTable($strTableName)
    {
        $strAddrCsv = $this->strTemporaryDirectory . '/' . $strTableName . '.csv';
        $intRetVal  = 0;
        $intRowsCnt = 0;
        $arrRows    = [];
        
        try {
            $this->log("\t" . '-- Populating table ' . $this->strSchema . '."' . $strTableName . '" ' . PHP_EOL);
            $this->connect();
            
            $sql        = 'SELECT * FROM `' . $strTableName . '`;';
            $stmt       = $this->mysql->query($sql);
            $arrRows    = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            $intRowsCnt = count($arrRows);
            unset($sql, $stmt);
            
            $this->log(
                "\t" . '-- Total rows to insert into ' . $this->strSchema . '."' 
                . $strTableName . '": ' . $intRowsCnt . PHP_EOL
            );
            
            $resourceCsv = fopen($strAddrCsv, 'w');
            
            foreach ($arrRows as $arrRow) {
                $boolValidCsvEntity  = true;
                $arrSanitizedCsvData = [];
                
                foreach ($arrRow as $value) {
                    $strSanitizedValue = $this->sanitizeValue($value);
                    
                    if (mb_check_encoding($strSanitizedValue, $this->strEncoding)) {
                        $arrSanitizedCsvData[] = $strSanitizedValue;
                    } else {
                        $strSanitizedValue = mb_convert_encoding($strSanitizedValue, $this->strEncoding);
                        
                        if (mb_check_encoding($strSanitizedValue, $this->strEncoding)) {
                            $arrSanitizedCsvData[] = $strSanitizedValue;
                        } else {
                            $boolValidCsvEntity = false;
                        }
                    }
                    
                    unset($value, $strSanitizedValue);
                }
                
                if ($boolValidCsvEntity) {
                    fputcsv($resourceCsv, $arrSanitizedCsvData);
                }
                
                unset($arrRow, $arrSanitizedCsvData, $boolValidCsvEntity);
            }
            
            fclose($resourceCsv);
            unset($resourceCsv);
            
            $sql  = "COPY " . $this->strSchema . ".\"" . $strTableName . "\" FROM '" . $strAddrCsv . "' DELIMITER ',' CSV;";
            $stmt = $this->pgsql->query($sql);
            
            if (false === $stmt) {
                $this->log("\t" . '-- Failed to populate table: ' . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL);
            }
            
            $intRetVal = count($stmt->fetchAll(\PDO::FETCH_ASSOC));
            $this->log("\t-- For now inserted: " . $intRetVal . ' rows' . PHP_EOL);
            
            if ($intRowsCnt != 0 && 0 == $intRetVal) {
                /**
                 * In most cases (~100%) the control will not get here.
                 * Perform given table population using prepared statment.
                 */
                $intRetVal = $this->populateTableByPrepStmt($arrRows, $strTableName, 0, $intRowsCnt, 0);
            }
            
            unset($sql, $stmt);
            
        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL;
            $this->generateError($e, $strMsg);
            unset($strMsg);
            
            /**
             * If the control got here, then no (usable) rows were inserted.
             * Perform given table population using prepared statment.
             */
            $intRetVal = $this->populateTableByPrepStmt($arrRows, $strTableName, 0, $intRowsCnt, 0);
        }
        
        unlink($strAddrCsv);
        unset($strAddrCsv, $intRowsCnt, $arrRows);
        echo PHP_EOL, PHP_EOL;
        return $intRetVal;
    }
    
    /**
     * Define which columns of the given table can contain the "NULL" value.
     * Set an appropriate constraint, if need.
     * 
     * @param  string $strTableName
     * @param  array  $arrColumns
     * @return void
     */
    private function processNull($strTableName, array $arrColumns)
    {
        try {
            $this->log(
                PHP_EOL . "\t" . '-- Define "NULLs" for table: ' . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL
            );
            $this->connect();
            
            foreach ($arrColumns as $arrColumn) {
                if ('no' == strtolower($arrColumn['Null'])) {
                    $sql = 'ALTER TABLE ' . $this->strSchema . '."' . $strTableName 
                         . '" ALTER COLUMN "' . $arrColumn['Field'] . '" SET NOT NULL;';
                    
                    $stmt = $this->pgsql->query($sql);
                    unset($sql, $stmt);
                }
                
                unset($arrColumn);
            }
            
            $this->log("\t-- Done." . PHP_EOL);
            
        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Cannot define "NULLs" for table: ' . $this->strSchema 
                    . '."' . $strTableName . '"...' . PHP_EOL;
            
            $this->generateError($e, $strMsg);
            unset($strMsg);
        }
    }
    
    /**
     * Define which columns of the given table have default value.
     * Set default values, if need.
     * 
     * @param  string $strTableName
     * @param  array  $arrColumns
     * @return void
     */
    private function processDefault($strTableName, array $arrColumns)
    {
        try {
            $this->log(
                PHP_EOL . "\t" . '-- Set default values for table: ' 
                . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL
            );
            
            $this->connect();
            $arrSqlReservedValues = [
                'CURRENT_DATE'        => 'CURRENT_DATE',
                '0000-00-00'          => 'CURRENT_DATE',
                'CURRENT_TIME'        => 'CURRENT_TIME',
                '00:00:00'            => 'CURRENT_TIME',
                'CURRENT_TIMESTAMP'   => 'CURRENT_TIMESTAMP',
                '0000-00-00 00:00:00' => 'CURRENT_TIMESTAMP',
                'LOCALTIME'           => 'LOCALTIME',
                'LOCALTIMESTAMP'      => 'LOCALTIMESTAMP',
                'NULL'                => 'NULL',
                'UTC_DATE'            => "(CURRENT_DATE AT TIME ZONE 'UTC')",
                'UTC_TIME'            => "(CURRENT_TIME AT TIME ZONE 'UTC')",
                'UTC_TIMESTAMP'       => "(NOW() AT TIME ZONE 'UTC')",
            ];
            
            foreach ($arrColumns as $arrColumn) {
                if (empty($arrColumn['Default'])) {
                    $this->log(
                        "\t" . '-- Default value for column "' . $arrColumn['Field'] . '" has not been detected...' . PHP_EOL
                    );
                    continue;
                }
                
                $sql = 'ALTER TABLE ' . $this->strSchema . '."' . $strTableName . '" '
                     . 'ALTER COLUMN "' . $arrColumn['Field'] . '" SET DEFAULT ';
                
                if (isset($arrSqlReservedValues[$arrColumn['Default']])) {
                    $sql .= $arrSqlReservedValues[$arrColumn['Default']] . ';';
                } else {
                    $sql .= " '" . $arrColumn['Default'] . "';";
                }
                
                $stmt = $this->pgsql->query($sql);
                
                if (false === $stmt) {
                    $this->log("\t" . '-- Cannot define the default value for column "' . $arrColumn['Field'] . '"...' . PHP_EOL);
                } else {
                    $this->log("\t" . '-- The default value for column "' . $arrColumn['Field'] . '" has defined...' . PHP_EOL);
                }
                
                unset($arrColumn, $sql, $stmt);
            }
            
            unset($arrSqlReservedValues);
            
        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Cannot set default values in table: ' . $this->strSchema 
                    . '."' . $strTableName . '"...' . PHP_EOL;
            
            $this->generateError($e, $strMsg);
            unset($strMsg);
        }
    }
    
    /**
     * Define which columns of the given table are of type "enum".
     * Set an appropriate constraint, if need.
     * 
     * @param  string $strTableName
     * @param  array  $arrColumns
     * @return void
     */
    private function processEnum($strTableName, array $arrColumns)
    {
        try {
            $this->log(PHP_EOL . "\t" . '-- Set "ENUMs" for table ' . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL);
            $this->connect();
            
            foreach ($arrColumns as $arrColumn) {
                $parenthesesFirstOccurrence = strpos($arrColumn['Type'], '(');
                
                if (false !== $parenthesesFirstOccurrence) {
                    $arrType = explode('(', $arrColumn['Type']);
                    
                    if ('enum' == $arrType[0]) {
                        /**
                         * $arrType[1] ends with ')'.
                         */
                        $sql = 'ALTER TABLE ' . $this->strSchema . '."' . $strTableName . '" '
                             . 'ADD CHECK ("' . $arrColumn['Field'] . '" IN (' . $arrType[1] . ');';
                        
                        $stmt = $this->pgsql->query($sql);
                        
                        if (false === $stmt) {
                            $this->log(
                                "\t" . '-- Cannot set "ENUM" for column "' . $arrColumn['Field'] . '"' 
                                .  PHP_EOL . '...Column "' . $arrColumn['Field'] 
                                . '" has defined as "CHARACTER VARYING(255)"...' . PHP_EOL
                            );
                            
                        } else {
                            $this->log(
                                "\t" . '-- "CHECK" was successfully added to column "' . $arrColumn['Field'] . '"...' . PHP_EOL
                            );
                        }
                        
                        unset($sql, $stmt);
                    }
                    
                    unset($arrType);
                }
                
                unset($arrColumn, $parenthesesFirstOccurrence);
            }
            
        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Cannot set "ENUMs" for table ' . $this->strSchema 
                    . '."' . $strTableName . '"...' . PHP_EOL;
            
            $this->generateError($e, $strMsg);
            unset($strMsg);
        }
    }
    
    /**
     * Define which column in given table has the "auto_increment" attribute.
     * Create an appropriate sequence.
     * 
     * @param  string $strTableName
     * @param  array  $arrColumns
     * @return void
     */
    private function createSequence($strTableName, array $arrColumns)
    {
        $strSeqName          = '';
        $boolSequenceCreated = false;
        
        try {
            $this->connect();
            
            foreach ($arrColumns as $arrColumn) {
                if ('auto_increment' == $arrColumn['Extra']) {
                    $strSeqName = $strTableName . '_' . $arrColumn['Field'] . '_seq';
                    $this->log("\t" . '-- Trying to create sequence ' . $this->strSchema . '."' . $strSeqName . '"...' . PHP_EOL);
                    $sql  = 'CREATE SEQUENCE ' . $this->strSchema . '."' . $strSeqName . '";';
                    $stmt = $this->pgsql->query($sql);
                    
                    if (false === $stmt) {
                        $this->log("\t" . '-- Failed to create sequence ' . $this->strSchema . '."' . $strSeqName . '"...' . PHP_EOL);
                        unset($stmt, $sql, $arrColumn);
                        break;
                    } else {
                        unset($stmt, $sql);
                    }
                    
                    $sql = 'ALTER TABLE ' . $this->strSchema . '."' . $strTableName . '" '
                         . 'ALTER COLUMN "' . $arrColumn['Field'] . '" '
                         . 'SET DEFAULT NEXTVAL(\'' . $this->strSchema . '."' . $strSeqName . '"\');';
                    
                    $stmt = $this->pgsql->query($sql);
                    
                    if (false === $stmt) {
                        $this->log(
                            "\t" . '-- Failed to set default value for ' . $this->strSchema . '."' 
                            . $strTableName . '"."' . $arrColumn['Field'] . '"...' . PHP_EOL 
                            . "\t" . '-- Note: sequence ' . $this->strSchema . '."' 
                            . $strSeqName . '" was created...' . PHP_EOL
                        );
                        
                        unset($stmt, $sql, $arrColumn);
                        break;
                    } else {
                        unset($stmt, $sql);
                    }
                    
                    $sql = 'ALTER SEQUENCE ' . $this->strSchema . '."' . $strSeqName . '" '
                         . 'OWNED BY ' . $this->strSchema . '."' . $strTableName . '"."' . $arrColumn['Field'] . '";';
                    
                    $stmt = $this->pgsql->query($sql);
                    
                    if (false === $stmt) {
                        $this->log(
                            "\t" . '-- Failed to relate sequence ' . $this->strSchema . '."' 
                            . $strSeqName . '" to ' . $this->strSchema . '."' . $strTableName . '"' 
                            . PHP_EOL . "\t" . '-- Note: sequence ' . $this->strSchema . '."' 
                            . $strSeqName . '" was created...' . PHP_EOL
                        );
                        
                        unset($stmt, $sql, $arrColumn);
                        break;
                    } else {
                        unset($stmt, $sql);
                    }
                    
                    $sql = 'SELECT SETVAL(\'' . $this->strSchema . '."' . $strSeqName . '"\', '
                          . '(SELECT MAX("' . $arrColumn['Field'] . '") FROM ' . $this->strSchema . '."' . $strTableName . '"));';
                    
                    $stmt = $this->pgsql->query($sql);
                    
                    if (false === $stmt) {
                        $this->log(
                            "\t" . '-- Failed to set max-value of  ' . $this->strSchema . '."' . $strTableName 
                            . '"."' . $arrColumn['Field'] . '" as the "NEXTVAL of "' . $this->strSchema 
                            . '."' . $strSeqName . '"...' . PHP_EOL
                            . "\t" . '-- Note: sequence ' . $this->strSchema . '."' 
                            . $strSeqName . '" was created...' . PHP_EOL
                        );
                        
                        unset($stmt, $sql, $arrColumn);
                        break;
                    } else {
                        unset($stmt, $sql);
                    }
                    
                    $boolSequenceCreated = true;
                }
                
                if ($boolSequenceCreated) {
                    unset($arrColumn);
                    $this->log("\t" . '-- Sequence ' . $this->strSchema . '."' . $strSeqName . '" was created...' . PHP_EOL);
                    break;
                }
                
                unset($arrColumn);
            }
            
        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Failed to create sequence ' . $this->strSchema 
                    . '."' . $strSeqName . '"...' . PHP_EOL;
            
            $this->generateError($e, $strMsg);
            unset($strMsg);
        }
    }
    
    /**
     * Create primary key and indices.
     * 
     * @param  string $strTableName
     * @param  array  $arrColumns
     * @return void
     */
    private function processIndexAndKey($strTableName, array $arrColumns)
    {
        try {
            $this->log(
                "\t" . '-- Set primary key and indices for table ' . $this->strSchema 
                . '."' . $strTableName . '"...' . PHP_EOL
            );
            
            $this->connect();
            $sql              = 'SHOW INDEX FROM `' . $strTableName . '`;';
            $stmt             = $this->mysql->query($sql);
            $arrIndices       = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            $arrPgIndices     = [];
            $intCounter       = 0;
            $strCurrentAction = '';
            unset($sql, $stmt);
            
            foreach ($arrIndices as $arrIndex) {
                if (isset($arrPgIndices[$arrIndex['Key_name']])) {
                    $arrPgIndices[$arrIndex['Key_name']]['column_name'][] = '"' . $arrIndex['Column_name'] . '"';
                } else {
                    $arrPgIndices[$arrIndex['Key_name']] = [
                        'is_unique'   => (0 == $arrIndex['Non_unique'] ? true : false),
                        'column_name' => ['"' . $arrIndex['Column_name'] . '"'],
                    ];
                }
                
                unset($arrIndex);
            }
            
            unset($arrIndices);
            
            foreach ($arrPgIndices as $strKeyName => $arrIndex) {
                $sql = '';
                
                if ('primary' == strtolower($strKeyName)) {
                    $strCurrentAction = 'PK';
                    $sql              = "ALTER TABLE " . $this->strSchema . '."' . $strTableName . '" '
                                      . 'ADD PRIMARY KEY(' . implode(',', $arrIndex['column_name']) . ');';
                    
                } else {
                    /**
                     * "schema_idxname_{integer}_idx" - is NOT a mistake.
                     */
                    $strColumnName    = str_replace('"', '', $arrIndex['column_name'][0]) . $intCounter;
                    $strCurrentAction = 'index';
                    $sql              = 'CREATE ' . ($arrIndex['is_unique'] ? 'UNIQUE ' : '') . 'INDEX "'
                                      . $this->strSchema . '_' . $strTableName . '_' . $strColumnName . '_idx" ON '
                                      . $this->strSchema . '."' . $strTableName 
                                      . '" (' . implode(',', $arrIndex['column_name']) . ');';
                    
                    unset($strColumnName);
                }
                
                $stmt = $this->pgsql->query($sql);
                
                if (false === $stmt) {
                    $this->log(
                        "\t" . '-- Failed to set ' . $strCurrentAction . ' for table ' 
                        . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL
                    );
                    
                } else {
                    $this->log(
                        "\t-- " . $strCurrentAction . ' for table '
                        . $this->strSchema . '."' . $strTableName . '" are set...' . PHP_EOL
                    );
                }
                
                unset($sql, $stmt, $strKeyName, $arrIndex);
                $intCounter++;
            }
            
            unset($arrPgIndices, $intCounter);
            
        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" 
                    . '-- Error occurred when tried to set primary key and indices for table ' 
                    . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL;
            
            $this->generateError($e, $strMsg);
            unset($strMsg);
        }
    }
    
    /**
     * Create foreign keys.
     * 
     * @param  string $strTableName
     * @param  array  $arrColumns
     * @return void
     */
    private function processForeignKey($strTableName, array $arrColumns)
    {
        try {
            $this->log("\t" . '-- Search foreign key for table ' . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL);
            $this->connect();
            
            $sql = "SELECT cols.COLUMN_NAME, 
                           refs.REFERENCED_TABLE_NAME, 
                           refs.REFERENCED_COLUMN_NAME, 
                           cRefs.UPDATE_RULE, 
                           cRefs.DELETE_RULE,  
                           cRefs.CONSTRAINT_NAME  
                    FROM INFORMATION_SCHEMA.`COLUMNS` AS cols  
                    INNER JOIN INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` AS refs 
                    ON refs.TABLE_SCHEMA = cols.TABLE_SCHEMA 
                        AND refs.REFERENCED_TABLE_SCHEMA = cols.TABLE_SCHEMA 
                        AND refs.TABLE_NAME = cols.TABLE_NAME 
                        AND refs.COLUMN_NAME = cols.COLUMN_NAME 
                    LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS cRefs 
                    ON cRefs.CONSTRAINT_SCHEMA = cols.TABLE_SCHEMA 
                        AND cRefs.CONSTRAINT_NAME = refs.CONSTRAINT_NAME 
                    LEFT JOIN INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` AS links 
                    ON links.TABLE_SCHEMA = cols.TABLE_SCHEMA 
                        AND links.REFERENCED_TABLE_SCHEMA = cols.TABLE_SCHEMA 
                        AND links.REFERENCED_TABLE_NAME = cols.TABLE_NAME 
                        AND links.REFERENCED_COLUMN_NAME = cols.COLUMN_NAME 
                    LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS cLinks 
                    ON cLinks.CONSTRAINT_SCHEMA = cols.TABLE_SCHEMA 
                        AND cLinks.CONSTRAINT_NAME = links.CONSTRAINT_NAME 
                    WHERE cols.TABLE_SCHEMA = '" . $this->strMySqlDbName . "' AND cols.TABLE_NAME = '" . $strTableName . "';";
            
            $stmt           = $this->mysql->query($sql);
            $arrForeignKeys = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            $arrConstraints = [];
            unset($sql, $stmt);
            
            foreach ($arrForeignKeys as $arrFk) {
                $arrConstraints[$arrFk['CONSTRAINT_NAME']][] = [
                    'column_name'            => $arrFk['COLUMN_NAME'],
                    'referenced_table_name'  => $arrFk['REFERENCED_TABLE_NAME'],
                    'referenced_column_name' => $arrFk['REFERENCED_COLUMN_NAME'],
                    'update_rule'            => $arrFk['UPDATE_RULE'],
                    'delete_rule'            => $arrFk['DELETE_RULE'],
                ];
                
                unset($arrFk);
            }
            
            unset($arrForeignKeys);
            
            foreach ($arrConstraints as $arrRows) {
                $arrFKs        = [];
                $arrPKs        = [];
                $strRefTbName  = '';
                $strDeleteRule = '';
                $strUpdateRule = '';
                $sql           = 'ALTER TABLE ' . $this->strSchema . '."' . $strTableName . '" ADD FOREIGN KEY (';
                
                foreach ($arrRows as $arrRow) {
                    $strRefTbName  = $arrRow['referenced_table_name'];
                    $strUpdateRule = $arrRow['update_rule'];
                    $strDeleteRule = $arrRow['delete_rule'];
                    $arrFKs[]      = '"' . $arrRow['column_name'] . '"';
                    $arrPKs[]      = '"' . $arrRow['referenced_column_name'] . '"';
                    unset($arrRow);
                }
                
                $sql .= implode(',', $arrFKs) . ') REFERENCES ' . $this->strSchema . '."' . $strRefTbName . '" ('
                     .  implode(',', $arrPKs) . ') ON UPDATE ' . $strUpdateRule . ' ON DELETE ' . $strDeleteRule . ';';
                
                $stmt = $this->pgsql->query($sql);
                
                if (false === $stmt) {
                    $this->log(
                        "\t" . '-- Failed to set foreign key for table ' 
                        . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL
                    );
                    
                } else {
                    $this->log(
                        "\t" . '-- Foreign key for table ' 
                        . $this->strSchema . '."' . $strTableName . '" is set...' . PHP_EOL
                    );
                }
                
                unset($sql, $stmt, $arrFKs, $arrPKs, $strRefTbName, $strDeleteRule, $strUpdateRule, $arrRows);
            }
            
        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" 
                    . '-- Error occurred when tried to create foreign key for table ' 
                    . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL;
            
            $this->generateError($e, $strMsg);
            unset($strMsg);
        }
    }
    
    /**
     * Runs "vacuum full" and "ANALYZE" for given table.
     * 
     * @param  string $strTableName
     * @return void
     */
    private function runVacuumFullAndAnalyze($strTableName)
    {
        $this->log(
            "\t" . '-- Running "VACUUM FULL and ANALYZE" query for table ' 
            . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL
        );
        
        try {
            $this->connect();
            $sql  = 'VACUUM (FULL, ANALYZE) ' . $this->strSchema . '."' . $strTableName . '";';
            $stmt = $this->pgsql->query($sql);
            
            if (false === $stmt) {
                $this->log(
                    "\t" . '-- Failed when run "VACUUM FULL and ANALYZE" query for table ' 
                    . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL
                );
                
            } else {
                $this->log(
                    "\t" . '-- "VACUUM FULL and ANALYZE" procedure for table ' 
                    . $this->strSchema . '."' . $strTableName . '" has been successfully accomplished...' . PHP_EOL
                );
            }
            
            unset($stmt, $sql);
            
        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" 
                    . '-- Error occurred when tried to run "VACUUM FULL and ANALYZE" query for table ' 
                    . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL;
            
            $this->generateError($e, $strMsg);
            unset($strMsg);
        }
    }
    
    /**
     * Set constraints for given table.
     * 
     * @param  string $strTableName
     * @return bool
     */
    private function setConstraints($strTableName)
    {
        $this->log("\t" . '-- Trying to set constraints for ' . $this->strSchema . '."' . $strTableName . '"...' . PHP_EOL);
        $arrColumns = [];
        
        try {
            $this->connect();
            $sql        = 'SHOW COLUMNS FROM `' . $strTableName . '`;';
            $stmt       = $this->mysql->query($sql);
            $arrColumns = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            unset($sql, $stmt);
            
        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Failed to set constraints for ' . $this->strSchema 
                    . '."' . $strTableName . '"...' . PHP_EOL;
            
            $this->generateError($e, $strMsg);
            unset($strMsg);
            return false;
        }
        
        $this->processEnum($strTableName, $arrColumns);
        $this->processNull($strTableName, $arrColumns);
        $this->processDefault($strTableName, $arrColumns);
        $this->createSequence($strTableName, $arrColumns);
        $this->processIndexAndKey($strTableName, $arrColumns);
        $this->processForeignKey($strTableName, $arrColumns);
        $this->log(
            "\t" . '-- Constraints for ' . $this->strSchema . '."' . $strTableName 
            . '" were set successfully...' . PHP_EOL
        );
        return true;
    }
    
    /**
     * Generates a summary report.
     * 
     * @param  void
     * @return string
     */
    private function generateReport()
    {
        $strRetVal              = PHP_EOL;
        $intLargestTableTitle   = 0;
        $intLargestRecordsTitle = 0;
        $intLargestTimeTitle    = 0;
        
        array_unshift($this->arrSummaryReport, ['TABLE', 'RECORDS', 'DATA LOAD TIME']);
        
        foreach ($this->arrSummaryReport as $arrReport) {
            $intTableTitleLength    = strlen($arrReport[0]);
            $intRecordsTitleLength  = strlen($arrReport[1]);
            $intTimeTitleLength     = strlen($arrReport[2]);
            $intLargestTableTitle   = $intLargestTableTitle > $intTableTitleLength ? $intLargestTableTitle : $intTableTitleLength;
            $intLargestRecordsTitle = $intLargestRecordsTitle > $intRecordsTitleLength ? $intLargestRecordsTitle : $intRecordsTitleLength;
            $intLargestTimeTitle    = $intLargestTimeTitle > $intTimeTitleLength ? $intLargestTimeTitle : $intTimeTitleLength;
            
            unset($arrReport, $intTableTitleLength, $intRecordsTitleLength, $intTimeTitleLength);
        }
        
        foreach ($this->arrSummaryReport as $arrReport) {
            $intSpace   = $intLargestTableTitle - strlen($arrReport[0]);
            $strRetVal .= "\t|  " . $arrReport[0];
            
            for ($i = 0; $i < $intSpace; $i++) {
                $strRetVal .= ' ';
            }
            
            $strRetVal .= '  |  ';
            
            $intSpace   = $intLargestRecordsTitle - strlen($arrReport[1]);
            $strRetVal .= $arrReport[1];
            
            for ($i = 0; $i < $intSpace; $i++) {
                $strRetVal .= ' ';
            }
            
            $strRetVal .= '  |  ';
            
            $intSpace   = $intLargestTimeTitle - strlen($arrReport[2]);
            $strRetVal .= $arrReport[2];
            
            for ($i = 0; $i < $intSpace; $i++) {
                $strRetVal .= ' ';
            }
            
            $strRetVal .= '  |' . PHP_EOL . "\t";
            $intSpace   = $intLargestTableTitle + $intLargestRecordsTitle + $intLargestTimeTitle + 16;
            
            for ($i = 0; $i < $intSpace; $i++) {
                $strRetVal .= '-';
            }
            
            $strRetVal .= PHP_EOL;
            unset($arrReport, $intSpace);
        }
        
        unset($intLargestTableTitle, $intLargestRecordsTitle, $intLargestTimeTitle);
        
        if (!empty($this->strWriteSummaryReportTo)) {
            file_put_contents($this->strWriteSummaryReportTo, $strRetVal);
        }
        
        return $strRetVal;
    }
    
    /**
     * Performs migration from source database to destination database.
     *
     * @param  void
     * @return void
     */
    public function migrate()
    {
        $intTimeBegin = time();
        $this->log(
            PHP_EOL . "\t" . '"FromMySqlToPostgreSql" - the database migration tool' . 
            PHP_EOL . "\tCopyright 2015  Anatoly Khaytovich <anatolyuss@gmail.com>" . 
            PHP_EOL . "\t-- Migration began..." . PHP_EOL
        );
        
        ini_set('memory_limit', '-1');
        
        /**
         * Create a database schema.
         */
        if (!$this->createSchema()) {
            $this->log('-- Script is terminated.' . PHP_EOL);
            exit;
        } else {
            $this->log('-- New schema "' . $this->strSchema . '" was successfully created...' . PHP_EOL);
        }
        
        if (!$this->loadTablesToMigrate()) {
            $this->log('-- Script is terminated.' . PHP_EOL);
            exit;
        } else {
            $intTablesCnt = count($this->arrTablesToMigrate);
            $this->log('-- ' . $intTablesCnt . ($intTablesCnt === 1 ? ' table ' : ' tables ') . 'detected' . PHP_EOL);
        }
        
        /**
         * Create tables with the basic structure (column names and data types).
         * Populate tables.
         */
        foreach ($this->arrTablesToMigrate as $arrTable) {
            $floatStartCopy = microtime(true);
            $intRecords     = 0;
            
            if (!$this->createTable($arrTable['Tables_in_' . $this->strMySqlDbName])) {
                $this->log('-- Script is terminated.' . PHP_EOL);
                exit;
            } else {
                $intRecords = $this->populateTable($arrTable['Tables_in_' . $this->strMySqlDbName]);
            }
            
            $floatEndCopy             = microtime(true);
            $this->arrSummaryReport[] = [
                $this->strSchema . '.' . $arrTable['Tables_in_' . $this->strMySqlDbName],
                $intRecords,
                round(($floatEndCopy - $floatStartCopy), 3) . ' seconds',
            ];
            
            unset($arrTable, $floatStartCopy, $floatEndCopy, $intRecords);
        }
        
        /**
         * Set constraints, then run "vacuum full" and "ANALYZE" for each table.
         */
        foreach ($this->arrTablesToMigrate as $arrTable) {
            $this->setConstraints($arrTable['Tables_in_' . $this->strMySqlDbName]);
            $this->runVacuumFullAndAnalyze($arrTable['Tables_in_' . $this->strMySqlDbName]);
            unset($arrTable);
        }
        
        /**
         * Remove the temporary directory.
         */
        if (!rmdir($this->strTemporaryDirectory)) {
            $this->log('-- NOTE: directory "' . $this->strTemporaryDirectory . '" was not removed!' . PHP_EOL);
        }
        
        $intTimeEnd  = time();
        $intExecTime = $intTimeEnd - $intTimeBegin;
        $intHours    = floor($intExecTime / 3600);
        $intMinutes  = ($intExecTime / 60) % 60;
        $intSeconds  = $intExecTime % 60;
        
        $this->log(
            $this->generateReport() . PHP_EOL 
            . '-- Migration was successfully accomplished!' . PHP_EOL
            . '-- Total time: ' . ($intHours < 10 ? '0' . $intHours : $intHours) 
            . ':' . ($intMinutes < 10 ? '0' . $intMinutes : $intMinutes) 
            . ':' . ($intSeconds < 10 ? '0' . $intSeconds : $intSeconds) 
            . ' (hours:minutes:seconds)' . PHP_EOL . PHP_EOL
        );
        
        unset($intTimeBegin, $intTimeEnd, $intExecTime, $intHours, $intMinutes, $intSeconds);
    }
}
