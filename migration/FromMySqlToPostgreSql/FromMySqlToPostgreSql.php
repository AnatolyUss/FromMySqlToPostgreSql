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
     * A \resource instance, connected to PostgreSql server for data loading via copy.
     *
     * @var \resource
     */
    private $pgsqldata;

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
     * An array of MySql views, that need to be migrated.
     *
     * @var array
     */
    private $arrViewsToMigrate;

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
     * Path to log files directory.
     *
     * @var string
     */
    private $strLogsDirectoryPath;

    /**
     * Path to "not_created_views" directory.
     *
     * @var string
     */
    private $strViewsErrorsDirectoryPath;

    /**
     * During migration each table's data will be split into chunks of $floatDataChunkSize.
     *
     * @var float
     */
    private $floatDataChunkSize;

    /**
     * Flag, indicating that only data should migrate
     *
     * @var bool
     */
    private $isDataOnly;

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

        $this->arrTablesToMigrate          = [];
        $this->arrViewsToMigrate           = [];
        $this->arrSummaryReport            = [];
        $this->strTemporaryDirectory       = $arrConfig['temp_dir_path'];
        $this->strLogsDirectoryPath        = $arrConfig['logs_dir_path'];
        $this->strWriteCommonLogTo         = $arrConfig['logs_dir_path'] . '/all.log';
        $this->strWriteSummaryReportTo     = $arrConfig['logs_dir_path'] . '/report-only.log';
        $this->strWriteErrorLogTo          = $arrConfig['logs_dir_path'] . '/errors-only.log';
        $this->strViewsErrorsDirectoryPath = $arrConfig['logs_dir_path'] . '/not_created_views';
        $this->strEncoding                 = isset($arrConfig['encoding']) ? $arrConfig['encoding'] : 'UTF-8';
        $this->floatDataChunkSize          = isset($arrConfig['data_chunk_size']) ? (float) $arrConfig['data_chunk_size'] : 10;
        $this->floatDataChunkSize          = $this->floatDataChunkSize < 1 ? 1 : $this->floatDataChunkSize;
        $this->strSourceConString          = $arrConfig['source'];
        $this->strTargetConString          = $arrConfig['target'];
        $this->mysql                       = null;
        $this->pgsql                       = null;
        $this->strMySqlDbName              = $this->extractDbName($this->strSourceConString);
        $this->strSchema                   = isset($arrConfig['schema']) ? $arrConfig['schema'] : '';
        $this->isDataOnly                  = isset($arrConfig['data_only']) ? (bool) $arrConfig['data_only'] : false;

        if (!file_exists($this->strTemporaryDirectory)) {
            mkdir($this->strTemporaryDirectory);

            if (!file_exists($this->strTemporaryDirectory)) {
                echo PHP_EOL,
                     '-- Cannot perform a migration due to impossibility to create "temporary_directory": ',
                     $this->strTemporaryDirectory,
                     PHP_EOL;

                exit;
            }
        }

        if (!file_exists($this->strLogsDirectoryPath)) {
            mkdir($this->strLogsDirectoryPath);

            if (!file_exists($this->strLogsDirectoryPath)) {
                echo PHP_EOL, '--Cannot create logs directory: ', $this->strLogsDirectoryPath, PHP_EOL;
            }
        }

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
     * If not, than create connections.
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
            // These are poor man's replacements to avoid 2 connection strings at this point in time.
            $datadsn = str_replace('pgsql:', '', $arrDestInput[0]);
            $datadsn = str_replace(';', ' ', $datadsn);
            $this->pgsqldata = pg_connect($datadsn." user=".$arrDestInput[1]." password=".$arrDestInput[2]);
            if (!$this->pgsqldata) {
                echo pg_last_error();
                exit;
            }
            pg_query($this->pgsqldata, "SET synchronous_commit=off");
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
     * @param  string        $strMessage
     * @param  string        $strSql
     * @return void
     */
    private function generateError(\PDOException $e, $strMessage, $strSql = '')
    {
        $strError = PHP_EOL . "\t-- " . $strMessage . PHP_EOL
                  . "\t-- PDOException code: " . $e->getCode() . PHP_EOL
                  . "\t-- File: " . $e->getFile() . PHP_EOL
                  . "\t-- Line: " . $e->getLine() . PHP_EOL
                  . "\t-- Message: " . $e->getMessage()
                  . (empty($strSql) ? '' : PHP_EOL . "\t-- SQL: " . $strSql . PHP_EOL)
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
    private function loadStructureToMigrate()
    {
        $boolRetVal = false;
        $sql        = '';

        try {
            $this->connect();
            $sql       = 'SHOW FULL TABLES IN `' . $this->strMySqlDbName . '`;';
            $stmt      = $this->mysql->query($sql);
            $arrResult = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            foreach ($arrResult as $arrRow) {
                if ('BASE TABLE' == $arrRow['Table_type']) {
                    $this->arrTablesToMigrate[] = $arrRow;
                } elseif ('VIEW' == $arrRow['Table_type']) {
                    $this->arrViewsToMigrate[] = $arrRow;
                }
                unset($arrRow);
            }

            $boolRetVal = true;
            unset($sql, $stmt, $arrResult);

        } catch (\PDOException $e) {
            $this->generateError(
                $e,
                __METHOD__ . PHP_EOL . "\t" . '-- Cannot load tables/views from source (MySql) database...',
                $sql
            );
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
        $boolRetVal       = false;
        $boolSchemaExists = false;
        $sql              = '';

        try {
            $this->connect();

            if (empty($this->strSchema)) {
                $this->strSchema = $this->strMySqlDbName;

                for ($i = 1; true; $i++) {
                    $sql = "SELECT schema_name FROM information_schema.schemata "
                         . "WHERE schema_name = '" . $this->strSchema . "';";

                    $stmt       = $this->pgsql->query($sql);
                    $arrSchemas = $stmt->fetchAll(\PDO::FETCH_ASSOC);

                    if (empty($arrSchemas)) {
                        unset($sql, $arrSchemas, $stmt);
                        break;
                    } elseif (1 == $i) {
                        $this->strSchema .= '_' . $i;
                        unset($sql, $arrSchemas, $stmt);
                    } else {
                        $arrSchema                        = explode('_', $this->strSchema);
                        $arrSchema[count($arrSchema) - 1] = $i;
                        $this->strSchema                  = implode('_', $arrSchema);
                        unset($sql, $arrSchemas, $stmt, $arrSchema);
                    }
                }

            } else {
                $sql = "SELECT schema_name FROM information_schema.schemata "
                     . "WHERE schema_name = '" . $this->strSchema . "';";

                $stmt             = $this->pgsql->query($sql);
                $arrSchemas       = $stmt->fetchAll(\PDO::FETCH_ASSOC);
                $boolSchemaExists = !empty($arrSchemas);
                unset($sql, $arrSchemas, $stmt);
            }

            if (!$boolSchemaExists) {
                $sql  = 'CREATE SCHEMA "' . $this->strSchema . '";';
                $stmt = $this->pgsql->query($sql);
                unset($sql, $stmt);
            }

            $boolRetVal = true;

        } catch (\PDOException $e) {
            $boolRetVal = false;
            $this->generateError(
                $e,
                __METHOD__ . PHP_EOL . "\t" . '-- Cannot create a new schema...',
                $sql
            );
        }

        return $boolRetVal;
    }

    /**
     * Migrate given view to PostgreSql server.
     *
     * @param  string $strViewName
     * @return void
     */
    private function createView($strViewName)
    {
        $sql = '';

        try {
            $this->log(PHP_EOL . "\t" . '-- Attempting to create view: "' . $this->strSchema . '"."' . $strViewName . '"...' . PHP_EOL);
            $this->connect();

            $sql        = 'SHOW CREATE VIEW `' . $strViewName . '`;';
            $stmt       = $this->mysql->query($sql);
            $arrColumns = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            unset($sql, $stmt);

            $sql  = \ViewGenerator::generateView($this->strSchema, $strViewName, $arrColumns[0]['Create View']);
            $stmt = $this->pgsql->query($sql);
            unset($sql, $stmt, $arrColumns);
            $this->log(PHP_EOL . "\t" . '-- View: "' . $this->strSchema . '"."' . $strViewName . '" is created...' . PHP_EOL);

        } catch (\PDOException $e) {
            $boolViewsErrorsDirectoryExists = true;

            if (!file_exists($this->strViewsErrorsDirectoryPath)) {
                mkdir($this->strViewsErrorsDirectoryPath);

                if (!file_exists($this->strViewsErrorsDirectoryPath)) {
                    $boolViewsErrorsDirectoryExists = false;
                }
            }

            if (file_exists($this->strViewsErrorsDirectoryPath)) {
                $resource = fopen($this->strViewsErrorsDirectoryPath . '/' . $strViewName . '.sql', 'w');
                fwrite($resource, $sql);
                fclose($resource);
                unset($resource);
            }

            $strMsg = $boolViewsErrorsDirectoryExists && file_exists($this->strViewsErrorsDirectoryPath . '/' . $strViewName . '.sql')
                    ? __METHOD__ . PHP_EOL . "\t" . '-- Cannot create view "' . $this->strSchema . '"."' .  $strViewName .  '" '
                      . PHP_EOL . "\t" . '-- You can find view definition at "logs_directory/not_created_views/' . $strViewName . '.sql"'
                      . PHP_EOL . "\t" . '-- You can try to fix view definition script and run it manually.'
                    : __METHOD__ . PHP_EOL . "\t" . '-- Cannot create view "' . $this->strSchema . '"."' .  $strViewName .  '" ';

            $this->log(PHP_EOL . "\t" . '-- Cannot create view "' . $this->strSchema . '"."' .  $strViewName .  '" ' . PHP_EOL);
            $this->generateError($e, $strMsg, $sql);
            unset($strMsg, $boolViewsErrorsDirectoryExists, $sql);
        }
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
        $sql        = '';

        try {
            $this->log(PHP_EOL . '-- Currently processing table: ' . $strTableName . '...' . PHP_EOL);
            $this->connect();

            $sql        = 'SHOW FULL COLUMNS FROM `' . $strTableName . '`;';
            $stmt       = $this->mysql->query($sql);
            $arrColumns = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            unset($sql, $stmt);

            $strSqlCreateTable = 'CREATE TABLE "' . $this->strSchema . '"."' . $strTableName . '"(';
            $arrSqlColumns = [];

            foreach ($arrColumns as $arrColumn) {
                $strColumnData = '"' . $arrColumn['Field'] . '" ' . \MapDataTypes::map($arrColumn['Type']);

                if ('no' == strtolower($arrColumn['Null'])) {
                    $strColumnData .= " NOT NULL";
                }
                $arrSqlColumns[] = $strColumnData;
            }

            $strSqlCreateTable .= implode(',', $arrSqlColumns) . ');';
            $stmt              = $this->pgsql->query($strSqlCreateTable);
            $boolRetVal        = true;

            unset($strSqlCreateTable, $stmt, $arrColumns);
            $this->log("\t" . '-- Table "' . $this->strSchema . '"."' . $strTableName . '" ' . 'is created.' . PHP_EOL);

        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Cannot create table "' . $this->strSchema . '"."' .  $strTableName .  '".';
            $this->generateError($e, $strMsg, $sql);
            unset($strMsg);
        }
        return $boolRetVal;
    }

    /**
     * Escape the given string for PostgreSQL's COPY text format.
     *
     * @param  string $value
     * @return string
     */
    private function escapeValue($value)
    {
        return str_replace(
            array(  "\\",  "\n",  "\r",  "\t"),
            array("\\\\", "\\n", "\\r", "\\t"),
            $value
        );
    }

    /**
     * Save a chunk of rows into PostgreSQL
     *
     * @param  string $strTableName
     * @param  array  $copyArray
     * @return int    Number of rows inserted.
     */
    private function copySaveRows($strTableName, $copyArray) {
        $intRetVal = 0;
        // Attempt copy, if it failes, do one at a time to ensure we find all the errors.
        // If the data is valid, we perform fast, otherwise we ensure correctness at the cost of speed.

        if (!@pg_copy_from($this->pgsqldata, "\"" . $this->strSchema . "\".\"" . $strTableName . "\"", $copyArray)) {
            // do each row, loging the failed ones.
            $this->log("\t-- The following contains rows rejected by PostgreSQL for table ".
                        "\"" . $this->strSchema . "\".\"" . $strTableName . "\"\n");
            foreach ($copyArray as $copyrow) {
                if (!@pg_copy_from($this->pgsqldata, "\"" . $this->strSchema . "\".\"" . $strTableName . "\"", array($copyrow))) {
                    $this->log($copyrow);
                } else {
                    $intRetVal++;
                }
            }
            $this->log("-- End of failed rows --\n");
        } else {
            $intRetVal += count($copyArray);
        }

        return $intRetVal;
    }

    /**
     * Load a chunk of data using "PostgreSql COPY".
     *
     * @param  string $strTableName
     * @param  string $strSelectFieldList
     * @param  int    $intOffset
     * @param  int    $intRowsInChunk
     * @param  int    $intRowsCnt
     * @return int
     */
    private function populateTableData(
        $strTableName,
        $strSelectFieldList,
        $intRowsInChunk,
        $intRowsCnt
    ) {
        $intRetVal   = 0;
        $sql         = '';
        $sqlCopy     = '';
        $copiedCount = 0;
        $copyArray = [];

        try {
            $this->connect();
            $sql            = 'SELECT ' . $strSelectFieldList . ' FROM `' . $strTableName . '`;';
            $stmt           = $this->mysql->prepare($sql);
            $arrRows        = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            $this->mysql->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
            $mysqlResult = $stmt->execute();

            /*
             * Ensure correctness of encoding and insert data into temporary csv file.
             */
            while ($arrRow = $stmt->fetch(PDO::FETCH_ASSOC)) {
                $boolValidCsvEntity  = true;
                $arrSanitizedCsvData = [];
                $copiedCount++;

                foreach ($arrRow as $value) {
                    if (is_null($value)) {
                        $arrSanitizedCsvData[] = '\N';
                    } else if (mb_check_encoding($value, $this->strEncoding)) {
                        $arrSanitizedCsvData[] = $this->escapeValue($value);
                    } else {
                        $value = mb_convert_encoding($value, $this->strEncoding);

                        if (mb_check_encoding($value, $this->strEncoding)) {
                            $arrSanitizedCsvData[] = $this->escapeValue($value);
                        } else {
                            $boolValidCsvEntity = false;
                        }
                    }
                    unset($value);
                }

                if ($boolValidCsvEntity) {
                    $copyArray[] = implode("\t", $arrSanitizedCsvData) . "\n";
                }

                if (($copiedCount % $intRowsInChunk) == 0) {
                    $intRetVal += $this->copySaveRows($strTableName, $copyArray);
                    $copiedCount = 0;
                    $copyArray = array();
                }
            }
            $intRetVal += $this->copySaveRows($strTableName, $copyArray);
            $copiedCount = 0;
            $copyArray = array();

        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL;
            $this->generateError($e, $strMsg, $sql . PHP_EOL . $sqlCopy);
            $this->log("\t--Following MySQL query will return a data set, rejected by PostgreSQL:\n" . $sql . "\n");
        }

        return $intRetVal;
    }

    /**
     * Arranges columns data before loading.
     *
     * @param  array $arrColumns
     * @return string
     */
    private function arrangeColumnsData(array $arrColumns)
    {
        $strRetVal = '';

        foreach ($arrColumns as $arrColumn) {
            if (
                stripos($arrColumn['Type'], 'geometry') !== false
                || stripos($arrColumn['Type'], 'point') !== false
                || stripos($arrColumn['Type'], 'linestring') !== false
                || stripos($arrColumn['Type'], 'polygon') !== false
            ) {
                $strRetVal .= 'HEX(ST_AsWKB(`' . $arrColumn['Field'] . '`)),';
            } elseif (
                stripos($arrColumn['Type'], 'blob') !== false
                || stripos($arrColumn['Type'], 'binary') !== false
            ) {
                $strRetVal .= 'HEX(`' . $arrColumn['Field'] . '`),';
            } elseif (
                stripos($arrColumn['Type'], 'bit') !== false
            ) {
                $strRetVal .= 'BIN(`' . $arrColumn['Field'] . '`),';
            } elseif (
                stripos($arrColumn['Type'], 'timestamp') !== false
                || stripos($arrColumn['Type'], 'date') !== false
            ) {
                $strRetVal .= 'IF(`' . $arrColumn['Field']
                           .  '` IN(\'0000-00-00\', \'0000-00-00 00:00:00\'), \'-INFINITY\', `'
                           .  $arrColumn['Field'] . '`),';
            } else {
                $strRetVal .= '`' . $arrColumn['Field'] . '`,';
            }
        }

        return substr($strRetVal, 0, -1);
    }

    /**
     * Populate current table.
     *
     * @param  string $strTableName
     * @return int
     */
    private function populateTable($strTableName)
    {
        $intRetVal = 0;
        $sql       = '';

        try {
            $this->log("\t" . '-- Populating table "' . $this->strSchema . '"."' . $strTableName . '" ' . PHP_EOL);
            $this->connect();

            // Determine current table size, apply "chunking".
            $sql = "SELECT ((data_length + index_length) / 1024 / 1024) AS size_in_mb
                    FROM information_schema.TABLES
                    WHERE table_schema = '" . $this->strMySqlDbName . "'
                      AND table_name = '" . $strTableName . "';";

            $stmt               = $this->mysql->query($sql);
            $arrRows            = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            $floatTableSizeInMb = (float) $arrRows[0]['size_in_mb'];
            $floatTableSizeInMb = $floatTableSizeInMb < 1 ? 1 : $floatTableSizeInMb;
            unset($sql, $stmt, $arrRows);

            $sql               = 'SELECT COUNT(1) AS rows_count FROM `' . $strTableName . '`;';
            $stmt              = $this->mysql->query($sql);
            $arrRows           = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            $intRowsCnt        = (int) $arrRows[0]['rows_count'];
            $floatChunksCnt    = $floatTableSizeInMb / $this->floatDataChunkSize;
            $floatChunksCnt    = $floatChunksCnt < 1 ? 1 : $floatChunksCnt;
            $intRowsInChunk    = ceil($intRowsCnt / $floatChunksCnt);
            unset($sql, $stmt, $arrRows);

            // Build field list for SELECT from MySQL and apply optional casting or function based on field type.
            $sql                = 'SHOW FULL COLUMNS FROM `' . $strTableName . '`;';
            $stmt               = $this->mysql->query($sql);
            $arrColumns         = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            $strSelectFieldList = $this->arrangeColumnsData($arrColumns);
            unset($sql, $stmt, $arrColumns);
            // End field list for SELECT from MySQL.

            $this->log(
                "\t" . '-- Total rows to insert into "' . $this->strSchema . '"."'
                . $strTableName . '": ' . $intRowsCnt . PHP_EOL
            );

            $intRetVal = $this->populateTableData(
                $strTableName,
                $strSelectFieldList,
                $intRowsInChunk,
                $intRowsCnt
            );
            $this->log(
                "\t" . '-- Total rows inserted into "' . $this->strSchema . '"."'
                . $strTableName . '": ' . $intRetVal . PHP_EOL
            );

        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL;
            $this->generateError($e, $strMsg, $sql);
            unset($strMsg);
        }

        echo PHP_EOL, PHP_EOL;
        return array($intRowsCnt, $intRowsCnt - $intRetVal);
    }

    /**
     * Create comments.
     *
     * @param  string $strTableName
     * @param  array  $arrColumns
     * @return void
     */
    private function processComment($strTableName, array $arrColumns)
    {
        $sql = '';

        foreach ($arrColumns as $arrColumn) {
            if (!isset($arrColumn['Comment'])) {
                continue;
            }

            try {
                $this->connect();
                $sql = 'COMMENT ON COLUMN "' . $this->strSchema . '"."' . $strTableName . '"."'
                     . $arrColumn['Field'] . '" IS \'' . $arrColumn['Comment'] . '\';';

                 $stmt = $this->pgsql->query($sql);

                 if ($stmt === false) {
                     $this->log("\t" . '-- Cannot create comment on column "' . $arrColumn['Field'] . '"...' . PHP_EOL);
                 } else {
                     $this->log("\t" . '-- Comment on column "' . $arrColumn['Field'] . '" has set...' . PHP_EOL);
                 }

            } catch (\PDOException $e) {
                $this->generateError($e, __METHOD__ . PHP_EOL, $sql);
            }
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
        $this->log(
            PHP_EOL . "\t" . '-- Set default values for table: "'
            . $this->strSchema . '"."' . $strTableName . '"...' . PHP_EOL
        );

        $sql                  = '';
        $arrSqlReservedValues = [
            'CURRENT_DATE'        => 'CURRENT_DATE',
            '0000-00-00'          => "'-INFINITY'",
            'CURRENT_TIME'        => 'CURRENT_TIME',
            '00:00:00'            => '00:00:00',
            'CURRENT_TIMESTAMP'   => 'CURRENT_TIMESTAMP',
            '0000-00-00 00:00:00' => "'-INFINITY'",
            'LOCALTIME'           => 'LOCALTIME',
            'LOCALTIMESTAMP'      => 'LOCALTIMESTAMP',
            'NULL'                => 'NULL',
            'UTC_DATE'            => "(CURRENT_DATE AT TIME ZONE 'UTC')",
            'UTC_TIME'            => "(CURRENT_TIME AT TIME ZONE 'UTC')",
            'UTC_TIMESTAMP'       => "(NOW() AT TIME ZONE 'UTC')",
        ];

        foreach ($arrColumns as $arrColumn) {
            if (!isset($arrColumn['Default'])) {
                $this->log(
                    "\t" . '-- Default value for column "' . $arrColumn['Field'] . '" has not been detected...' . PHP_EOL
                );
                continue;
            }

            try {
                $this->connect();
                $sql = 'ALTER TABLE "' . $this->strSchema . '"."' . $strTableName . '" '
                     . 'ALTER COLUMN "' . $arrColumn['Field'] . '" SET DEFAULT ';

                if (isset($arrSqlReservedValues[$arrColumn['Default']])) {
                    $sql .= $arrSqlReservedValues[$arrColumn['Default']] . ';';
                } else if (substr($arrColumn['Type'], 0, 3) === 'bit' && substr($arrColumn['Default'], 0, 2) === "b'") {
                    // This is a defaultl for a bit column use PostgreSql syntax.
                    $sql .= substr($arrColumn['Default'], 1) . "::bit;";
                } else {
                    $sql .= is_numeric($arrColumn['Default'])
                          ? $arrColumn['Default'] . ';'
                          : " '" . $arrColumn['Default'] . "';";
                }

                $stmt = $this->pgsql->query($sql);

                if ($stmt === false) {
                    $this->log("\t" . '-- Cannot define the default value for column "' . $arrColumn['Field'] . '"...' . PHP_EOL);
                } else {
                    $this->log("\t" . '-- The default value for column "' . $arrColumn['Field'] . '" has defined...' . PHP_EOL);
                }
            } catch (\PDOException $e) {
                $this->generateError($e, __METHOD__ . PHP_EOL, $sql);
            }

            unset($arrColumn, $sql, $stmt);
        }

        unset($arrSqlReservedValues);
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
        $this->log(PHP_EOL . "\t" . '-- Set "ENUMs" for table "' . $this->strSchema . '"."' . $strTableName . '"...' . PHP_EOL);
        $sql = '';

        foreach ($arrColumns as $arrColumn) {
            try {
                $this->connect();
                $parenthesesFirstOccurrence = strpos($arrColumn['Type'], '(');

                if (false !== $parenthesesFirstOccurrence) {
                    $arrType = explode('(', $arrColumn['Type']);

                    if ('enum' == $arrType[0]) {
                        // $arrType[1] ends with ')'.
                        $sql = 'ALTER TABLE "' . $this->strSchema . '"."' . $strTableName . '" '
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

            } catch (\PDOException $e) {
                $this->generateError($e, __METHOD__ . PHP_EOL, $sql);
            }

            unset($arrColumn, $parenthesesFirstOccurrence);
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
        $sql                 = '';
        $strSeqName          = '';
        $boolSequenceCreated = false;

        try {
            $this->connect();

            foreach ($arrColumns as $arrColumn) {
                if ('auto_increment' == $arrColumn['Extra']) {
                    $strSeqName = $strTableName . '_' . $arrColumn['Field'] . '_seq';
                    $this->log("\t" . '-- Trying to create sequence "' . $this->strSchema . '"."' . $strSeqName . '"...' . PHP_EOL);
                    $sql  = 'CREATE SEQUENCE "' . $this->strSchema . '"."' . $strSeqName . '";';
                    $stmt = $this->pgsql->query($sql);

                    if (false === $stmt) {
                        $this->log("\t" . '-- Failed to create sequence "' . $this->strSchema . '"."' . $strSeqName . '"...' . PHP_EOL);
                        unset($stmt, $sql, $arrColumn);
                        break;
                    } else {
                        unset($stmt, $sql);
                    }

                    $sql = 'ALTER TABLE "' . $this->strSchema . '"."' . $strTableName . '" '
                         . 'ALTER COLUMN "' . $arrColumn['Field'] . '" '
                         . 'SET DEFAULT NEXTVAL(\'"' . $this->strSchema . '"."' . $strSeqName . '"\');';

                    $stmt = $this->pgsql->query($sql);

                    if (false === $stmt) {
                        $this->log(
                            "\t" . '-- Failed to set default value for "' . $this->strSchema . '"."'
                            . $strTableName . '"."' . $arrColumn['Field'] . '"...' . PHP_EOL
                            . "\t" . '-- Note: sequence "' . $this->strSchema . '"."'
                            . $strSeqName . '" was created...' . PHP_EOL
                        );

                        unset($stmt, $sql, $arrColumn);
                        break;
                    } else {
                        unset($stmt, $sql);
                    }

                    $sql = 'ALTER SEQUENCE "' . $this->strSchema . '"."' . $strSeqName . '" '
                         . 'OWNED BY "' . $this->strSchema . '"."' . $strTableName . '"."' . $arrColumn['Field'] . '";';

                    $stmt = $this->pgsql->query($sql);

                    if (false === $stmt) {
                        $this->log(
                            "\t" . '-- Failed to relate sequence "' . $this->strSchema . '"."'
                            . $strSeqName . '" to "' . $this->strSchema . '"."' . $strTableName . '"'
                            . PHP_EOL . "\t" . '-- Note: sequence "' . $this->strSchema . '"."'
                            . $strSeqName . '" was created...' . PHP_EOL
                        );

                        unset($stmt, $sql, $arrColumn);
                        break;
                    } else {
                        unset($stmt, $sql);
                    }

                    $sql = 'SELECT SETVAL(\'"' . $this->strSchema . '"."' . $strSeqName . '"\', '
                         . '(SELECT MAX("' . $arrColumn['Field'] . '") FROM "'
                         . $this->strSchema . '"."' . $strTableName . '"));';

                    $stmt = $this->pgsql->query($sql);

                    if (false === $stmt) {
                        $this->log(
                            "\t" . '-- Failed to set max-value of "' . $this->strSchema . '"."' . $strTableName
                            . '"."' . $arrColumn['Field'] . '" as the "NEXTVAL of "' . $this->strSchema
                            . '."' . $strSeqName . '"...' . PHP_EOL
                            . "\t" . '-- Note: sequence "' . $this->strSchema . '"."'
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
                    $this->log("\t" . '-- Sequence "' . $this->strSchema . '"."' . $strSeqName . '" was created...' . PHP_EOL);
                    break;
                }

                unset($arrColumn);
            }

        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Failed to create sequence "' . $this->strSchema
                    . '"."' . $strSeqName . '"...' . PHP_EOL;

            $this->generateError($e, $strMsg, $sql);
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
        $sql = '';

        try {
            $this->log(
                "\t" . '-- Set primary key and indices for table "' . $this->strSchema
                . '"."' . $strTableName . '"...' . PHP_EOL
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
                        'Index_type'  => ' USING ' . ($arrIndex['Index_type'] === 'SPATIAL' ? 'GIST' : $arrIndex['Index_type']),
                    ];
                }
                unset($arrIndex);
            }

            unset($arrIndices);

            foreach ($arrPgIndices as $strKeyName => $arrIndex) {
                $sql = '';

                if (strtolower($strKeyName) === 'primary') {
                    $strCurrentAction = 'PK';
                    $sql              = 'ALTER TABLE "' . $this->strSchema . '"."' . $strTableName . '" '
                                      . 'ADD PRIMARY KEY(' . implode(',', $arrIndex['column_name']) . ');';

                } else {
                    // "schema_idxname_{integer}_idx" - is NOT a mistake.
                    $strColumnName    = str_replace('"', '', $arrIndex['column_name'][0]) . $intCounter;
                    $strCurrentAction = 'index';
                    $sql              = 'CREATE ' . ($arrIndex['is_unique'] ? 'UNIQUE ' : '') . 'INDEX "'
                                      . $this->strSchema . '_' . $strTableName . '_' . $strColumnName . '_idx" ON "'
                                      . $this->strSchema . '"."' . $strTableName . '" ' . $arrIndex['Index_type']
                                      . ' (' . implode(',', $arrIndex['column_name']) . ');';

                    unset($strColumnName);
                }

                $stmt = $this->pgsql->query($sql);

                if (false === $stmt) {
                    $this->log(
                        "\t" . '-- Failed to set ' . $strCurrentAction . ' for table "'
                        . $this->strSchema . '"."' . $strTableName . '"...' . PHP_EOL
                    );

                } else {
                    $this->log(
                        "\t-- " . $strCurrentAction . ' for table "'
                        . $this->strSchema . '"."' . $strTableName . '" are set...' . PHP_EOL
                    );
                }

                unset($sql, $stmt, $strKeyName, $arrIndex);
                $intCounter++;
            }

            unset($arrPgIndices, $intCounter);

        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t"
                    . '-- Error occurred when tried to set primary key and indices for table "'
                    . $this->strSchema . '"."' . $strTableName . '"...' . PHP_EOL;

            $this->generateError($e, $strMsg, $sql);
            unset($strMsg);
        }
    }

    /**
     * Create foreign keys.
     *
     * @param  string $strTableName
     * @return void
     */
    private function processForeignKey($strTableName)
    {
        $sql = '';

        try {
            $this->log("\t" . '-- Search foreign key for table "' . $this->strSchema . '"."' . $strTableName . '"...' . PHP_EOL);
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
                $sql           = 'ALTER TABLE "' . $this->strSchema . '"."' . $strTableName . '" ADD FOREIGN KEY (';

                foreach ($arrRows as $arrRow) {
                    $strRefTbName  = $arrRow['referenced_table_name'];
                    $strUpdateRule = $arrRow['update_rule'];
                    $strDeleteRule = $arrRow['delete_rule'];
                    $arrFKs[]      = '"' . $arrRow['column_name'] . '"';
                    $arrPKs[]      = '"' . $arrRow['referenced_column_name'] . '"';
                    unset($arrRow);
                }

                $sql .= implode(',', $arrFKs) . ') REFERENCES "' . $this->strSchema . '"."' . $strRefTbName . '" ('
                     .  implode(',', $arrPKs) . ') ON UPDATE ' . $strUpdateRule . ' ON DELETE ' . $strDeleteRule . ';';

                $stmt = $this->pgsql->query($sql);

                if (false === $stmt) {
                    $this->log(
                        "\t" . '-- Failed to set foreign keys for table "'
                        . $this->strSchema . '"."' . $strTableName . '"...' . PHP_EOL
                    );

                } else {
                    $this->log(
                        "\t" . '-- Foreign key for table "'
                        . $this->strSchema . '"."' . $strTableName . '" is set...' . PHP_EOL
                    );
                }

                unset($sql, $stmt, $arrFKs, $arrPKs, $strRefTbName, $strDeleteRule, $strUpdateRule, $arrRows);
            }

        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t"
                    . '-- Error occurred when tried to create foreign key for table "'
                    . $this->strSchema . '"."' . $strTableName . '"...' . PHP_EOL;

            $this->generateError($e, $strMsg, $sql);
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
        $sql = '';

        $this->log(
            "\t" . '-- Running "VACUUM FULL and ANALYZE" query for table "'
            . $this->strSchema . '"."' . $strTableName . '"...' . PHP_EOL
        );

        try {
            $this->connect();
            $sql  = 'VACUUM (FULL, ANALYZE) "' . $this->strSchema . '"."' . $strTableName . '";';
            $stmt = $this->pgsql->query($sql);

            if (false === $stmt) {
                $this->log(
                    "\t" . '-- Failed when run "VACUUM FULL and ANALYZE" query for table "'
                    . $this->strSchema . '"."' . $strTableName . '"...' . PHP_EOL
                );

            } else {
                $this->log(
                    "\t" . '-- "VACUUM FULL and ANALYZE" procedure for table "'
                    . $this->strSchema . '"."' . $strTableName . '" has been successfully accomplished...' . PHP_EOL
                );
            }

            unset($stmt, $sql);

        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t"
                    . '-- Error occurred when tried to run "VACUUM FULL and ANALYZE" query for table "'
                    . $this->strSchema . '"."' . $strTableName . '"...' . PHP_EOL;

            $this->generateError($e, $strMsg, $sql);
            unset($strMsg);
        }
    }

    /**
     * Set constraints (excluding foreign key constraints) for given table.
     *
     * @param  string $strTableName
     * @return bool
     */
    private function setTableConstraints($strTableName)
    {
        $this->log("\t" . '-- Trying to set table constraints for "' . $this->strSchema . '"."' . $strTableName . '"...' . PHP_EOL);
        $arrColumns = [];
        $sql        = '';

        try {
            $this->connect();
            $sql        = 'SHOW FULL COLUMNS FROM `' . $strTableName . '`;';
            $stmt       = $this->mysql->query($sql);
            $arrColumns = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            unset($sql, $stmt);

        } catch (\PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Failed to set constraints for "' . $this->strSchema
                    . '"."' . $strTableName . '"...' . PHP_EOL;

            $this->generateError($e, $strMsg, $sql);
            unset($strMsg);
            return false;
        }

        $this->processEnum($strTableName, $arrColumns);
        $this->processDefault($strTableName, $arrColumns);
        $this->createSequence($strTableName, $arrColumns);
        $this->processIndexAndKey($strTableName, $arrColumns);
        $this->processComment($strTableName, $arrColumns);
        $this->log(
            "\t" . '-- Constraints for "' . $this->strSchema . '"."' . $strTableName
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
        $intLargestFailedTitle  = 0;
        $intLargestTimeTitle    = 0;

        array_unshift($this->arrSummaryReport, ['TABLE', 'RECORDS', 'FAILED', 'DATA LOAD TIME']);

        foreach ($this->arrSummaryReport as $arrReport) {
            $intTableTitleLength    = strlen($arrReport[0]);
            $intRecordsTitleLength  = strlen($arrReport[1]);
            $intFailedTitleLength  = strlen($arrReport[2]);
            $intTimeTitleLength     = strlen($arrReport[3]);
            $intLargestTableTitle   = $intLargestTableTitle > $intTableTitleLength ? $intLargestTableTitle : $intTableTitleLength;
            $intLargestRecordsTitle = $intLargestRecordsTitle > $intRecordsTitleLength ? $intLargestRecordsTitle : $intRecordsTitleLength;
            $intLargestFailedTitle  = $intLargestFailedTitle > $intFailedTitleLength ? $intLargestFailedTitle : $intFailedTitleLength;
            $intLargestTimeTitle    = $intLargestTimeTitle > $intTimeTitleLength ? $intLargestTimeTitle : $intTimeTitleLength;
        }

        foreach ($this->arrSummaryReport as $arrReport) {
            $intSpace   = $intLargestTableTitle - strlen($arrReport[0]);
            $strRetVal .= "\t|  " . $arrReport[0];

            $strRetVal .= str_repeat(' ', $intSpace);
            $strRetVal .= '  |  ';

            $intSpace   = $intLargestRecordsTitle - strlen($arrReport[1]);
            $strRetVal .= $arrReport[1];

            $strRetVal .= str_repeat(' ', $intSpace);
            $strRetVal .= '  |  ';

            $intSpace   = $intLargestFailedTitle - strlen($arrReport[2]);
            $strRetVal .= $arrReport[2];

            $strRetVal .= str_repeat(' ', $intSpace);
            $strRetVal .= '  |  ';

            $intSpace   = $intLargestTimeTitle - strlen($arrReport[3]);
            $strRetVal .= $arrReport[3];

            $strRetVal .= str_repeat(' ', $intSpace);
            $strRetVal .= '  |' . PHP_EOL . "\t";
            $intSpace   = $intLargestTableTitle + $intLargestRecordsTitle + $intLargestFailedTitle + $intLargestTimeTitle + 21;

            $strRetVal .= str_repeat('-', $intSpace);

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
     * Create tables with the basic structure (column names and data types).
     * Populate tables.
     *
     * @return bool
     */
    private function createAndPopulateTables()
    {
        foreach ($this->arrTablesToMigrate as $arrTable) {
            $floatStartCopy = microtime(true);
            $intRecords     = 0;

            if (
                !$this->isDataOnly
                && !$this->createTable($arrTable['Tables_in_' . $this->strMySqlDbName])
            ) {
                return false;
            } else {
                list($intRecords, $failedRecords) = $this->populateTable($arrTable['Tables_in_' . $this->strMySqlDbName]);
            }

            $floatEndCopy             = microtime(true);
            $this->arrSummaryReport[] = [
                $this->strSchema . '.' . $arrTable['Tables_in_' . $this->strMySqlDbName],
                $intRecords,
                $failedRecords,
                round(($floatEndCopy - $floatStartCopy), 3) . ' seconds',
            ];

            unset($arrTable, $floatStartCopy, $floatEndCopy, $intRecords);
        }

        return true;
    }

    /**
     * Set table constraints.
     */
    private function createConstraints()
    {
        foreach ($this->arrTablesToMigrate as $arrTable) {
            $this->setTableConstraints($arrTable['Tables_in_' . $this->strMySqlDbName]);
            unset($arrTable);
        }
    }

    /**
     * Set foreign key constraints, then run "vacuum full" and "ANALYZE" for each table.
     */
    private function createForeignKeysAndRunVacuumFullAndAnalyze()
    {
        foreach ($this->arrTablesToMigrate as $arrTable) {
            $this->processForeignKey($arrTable['Tables_in_' . $this->strMySqlDbName]);
            $this->runVacuumFullAndAnalyze($arrTable['Tables_in_' . $this->strMySqlDbName]);
            unset($arrTable);
        }
    }

    /**
     * Attempt to create views.
     */
    private function createViews()
    {
        foreach ($this->arrViewsToMigrate as $arrView) {
            $this->createView($arrView['Tables_in_' . $this->strMySqlDbName]);
            unset($arrView);
        }
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
            PHP_EOL . "\t-- Migration began..." .
            ($this->isDataOnly ? PHP_EOL . "\t-- Only data will migrate." : '') .
            PHP_EOL
        );

        ini_set('memory_limit', '-1');

        /*
         * Create a database schema.
         */
        if (!$this->createSchema()) {
            $this->log('-- Script is terminated.' . PHP_EOL);
            exit;
        } else {
            $this->log('-- New schema "' . $this->strSchema . '" was successfully created...' . PHP_EOL);
        }

        if (!$this->loadStructureToMigrate()) {
            $this->log('-- Script is terminated.' . PHP_EOL);
            exit;
        } else {
            $intTablesCnt = count($this->arrTablesToMigrate);
            $this->log('-- ' . $intTablesCnt . ($intTablesCnt === 1 ? ' table ' : ' tables ') . 'detected' . PHP_EOL);
        }

        if (!$this->createAndPopulateTables()) {
            $this->log('-- Script is terminated.' . PHP_EOL);
            exit;
        }

        if (!$this->isDataOnly) {
            $this->createConstraints();
            $this->createForeignKeysAndRunVacuumFullAndAnalyze();
            $this->createViews();
        }

        /*
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
    }
}
