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
 * This class translates mysql data types into postgresql data types.
 * 
 * @author Anatoly Khaytovich
 */
class MapDataTypes
{
    /**
     * The purpose of explicit private constructor is 
     * to prevent an instance initialization.
     * 
     * @param void
     */
    private function __construct()
    {
        // No code should be put here.
    }
    
    /**
     * Dictionary of MySql data types with corresponding PostgreSql data types.
     * 
     * @var array
     */
    private static $arrMySqlPgSqlTypesMap = array(
        'bit' => array(
            'increased_size'           => 'smallint', 
            'type'                     => 'bit',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'year' => array(
            'increased_size'           => 'int', 
            'type'                     => 'smallint',
            'mySqlVarLenPgSqlFixedLen' => true,
        ),
        
        'tinyint' => array(
            'increased_size'           => 'int', 
            'type'                     => 'smallint',
            'mySqlVarLenPgSqlFixedLen' => true,
        ),
        
        'smallint' => array(
            'increased_size'           => 'int', 
            'type'                     => 'smallint',
            'mySqlVarLenPgSqlFixedLen' => true,
        ),
        
        'mediumint' => array(
            'increased_size'           => 'bigint', 
            'type'                     => 'int',
            'mySqlVarLenPgSqlFixedLen' => true,
        ),
        
        'int' => array(
            'increased_size'           => 'bigint', 
            'type'                     => 'int',
            'mySqlVarLenPgSqlFixedLen' => true,
        ),
        
        'bigint' => array(
            'increased_size'           => 'bigint', 
            'type'                     => 'bigint',
            'mySqlVarLenPgSqlFixedLen' => true,
        ),
        
        'float' => array(
            'increased_size'           => 'double precision', 
            'type'                     => 'real',
            'mySqlVarLenPgSqlFixedLen' => true,
        ),
        
        'double' => array(
            'increased_size'           => 'double precision', 
            'type'                     => 'double precision',
            'mySqlVarLenPgSqlFixedLen' => true,
        ),
        
        'double precision' => array(
            'increased_size'           => 'double precision', 
            'type'                     => 'double precision',
            'mySqlVarLenPgSqlFixedLen' => true,
        ),
        
        'numeric' => array(
            'increased_size'           => '', 
            'type'                     => 'numeric',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'decimal' => array(
            'increased_size'           => '', 
            'type'                     => 'decimal',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'decimal(19,2)' => array(
            'increased_size'           => 'numeric', 
            'type'                     => 'money',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'char' => array(
            'increased_size'           => '', 
            'type'                     => 'character',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'varchar' => array(
            'increased_size'           => '', 
            'type'                     => 'character varying',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'date' => array(
            'increased_size'           => '', 
            'type'                     => 'date',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'time' => array(
            'increased_size'           => '', 
            'type'                     => 'time',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'datetime' => array(
            'increased_size'           => '', 
            'type'                     => 'timestamp',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'timestamp' => array(
            'increased_size'           => '', 
            'type'                     => 'timestamp',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'point' => array(
            'increased_size'           => '', 
            'type'                     => 'point',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'linestring' => array(
            'increased_size'           => '', 
            'type'                     => 'line',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'polygon' => array(
            'increased_size'           => '', 
            'type'                     => 'polygon',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'enum' => array(
            'increased_size'           => '', 
            'type'                     => 'character varying(255)',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'tinytext' => array(
            'increased_size'           => '', 
            'type'                     => 'text',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'mediumtext' => array(
            'increased_size'           => '', 
            'type'                     => 'text',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'longtext' => array(
            'increased_size'           => '', 
            'type'                     => 'text',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'text' => array(
            'increased_size'           => '', 
            'type'                     => 'text',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'varbinary' => array(
            'increased_size'           => '', 
            'type'                     => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => true,
        ),
        
        'binary' => array(
            'increased_size'           => '', 
            'type'                     => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => true,
        ),
        
        'tinyblob' => array(
            'increased_size'           => '', 
            'type'                     => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'mediumblob' => array(
            'increased_size'           => '', 
            'type'                     => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'longblob' => array(
            'increased_size'           => '', 
            'type'                     => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
        
        'blob' => array(
            'increased_size'           => '', 
            'type'                     => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => false,
        ),
    );
    
    /**
     * Translate mysql data types into postgresql data types.
     * 
     * @param  string $strMySqlDataType
     * @return string
     */
    public static function map($strMySqlDataType)
    {
        $strRetVal                  = '';
        $arrDataTypeDetails         = explode(' ', $strMySqlDataType);
        $boolIncreaseOriginalSize   = in_array('unsigned', $arrDataTypeDetails) || in_array('zerofill', $arrDataTypeDetails);
        $strMySqlDataType           = $arrDataTypeDetails[0];
        $strMySqlDataType           = strtolower($strMySqlDataType);
        $parenthesesFirstOccurrence = strpos($strMySqlDataType, '(');
        $parenthesesLastOccurrence  = false;
        
        if (false === $parenthesesFirstOccurrence) {
            // No parentheses detected.
            $strRetVal = $boolIncreaseOriginalSize 
                       ? self::$arrMySqlPgSqlTypesMap[$strMySqlDataType]['increased_size']
                       : self::$arrMySqlPgSqlTypesMap[$strMySqlDataType]['type'];
            
        } else {
            // Parentheses detected.
            $parenthesesLastOccurrence = strpos($strMySqlDataType, ')');
            $arrDataType               = explode('(', $strMySqlDataType);
            $strDataType               = strtolower($arrDataType[0]);
            
            if ('enum' == $strDataType) {
                $strRetVal = 'varchar(255)';
            } elseif ('decimal' == $strDataType || 'numeric' == $strDataType) {
                $strRetVal = self::$arrMySqlPgSqlTypesMap[$strDataType]['type'] . '(' . $arrDataType[1];
            } elseif ('decimal(19,2)' == $strMySqlDataType) {
                $strRetVal = $boolIncreaseOriginalSize 
                           ? self::$arrMySqlPgSqlTypesMap[$strDataType]['increased_size']
                           : self::$arrMySqlPgSqlTypesMap[$strDataType]['type'];
                
            } elseif (self::$arrMySqlPgSqlTypesMap[$strDataType]['mySqlVarLenPgSqlFixedLen']) {
                // Should be converted without a length definition.
                $strRetVal = $boolIncreaseOriginalSize 
                           ? self::$arrMySqlPgSqlTypesMap[$strDataType]['increased_size']
                           : self::$arrMySqlPgSqlTypesMap[$strDataType]['type'];
                
            } else {
                // Should be converted with a length definition.
                $strRetVal = $boolIncreaseOriginalSize 
                           ? self::$arrMySqlPgSqlTypesMap[$strDataType]['increased_size'] . '(' . $arrDataType[1]
                           : self::$arrMySqlPgSqlTypesMap[$strDataType]['type'] . '(' . $arrDataType[1];
            }
            
            // Prevent incompatible length (CHARACTER(0) or CHARACTER VARYING(0)).
            switch ($strRetVal) {
                case 'character(0)':
                    $strRetVal = 'character(1)';
                    break;
                
                case 'character varying(0)':
                    $strRetVal = 'character varying(1)';
                    break;
            }
        }
        
        return ' ' . strtoupper($strRetVal) . ' ';
    }
}
