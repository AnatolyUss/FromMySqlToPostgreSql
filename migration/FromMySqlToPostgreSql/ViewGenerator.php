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
 * This class translates mysql views into postgresql views.
 * 
 * @author Anatoly Khaytovich
 */
class ViewGenerator
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
     * Attempt to convert mysql view to postgresql view.
     * 
     * @param  string $strSchema
     * @param  string $strViewName
     * @param  string $strMySqlViewCode
     * @return string
     */
    public static function generateView($strSchema, $strViewName, $strMySqlViewCode)
    {
        $strMySqlViewCode = str_replace('`', '"', $strMySqlViewCode);
        $intQueryStart    = stripos($strMySqlViewCode, 'as');
        $strMySqlViewCode = substr($strMySqlViewCode, $intQueryStart);
        
        $arrMySqlViewCode      = explode(' ', $strMySqlViewCode);
        $intMySqlViewCodeCount = count($arrMySqlViewCode);
        
        for ($i = 0; $i < $intMySqlViewCodeCount; $i++) {
            if (
                ('from' == strtolower($arrMySqlViewCode[$i]) || 'join' == strtolower($arrMySqlViewCode[$i])) 
                && ($i + 1 < $intMySqlViewCodeCount)
            ) {
                // This code only handles a single set of ( in the code, we assume MySQL outputs the same way always.
                // Tables might be prefixed by (, so take care of that.
                $bracketSize = strpos($arrMySqlViewCode[$i + 1], '"');
                $brackets = $bracketSize == 0 ? '' : substr($arrMySqlViewCode[$i + 1], 0, $bracketSize);
                $tablename = substr($arrMySqlViewCode[$i + 1], $bracketSize);

                $arrMySqlViewCode[$i + 1] = $brackets . '"' . $strSchema . '".' . $tablename;

            }
        }
        
        return 'CREATE OR REPLACE VIEW "' . $strSchema . '"."' . $strViewName . '" ' . implode(' ', $arrMySqlViewCode);
    }
}
