/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.sql;

import org.datanucleus.store.rdbms.table.Table;

/**
 * SQLTable namer that generates names like A0, B1, C0, ... , Z0, AA0, AB0, ... etc.
 * Tables are prefixed by a letter based on the table-group they are in, followed by a number
 * being the number within that table-group.
 * The candidate table-group will always be prefixed A when not predefined (i.e for JDOQL).
 * Should handle up to 26x27 combinations (i.e 702). Anyone needing more than that has serious problems, as does
 * their RDBMS.
 */
public class SQLTableAlphaNamer implements SQLTableNamer
{
    static String[] CHARS = new String[]{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
                                         "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"};

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.SQLTableNamer#getAliasForTable(org.datanucleus.store.rdbms.sql.SQLStatement, org.datanucleus.store.rdbms.DatastoreContainerObject)
     */
    public String getAliasForTable(SQLStatement stmt, Table table, String groupName)
    {
        SQLTableGroup tableGrp = stmt.tableGroups.get(groupName);
        String groupLetters = null;
        int numTablesInGroup = 0;
        if (tableGrp == null || tableGrp.getNumberOfTables() == 0)
        {
            // Take next available letter (assuming first group is A)
            int number = stmt.tableGroups.size();
            groupLetters = getLettersForNumber(number);

            // Check that this doesn't clash with predefined table aliases. Note, we allow for lowercase here too since some datastores (e.g Postgresql) convert to lower.
            boolean nameClashes = true;
            while (nameClashes)
            {
                if (stmt.primaryTable != null && stmt.primaryTable.alias.getName().equalsIgnoreCase(groupLetters))
                {
                    // Clashes with primary table of statement (assume case-insensitive)
                    number++;
                    groupLetters = getLettersForNumber(number);
                }
                else if (stmt.tables == null)
                {
                    // No other tables defined so ok
                    nameClashes = false;
                }
                else if (stmt.tables.containsKey(groupLetters) || stmt.tables.containsKey(groupLetters.toLowerCase())) // Try a predefined table of that letter
                {
                    // Clashes with other table
                    number++;
                    groupLetters = getLettersForNumber(number);
                }
                else if (stmt.tables.containsKey(groupLetters + "0") || stmt.tables.containsKey(groupLetters.toLowerCase() + "0")) // Try the first of that group
                {
                    // Clashes with other table
                    number++;
                    groupLetters = getLettersForNumber(number);
                }
                else
                {
                    nameClashes = false;
                }
            }

            numTablesInGroup = 0;
        }
        else
        {
            // Extract same letter from existing group
            SQLTable refSqlTbl = tableGrp.getTables()[0];
            String baseTableAlias = refSqlTbl.getAlias().toString();
            String quote = stmt.getRDBMSManager().getDatastoreAdapter().getIdentifierQuoteString();

            // Find start point of letter(s)
            int lettersStartPoint = 0;
            if (baseTableAlias.startsWith(quote))
            {
                // Omit any leading quote
                lettersStartPoint = quote.length();
            }

            int lettersLength = 1;
            if (baseTableAlias.length() > lettersStartPoint+1)
            {
                if (Character.isLetter(baseTableAlias.charAt(lettersStartPoint+1))) // This group is of the form "AA", "AB", etc
                {
                    lettersLength = 2;
                }
            }
            groupLetters = baseTableAlias.substring(lettersStartPoint, lettersStartPoint+lettersLength);

            // Find max number of tables in this group (allow for unions too)
            numTablesInGroup = tableGrp.getNumberOfTables();
            for (int i=0;i<stmt.getNumberOfUnions();i++)
            {
                int num = stmt.unions.get(i).getTableGroup(tableGrp.getName()).getNumberOfTables();
                if (num > numTablesInGroup)
                {
                    numTablesInGroup = num;
                }
            }
        }

        if (stmt.parent != null)
        {
            // Support 3 levels of subqueries. Any more than that and your query is inefficient anyway!
            if (stmt.parent.parent != null)
            {
                if (stmt.parent.parent.parent != null)
                {
                    return groupLetters + numTablesInGroup + "_SUB_SUB_SUB";
                }
                return groupLetters + numTablesInGroup + "_SUB_SUB";
            }
            return groupLetters + numTablesInGroup + "_SUB";
        }
        return groupLetters + numTablesInGroup;
    }

    private String getLettersForNumber(int number)
    {
        String groupLetters;
        if (number >= CHARS.length)
        {
            // "AA", "AB", "AC", etc
            groupLetters = CHARS[number / 26] + CHARS[number % 26];
        }
        else
        {
            // "A", "B", "C", etc
            groupLetters = CHARS[number];
        }
        return groupLetters;
    }
}