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
 * SQLTable namer that generates names like T0, T1, T2, etc.
 * T0 is the primary table of the statement.
 * Doesn't make any use of table-groups, just incrementing the number for each new table.
 */
public class SQLTableTNamer implements SQLTableNamer
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.SQLTableNamer#getAliasForTable(org.datanucleus.store.rdbms.sql.SQLStatement, org.datanucleus.store.rdbms.DatastoreContainerObject)
     */
    public String getAliasForTable(SQLStatement stmt, Table table, String groupName)
    {
        int number = 0; // Primary table is number '0'
        if (stmt.getPrimaryTable() != null)
        {
            // Find max number of tables (allow for unions too)
            int numTables = stmt.getNumberOfTables();
            for (int i=0;i<stmt.getNumberOfUnions();i++)
            {
                int num = stmt.unions.get(i).getNumberOfTables();
                if (num > numTables)
                {
                    numTables = num;
                }
            }

            // Increment from current max number (already got primary table)
            number = (numTables > 0 ? numTables + 1 : 1);
        }

        if (stmt.getParentStatement() != null)
        {
            // Support 3 levels of subqueries. Any more than that and your query is inefficient anyway!
            if (stmt.getParentStatement().getParentStatement() != null)
            {
                if (stmt.getParentStatement().getParentStatement().getParentStatement() != null)
                {
                    return "T" + number + "_SUB_SUB_SUB";
                }
                return "T" + number + "_SUB_SUB";
            }
            return "T" + number + "_SUB";
        }
        return "T" + number;
    }
}