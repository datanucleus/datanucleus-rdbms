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
 * Interface to be implemented by a class providing naming for SQL tables.
 */
public interface SQLTableNamer
{
    /**
     * Method to return the alias to use for the specified table.
     * @param stmt The statement where we will use the table
     * @param table The table
     * @param groupName Name of the table group
     * @return The alias to use
     */
    public String getAliasForTable(SQLStatement stmt, Table table, String groupName);
}