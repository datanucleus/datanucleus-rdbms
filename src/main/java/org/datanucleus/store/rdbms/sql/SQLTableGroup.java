/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
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

import java.util.HashMap;
import java.util.Map;

import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.util.StringUtils;

/**
 * Group of tables in an SQL statement.
 * Tables are grouped to represent a particular object, so equates to an inheritance tree.
 * In this way, if we have a class B which extends class A and they have tables B and A
 * respectively then tables B and A will be in the same group when related to that object.
 */
public class SQLTableGroup
{
    /** Name of this group. */
    String name;

    /** Type of join to this group (from the candidate group presumably). */
    JoinType joinType = null;

    /** Map of tables in this group, keyed by their alias. */
    Map<String, SQLTable> tablesByAlias = new HashMap<>();

    /**
     * Constructor for a group with this name.
     * @param name Name of the group
     */
    SQLTableGroup(String name, JoinType joinType)
    {
        this.name = name;
        this.joinType = joinType;
    }

    public String getName()
    {
        return name;
    }

    public JoinType getJoinType()
    {
        return joinType;
    }

    public void addTable(SQLTable tbl)
    {
        tablesByAlias.put(tbl.getAlias().toString(), tbl);
    }

    public int getNumberOfTables()
    {
        return tablesByAlias.size();
    }

    public SQLTable[] getTables()
    {
        return tablesByAlias.values().toArray(new SQLTable[tablesByAlias.size()]);
    }

    public String toString()
    {
        return "SQLTableGroup: " + name + " join=" + joinType + " tables=" + StringUtils.mapToString(tablesByAlias);
    }
}