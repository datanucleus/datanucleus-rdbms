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

import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.table.Table;

/**
 * Representation of a table reference in an SQL statement.
 * Has a table, and an alias.
 */
public class SQLTable
{
    protected SQLStatement stmt;
    protected Table table;
    protected DatastoreIdentifier alias;
    protected String groupName;

    /**
     * Constructor for a table involved in an SQLStatement.
     * Package permission so that it can't be created from other packages - i.e to restrict construction
     * to SQLStatement
     * @param stmt Statement that this table relates to
     * @param tbl The underlying table
     * @param alias Alias to use for this table in the SQLStatement
     * @param grpName Name of the group this table is in
     */
    SQLTable(SQLStatement stmt, Table tbl, DatastoreIdentifier alias, String grpName)
    {
        this.stmt = stmt;
        this.table = tbl;
        this.alias = alias;
        this.groupName = grpName;
    }

    public SQLStatement getSQLStatement()
    {
        return stmt;
    }

    public Table getTable()
    {
        return table;
    }

    public DatastoreIdentifier getAlias()
    {
        return alias;
    }

    public String getGroupName()
    {
        return groupName;
    }

    public int hashCode()
    {
        if (alias != null)
        {
            return alias.hashCode() ^ table.hashCode();
        }

        return table.hashCode();
    }

    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (!(obj instanceof SQLTable))
        {
            return false;
        }
        SQLTable other = (SQLTable)obj;
        if (other.alias == null)
        {
            return other.table == table && alias == null;
        }

        return other.table == table && other.alias.equals(alias);
    }

    /**
     * Stringifier method to return this "table" in a form for use in SQL statements.
     * This can be of the following form(s)
     * <pre>
     * MYTABLE MYALIAS
     * MYTABLE
     * </pre>
     * @return The String form for use
     */
    public String toString()
    {
        if (alias != null)
        {
            return table.toString() + " " + alias.toString();
        }
        return table.toString();
    }
}