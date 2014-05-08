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
import org.datanucleus.store.rdbms.table.Column;

/**
 * Representation of a column reference in an SQL statement.
 * Has a column, and an optional alias.
 * TODO Merge this with ColumnExpression
 */
public class SQLColumn
{
    /** The SQL Table that we are selecting columns from. */
    protected SQLTable table;

    /** The column being referenced. */
    protected Column column;

    /** Alias for the column, for use in a SELECT clause. */
    protected DatastoreIdentifier alias;

    /**
     * Constructor for a column reference.
     * @param table The SQLTable being selected
     * @param col The column
     * @param alias An alias
     */
    public SQLColumn(SQLTable table, Column col, DatastoreIdentifier alias)
    {
        this.table = table;
        this.column = col;
        this.alias = alias;
    }

    public SQLTable getTable()
    {
        return table;
    }

    public Column getColumn()
    {
        return column;
    }

    public DatastoreIdentifier getAlias()
    {
        return alias;
    }

    /**
     * Stringifier method to return this "column" in a form for use in SQL statements.
     * This can be of the following form(s)
     * <pre>
     * TABLEALIAS.MYCOLUMN AS COLUMNALIAS
     * MYTABLE.MYCOLUMN AS COLUMNALIAS
     * TABLEALIAS.MYCOLUMN
     * MYTABLE.MYCOLUMN
     * </pre>
     * Also applies any "select-function" defined on the Column.
     * @return The String form for use
     */
    public String toString()
    {
        String str = null;
        if (table.getAlias() != null)
        {
            str = table.getAlias() + "." + column.getIdentifier().toString();
        }
        else
        {
            str = table.getTable() + "." + column.getIdentifier().toString();
        }
        if (alias != null)
        {
            return column.applySelectFunction(str) + " AS " + alias;
        }
        else
        {
            return column.applySelectFunction(str);
        }
    }
}