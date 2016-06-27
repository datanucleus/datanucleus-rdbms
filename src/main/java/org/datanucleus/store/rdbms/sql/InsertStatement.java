/**********************************************************************
Copyright (c) 2015 Andy Jefferson and others. All rights reserved.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.table.Table;

/**
 * SQL INSERT Statement representation.
 * This will create a statement like
 * <pre>
 * INSERT INTO {tbl} (col1, col2, ...)
 * SELECT ...
 * </pre>
 * TODO Support INSERT INTO {tbl} (col1, col2, ...) VALUES (...)
 */
public class InsertStatement extends SQLStatement
{
    List<SQLExpression> columnList = new ArrayList<>();

    SelectStatement selectStmt;

    /**
     * Constructor for an INSERT statement.
     * @param rdbmsMgr Store Manager
     * @param table The primary table to INSERT
     * @param alias Alias for the primary table
     * @param tableGroupName Group name for the primary table
     * @param extensions Any extensions (optional)
     */
    public InsertStatement(RDBMSStoreManager rdbmsMgr, Table table, DatastoreIdentifier alias, String tableGroupName, Map<String, Object> extensions)
    {
        super(null, rdbmsMgr, table, alias, tableGroupName, extensions);
    }

    public void addColumn(SQLExpression expr)
    {
        columnList.add(expr);
    }

    public void setSelectStatement(SelectStatement selectStmt)
    {
        this.selectStmt = selectStmt;
    }
    public SelectStatement getSelectStatement()
    {
        return selectStmt;
    }

    public synchronized SQLText getSQLText()
    {
        if (sql != null)
        {
            return sql;
        }

        sql = new SQLText("INSERT INTO ");
        sql.append(primaryTable.getTable().toString());
        sql.append('(');
        Iterator<SQLExpression> columnListIter = columnList.iterator();
        while (columnListIter.hasNext())
        {
            SQLExpression colExpr = columnListIter.next();
            sql.append(colExpr.toSQLText());
            if (columnListIter.hasNext())
            {
                sql.append(',');
            }
        }
        sql.append(") ");

        if (selectStmt != null)
        {
            sql.append(selectStmt.getSQLText());
        }

        return sql;
    }
}
