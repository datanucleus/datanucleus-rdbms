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

import java.util.Iterator;
import java.util.Map;

import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.BooleanSubqueryExpression;
import org.datanucleus.store.rdbms.table.Table;

/**
 * SQL DELETE Statement representation.
 * This will create a statement like
 * <pre>
 * DELETE FROM {tbl}
 * WHERE {boolExpr} [AND|OR] {boolExpr} ...
 * </pre>
 * Any joins are converted into a WHERE clause like
 * <pre>
 * EXISTS (SELECT * FROM OTHER_TBL ...)
 * </pre>
 */
public class DeleteStatement extends SQLStatement
{
    /**
     * Constructor for a DELETE statement.
     * @param rdbmsMgr Store Manager
     * @param table The primary table to DELETE
     * @param alias Alias for the primary table
     * @param tableGroupName Group name for the primary table
     * @param extensions Any extensions (optional)
     */
    public DeleteStatement(RDBMSStoreManager rdbmsMgr, Table table, DatastoreIdentifier alias, String tableGroupName, Map<String, Object> extensions)
    {
        super(null, rdbmsMgr, table, alias, tableGroupName, extensions);
    }

    public SQLText getSQLText()
    {
        if (sql != null)
        {
            return sql;
        }

        sql = new SQLText(rdbmsMgr.getDatastoreAdapter().getDeleteTableStatement(primaryTable));

        if (joins != null)
        {
            // Joins present so convert to "DELETE FROM MYTABLE WHERE EXISTS (SELECT * FROM OTHER_TBL ...)"
            Iterator<SQLJoin> joinIter = joins.iterator();

            // Create sub-statement selecting the first joined table, joining back to the outer statement
            SQLJoin subJoin = joinIter.next();
            SQLStatement subStmt = new SelectStatement(this, rdbmsMgr, subJoin.getTargetTable().getTable(), subJoin.getTargetTable().getAlias(), subJoin.getTargetTable().getGroupName());
            subStmt.whereAnd(subJoin.getCondition(), false);
            if (where != null)
            {
                // Move the WHERE clause to the sub-statement
                subStmt.whereAnd(where, false);
            }

            // Put any remaining joins into the sub-statement
            while (joinIter.hasNext())
            {
                SQLJoin join = joinIter.next();
                subStmt.joins.add(join);
            }

            // Set WHERE clause of outer statement to "EXISTS (sub-statement)"
            BooleanExpression existsExpr = new BooleanSubqueryExpression(this, "EXISTS", subStmt);
            where = existsExpr;
        }
        if (where != null)
        {
            sql.append(" WHERE ").append(where.toSQLText());
        }

        return sql;
    }
}