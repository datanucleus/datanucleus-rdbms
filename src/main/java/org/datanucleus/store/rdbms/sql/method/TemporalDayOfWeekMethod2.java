/**********************************************************************
Copyright (c) 2017 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.sql.method;

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.ExpressionUtils;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;

/**
 * Method for evaluating DAY_OF_WEEK({dateExpr}) using PostgreSQL.
 * Returns a NumericExpression that equates to <pre>date_part("day", expr)+1</pre>
 */
public class TemporalDayOfWeekMethod2 extends TemporalBaseMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        SQLExpression invokedExpr = getInvokedExpression(expr, args, "DAY");

        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        JavaTypeMapping mapping = storeMgr.getMappingManager().getMapping(String.class);
        ArrayList funcArgs = new ArrayList();
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        funcArgs.add(exprFactory.newLiteral(stmt, mapping, "dow"));
        funcArgs.add(invokedExpr);

        // Add one to the SQL "dow" (origin=0) to be compatible with Java Calendar day of week (origin=1)
        SQLExpression one = ExpressionUtils.getLiteralForOne(stmt);
        NumericExpression numExpr = new NumericExpression(new NumericExpression(stmt, stmt.getSQLExpressionFactory().getMappingForType(int.class), "date_part", funcArgs), Expression.OP_ADD, one);
        numExpr.encloseInParentheses();
        return numExpr;
    }
}
