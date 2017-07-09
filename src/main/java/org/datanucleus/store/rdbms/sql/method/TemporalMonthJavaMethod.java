/**********************************************************************
Copyright (c) 2016 Andy Jefferson and others. All rights reserved.
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

import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.ExpressionUtils;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;

/**
 * Method for evaluating {dateExpr}.getMonth().
 * Returns a NumericExpression that equates to <pre>MONTH(dateExpr)-1</pre>
 */
public class TemporalMonthJavaMethod extends TemporalBaseMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        SQLExpression invokedExpr = getInvokedExpression(expr, args, "MONTH_JAVA");

        ArrayList funcArgs = new ArrayList();
        funcArgs.add(invokedExpr);
        NumericExpression monthExpr = new NumericExpression(stmt, stmt.getSQLExpressionFactory().getMappingForType(int.class, true), "MONTH", funcArgs);

        // Delete one from the SQL "month" (origin=1) to be compatible with Java month (origin=0)
        SQLExpression one = ExpressionUtils.getLiteralForOne(stmt);
        NumericExpression numExpr = new NumericExpression(monthExpr, Expression.OP_SUB, one);
        numExpr.encloseInParentheses();
        return numExpr;
    }
}
