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
package org.datanucleus.store.rdbms.sql.operation;

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;

/**
 * Implementation of CONCAT, using SQL CONCAT operator.
 * Results in
 * <pre>
 * CAST(expr1 AS VARCHAR(4000)) || CAST(expr2 AS VARCHAR(4000))
 * </pre>
 */
public class Concat3Operation extends AbstractSQLOperation
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.operation.SQLOperation#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    public SQLExpression getExpression(SQLExpression expr, SQLExpression expr2)
    {
        /*
         * We cast it to VARCHAR otherwise the concatenation in derby it is promoted to LONG VARCHAR.
         * 
         * In Derby, ? string parameters are promoted to LONG VARCHAR, and Derby
         * does not allow comparisons between LONG VARCHAR types. so the below 
         * example would be invalid in Derby
         * (THIS.FIRSTNAME||?) = ?
         * 
         * Due to that we convert it to
         * (CAST(THIS.FIRSTNAME||? AS VARCHAR(4000))) = ?
         * 
         * The only issue with this solution is for columns bigger than 4000 chars.
         * Secondly, if both operands are parameters, derby does not allow concatenation e.g.
         * ? || ? is not allowed by derby
         * 
         * so we do
         * CAST(? AS VARCHAR(4000)) || CAST(? AS VARCHAR(4000))
         *
         * If both situations happen,
         * (CAST(CAST( ? AS VARCHAR(4000) ) || CAST( ? AS VARCHAR(4000) ) AS VARCHAR(4000))) = ? 
         */
        JavaTypeMapping m = exprFactory.getMappingForType(String.class, false);

        List types = new ArrayList();
        types.add("VARCHAR(4000)");

        List argsOp1 = new ArrayList();
        argsOp1.add(expr);
        SQLExpression firstExpr = new StringExpression(expr.getSQLStatement(), m, "CAST", argsOp1, types).encloseInParentheses();

        List argsOp2 = new ArrayList();
        argsOp2.add(expr2);
        SQLExpression secondExpr = new StringExpression(expr.getSQLStatement(), m, "CAST", argsOp2, types).encloseInParentheses();

        StringExpression concatExpr = new StringExpression(expr.getSQLStatement(), null, null);
        SQLText sql = concatExpr.toSQLText();
        sql.clearStatement();
        sql.append(firstExpr);
        sql.append("||");
        sql.append(secondExpr);

        List args = new ArrayList();
        args.add(concatExpr);

        return new StringExpression(expr.getSQLStatement(), m, "CAST", args, types);
    }
}