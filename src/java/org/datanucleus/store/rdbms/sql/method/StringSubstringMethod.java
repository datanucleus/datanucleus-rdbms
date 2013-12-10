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
package org.datanucleus.store.rdbms.sql.method;

import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.sql.expression.ExpressionUtils;
import org.datanucleus.store.rdbms.sql.expression.IntegerLiteral;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.ParameterLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;

/**
 * Method for evaluating {strExpr}.substring(numExpr1 [,numExpr2]).
 * Returns a StringExpression that equates to
 * <ul>
 * <li><pre>SUBSTRING(strExpr FROM numExpr1+1)</pre> when no end position provided.</li>
 * <li><pre>SUBSTRING(strExpr FROM numExpr1+1 FOR numExpr2-numExpr1)</pre> when end position provided.</li>
 * </ul>
 */
public class StringSubstringMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List args)
    {
        if (args == null || args.size() == 0 || args.size() > 2)
        {
            throw new NucleusException(LOCALISER.msg("060003", "substring", "StringExpression", 0,
                "NumericExpression/IntegerLiteral/ParameterLiteral"));
        }
        else if (args.size() == 1)
        {
            // {stringExpr}.substring(numExpr1)
            SQLExpression one = ExpressionUtils.getLiteralForOne(stmt);

            SQLExpression startExpr = (SQLExpression)args.get(0);
            if (!(startExpr instanceof NumericExpression) &&
                !(startExpr instanceof IntegerLiteral) &&
                !(startExpr instanceof ParameterLiteral))
            {
                throw new NucleusException(LOCALISER.msg("060003", "substring", "StringExpression", 0,
                    "NumericExpression/IntegerLiteral/ParameterLiteral"));
            }

            // Create a new StringExpression and manually update its SQL
            StringExpression strExpr = new StringExpression(stmt, null, null);
            SQLText sql = strExpr.toSQLText();
            sql.append("SUBSTRING(").append(expr).append(" FROM ").append(startExpr.add(one)).append(')');
            return strExpr;
        }
        else
        {
            // {stringExpr}.substring(numExpr1, numExpr2)
            SQLExpression one = ExpressionUtils.getLiteralForOne(stmt);

            SQLExpression startExpr = (SQLExpression)args.get(0);
            if (!(startExpr instanceof NumericExpression))
            {
                throw new NucleusException(LOCALISER.msg("060003", "substring", "StringExpression", 0,
                    "NumericExpression"));
            }
            SQLExpression endExpr = (SQLExpression)args.get(1);
            if (!(endExpr instanceof NumericExpression))
            {
                throw new NucleusException(LOCALISER.msg("060003", "substring", "StringExpression", 1,
                    "NumericExpression"));
            }

            // Create a new StringExpression and manually update its SQL
            StringExpression strExpr = new StringExpression(stmt, null, null);
            SQLText sql = strExpr.toSQLText();
            sql.append("SUBSTRING(").append(expr).append(" FROM ").append(startExpr.add(one))
                .append(" FOR ").append(endExpr.sub(startExpr)).append(')');
            return strExpr;
        }
    }
}
