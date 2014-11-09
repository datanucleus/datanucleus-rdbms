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
package org.datanucleus.store.rdbms.sql.method;

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.sql.expression.ExpressionUtils;
import org.datanucleus.store.rdbms.sql.expression.IntegerLiteral;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.ParameterLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.util.Localiser;

/**
 * Method for evaluating {strExpr}.charAt(numExpr).
 * Returns a StringExpression that equates to
 * <ul>
 * <li><pre>SUBSTRING(strExpr FROM numExpr1+1)</pre> when no end position provided.</li>
 * <li><pre>SUBSTRING(strExpr FROM numExpr1+1 FOR numExpr2-numExpr1)</pre> when end position provided.</li>
 * </ul>
 */
public class StringCharAtMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (args == null || args.size() == 0 || args.size() > 1)
        {
            throw new NucleusException(Localiser.msg("060003", "charAt", "StringExpression", 0, "NumericExpression/IntegerLiteral/ParameterLiteral"));
        }

        // {strExpr}.charAt(numExpr)
        SQLExpression startExpr = args.get(0);
        if (!(startExpr instanceof NumericExpression) && !(startExpr instanceof IntegerLiteral) && !(startExpr instanceof ParameterLiteral))
        {
            throw new NucleusException(Localiser.msg("060003", "charAt", "StringExpression", 0, "NumericExpression/IntegerLiteral/ParameterLiteral"));
        }

        SQLExpression endExpr = startExpr.add(ExpressionUtils.getLiteralForOne(stmt));

        // Invoke substring(startExpr, endExpr)
        List<SQLExpression> newArgs = new ArrayList<SQLExpression>(2);
        newArgs.add(startExpr);
        newArgs.add(endExpr);
        return exprFactory.invokeMethod(stmt, String.class.getName(), "substring", expr, newArgs);
    }
}
