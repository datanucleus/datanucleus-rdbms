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

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.sql.expression.CaseExpression;
import org.datanucleus.store.rdbms.sql.expression.CharacterExpression;
import org.datanucleus.store.rdbms.sql.expression.ExpressionUtils;
import org.datanucleus.store.rdbms.sql.expression.IntegerLiteral;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.ParameterLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;

/**
 * Method for evaluating {strExpr1}.indexOf(strExpr2[,pos]).
 * If pos is not specified then returns a NumericExpression equating to
 * <pre>STRPOS(strExpr1, strExpr2, numExpr1+1)-1</pre>
 * otherwise returns
 * <pre>
 * CASE WHEN (STRPOS(SUBSTR(STR_FIELD, START_POS)) > 0) 
 *   THEN (STRPOS(SUBSTR(STR_FIELD, START_POS), STR) -1 + START_POS)
 * ELSE
 *   -1
 * </pre>
 */
public class StringIndexOf5Method extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List args)
    {
        if (args == null || args.size() == 0 || args.size() > 2)
        {
            throw new NucleusException(LOCALISER.msg("060003", "indexOf", "StringExpression", 0,
                "StringExpression/CharacterExpression/ParameterLiteral"));
        }
        else
        {
            // {stringExpr}.indexOf(strExpr1 [,numExpr2])
            SQLExpression substrExpr = (SQLExpression)args.get(0);
            if (!(substrExpr instanceof StringExpression) &&
                !(substrExpr instanceof CharacterExpression) &&
                !(substrExpr instanceof ParameterLiteral))
            {
                throw new NucleusException(LOCALISER.msg("060003", "indexOf", "StringExpression", 0,
                    "StringExpression/CharacterExpression/ParameterLiteral"));
            }

            ArrayList funcArgs = new ArrayList();
            if (args.size() == 1)
            {
                // strExpr.indexOf(str1)
                funcArgs.add(expr);
                funcArgs.add(substrExpr);
                SQLExpression oneExpr = ExpressionUtils.getLiteralForOne(stmt);
                NumericExpression locateExpr = new NumericExpression(stmt, getMappingForClass(int.class), "STRPOS", funcArgs);
                return new NumericExpression(locateExpr, Expression.OP_SUB, oneExpr);
            }
            else
            {
                // strExpr.indexOf(str1, pos)
                SQLExpression fromExpr = (SQLExpression)args.get(1);
                if (!(fromExpr instanceof NumericExpression))
                {
                    throw new NucleusException(LOCALISER.msg("060003", "indexOf", "StringExpression", 1,
                        "NumericExpression"));
                }

                // Find the substring starting at this position
                ArrayList substrArgs = new ArrayList(1);
                substrArgs.add(fromExpr);
                SQLExpression strExpr = exprFactory.invokeMethod(stmt, "java.lang.String", "substring", expr, substrArgs);

                funcArgs.add(strExpr);
                funcArgs.add(substrExpr);
                NumericExpression locateExpr = new NumericExpression(stmt, getMappingForClass(int.class), "STRPOS", funcArgs);

                SQLExpression[] whenExprs = new SQLExpression[1];
                NumericExpression zeroExpr = new IntegerLiteral(stmt, exprFactory.getMappingForType(Integer.class, false), Integer.valueOf(0), null);
                whenExprs[0] = locateExpr.gt(zeroExpr);

                SQLExpression[] actionExprs = new SQLExpression[1];
                SQLExpression oneExpr = ExpressionUtils.getLiteralForOne(stmt);
                NumericExpression posExpr1 = new NumericExpression(locateExpr, Expression.OP_SUB, oneExpr);
                actionExprs[0] = new NumericExpression(posExpr1, Expression.OP_ADD, fromExpr);

                SQLExpression elseExpr = new IntegerLiteral(stmt, exprFactory.getMappingForType(Integer.class, false), Integer.valueOf(-1), null);

                return new CaseExpression(whenExprs, actionExprs, elseExpr);
            }
        }
    }
}