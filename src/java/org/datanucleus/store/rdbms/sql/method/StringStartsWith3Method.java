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
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.CharacterExpression;
import org.datanucleus.store.rdbms.sql.expression.ExpressionUtils;
import org.datanucleus.store.rdbms.sql.expression.ParameterLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;

/**
 * Method for evaluating {strExpr1}.startsWith(strExpr2[,numExpr]).
 * Returns a StringExpression that equates to <pre>LOCATE(strExpr2, strExpr1[, numExpr]) = 1</pre>
 */
public class StringStartsWith3Method extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (args == null || args.size() == 0 || args.size() > 2)
        {
            throw new NucleusException(LOCALISER.msg("060003", "startsWith", "StringExpression", 0,
                "StringExpression/CharacterExpression/Parameter"));
        }
        else
        {
            // {stringExpr}.indexOf(strExpr1 [,numExpr2])
            ArrayList funcArgs = new ArrayList();
            SQLExpression substrExpr = args.get(0);
            if (!(substrExpr instanceof StringExpression) &&
                !(substrExpr instanceof CharacterExpression) &&
                !(substrExpr instanceof ParameterLiteral))
            {
                throw new NucleusException(LOCALISER.msg("060003", "startsWith", "StringExpression", 0,
                    "StringExpression/CharacterExpression/Parameter"));
            }

            SQLExpression one = ExpressionUtils.getLiteralForOne(stmt);
            if (args.size() > 1)
            {
                SQLExpression numExpr = args.get(1);
                funcArgs.add(substrExpr);
                funcArgs.add(expr);
                return new BooleanExpression(
                    new StringExpression(stmt, getMappingForClass(int.class), "LOCATE", funcArgs), Expression.OP_EQ, one.add(numExpr));
            }
            else
            {
                funcArgs.add(substrExpr);
                funcArgs.add(expr);
                return new BooleanExpression(
                    new StringExpression(stmt, getMappingForClass(int.class), "LOCATE", funcArgs), Expression.OP_EQ, one);
            }
        }
    }
}
