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
import org.datanucleus.store.rdbms.sql.expression.CharacterExpression;
import org.datanucleus.store.rdbms.sql.expression.ExpressionUtils;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.ParameterLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.util.Localiser;

/**
 * Method for evaluating {strExpr1}.indexOf(strExpr2[,pos]).
 * Returns a StrignExpression that equates to <pre>INSTR(strExpr1, strExpr2, numExpr1+1)-1</pre>
 */
public class StringIndexOf2Method extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (args == null || args.size() == 0 || args.size() > 2)
        {
            throw new NucleusException(Localiser.msg("060003", "indexOf", "StringExpression", 0, "StringExpression/CharacterExpression/ParameterLiteral"));
        }

        // {stringExpr}.indexOf(strExpr1 [,numExpr2])
        SQLExpression substrExpr = args.get(0);
        if (!(substrExpr instanceof StringExpression) && !(substrExpr instanceof CharacterExpression) && !(substrExpr instanceof ParameterLiteral))
        {
            throw new NucleusException(Localiser.msg("060003", "indexOf", "StringExpression", 0, "StringExpression/CharacterExpression/ParameterLiteral"));
        }

        SQLExpression one = ExpressionUtils.getLiteralForOne(stmt);

        ArrayList funcArgs = new ArrayList();
        funcArgs.add(expr);
        funcArgs.add(substrExpr);
        if (args.size() == 2)
        {
            SQLExpression fromExpr = args.get(1);
            if (!(fromExpr instanceof NumericExpression))
            {
                throw new NucleusException(Localiser.msg("060003", "indexOf", "StringExpression", 1, "NumericExpression"));
            }
            funcArgs.add(fromExpr.add(one));
        }
        NumericExpression locateExpr = new NumericExpression(stmt, getMappingForClass(int.class), "INSTR", funcArgs);
        return new NumericExpression(locateExpr, Expression.OP_SUB, one).encloseInParentheses();
    }
}
