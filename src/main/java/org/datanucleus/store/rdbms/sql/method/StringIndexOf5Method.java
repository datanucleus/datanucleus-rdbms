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
import org.datanucleus.store.query.expression.Expression;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.CaseNumericExpression;
import org.datanucleus.store.rdbms.sql.expression.CharacterExpression;
import org.datanucleus.store.rdbms.sql.expression.ExpressionUtils;
import org.datanucleus.store.rdbms.sql.expression.IntegerLiteral;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.ParameterLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.util.Localiser;

/**
 * Method for evaluating {strExpr1}.indexOf(strExpr2[,pos]) for PostgreSQL.
 * Note that this will not work if there is subsequent addition/subtraction of this value, but PostgreSQL doesn't have a simple function for this.
 * If pos is not specified then returns a NumericExpression equating to
 * <pre>STRPOS(strExpr1, strExpr2, numExpr1+1)-1</pre>
 * otherwise returns
 * <pre>
 * CASE WHEN (STRPOS(SUBSTR(STR_FIELD, START_POS)) &gt; 0) 
 *   THEN (STRPOS(SUBSTR(STR_FIELD, START_POS), STR) -1 + START_POS)
 * ELSE
 *   -1
 * </pre>
 */
public class StringIndexOf5Method implements SQLMethod
{
    @Override
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        if (args == null || args.size() == 0 || args.size() > 2)
        {
            throw new NucleusException(Localiser.msg("060003", "indexOf", "StringExpression", 0,
                "StringExpression/CharacterExpression/ParameterLiteral"));
        }

        // {stringExpr}.indexOf(strExpr1 [,numExpr2])
        SQLExpression substrExpr = args.get(0);
        if (!(substrExpr instanceof StringExpression) && !(substrExpr instanceof CharacterExpression) && !(substrExpr instanceof ParameterLiteral))
        {
            throw new NucleusException(Localiser.msg("060003", "indexOf", "StringExpression", 0,
                    "StringExpression/CharacterExpression/ParameterLiteral"));
        }

        if (args.size() == 1)
        {
            // strExpr.indexOf(str1)
            List<SQLExpression> funcArgs = new ArrayList<>();
            funcArgs.add(expr);
            funcArgs.add(substrExpr);

            SQLExpression oneExpr = ExpressionUtils.getLiteralForOne(stmt);
            NumericExpression locateExpr = new NumericExpression(stmt, stmt.getSQLExpressionFactory().getMappingForType(int.class, true), "STRPOS", funcArgs);
            return new NumericExpression(locateExpr, Expression.OP_SUB, oneExpr);
        }

        // strExpr.indexOf(str1, pos)
        SQLExpression fromExpr = args.get(1);
        if (!(fromExpr instanceof NumericExpression))
        {
            throw new NucleusException(Localiser.msg("060003", "indexOf", "StringExpression", 1, "NumericExpression"));
        }

        // Find the substring starting at this position
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        List<SQLExpression> substrArgs = List.of(fromExpr);
        SQLExpression strExpr = exprFactory.invokeMethod(stmt, "java.lang.String", "substring", expr, substrArgs);

        List<SQLExpression> funcArgs = new ArrayList<>();
        funcArgs.add(strExpr);
        funcArgs.add(substrExpr);
        NumericExpression locateExpr = new NumericExpression(stmt, stmt.getSQLExpressionFactory().getMappingForType(int.class, true), "STRPOS", funcArgs);

        SQLExpression[] whenExprs = new SQLExpression[1];
        whenExprs[0] = locateExpr.gt(new IntegerLiteral(stmt, exprFactory.getMappingForType(Integer.class, false), Integer.valueOf(0), null));

        SQLExpression[] actionExprs = new SQLExpression[1];
        NumericExpression posExpr1 = new NumericExpression(locateExpr, Expression.OP_SUB, ExpressionUtils.getLiteralForOne(stmt));
        actionExprs[0] = new NumericExpression(posExpr1, Expression.OP_ADD, fromExpr);

        SQLExpression elseExpr = new IntegerLiteral(stmt, exprFactory.getMappingForType(Integer.class, false), Integer.valueOf(-1), null);

        return new CaseNumericExpression(whenExprs, actionExprs, elseExpr);
    }
}