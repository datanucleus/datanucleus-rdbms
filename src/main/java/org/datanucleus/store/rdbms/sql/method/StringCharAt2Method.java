/**********************************************************************
 Copyright 2021 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 except in compliance with the License. You may obtain a copy of the License at

 https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software distributed under the
 License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 express or implied. See the License for the specific language governing permissions and
 limitations under the License.

 Contributors:
 2021 Yunus Durmus - Spanner support
 **********************************************************************/
package org.datanucleus.store.rdbms.sql.method;

import java.util.ArrayList;
import java.util.List;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.ExpressionUtils;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.ParameterLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.util.Localiser;

/**
 * Method for evaluating {strExpr}.charAt(numExpr).
 * Returns a StringExpression that equates to <pre>SUBSTR(strExpr, numExpr1, 1)</pre> for Cloud Spanner
 */
public class StringCharAt2Method implements SQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        if (args == null || args.size() == 0 || args.size() > 1)
        {
            throw new NucleusException(Localiser.msg("060003", "charAt", "StringExpression", 0, "NumericExpression/IntegerLiteral/ParameterLiteral"));
        }

        // {strExpr}.charAt(numExpr)
        SQLExpression numExpr = args.get(0);
        if (!(numExpr instanceof NumericExpression) && !(numExpr instanceof ParameterLiteral))
        {
            throw new NucleusException(Localiser.msg("060003", "charAt", "StringExpression", 0, "NumericExpression/IntegerLiteral/ParameterLiteral"));
        }

        // Invoke substring(startExpr, numExpr, 1)
        List<SQLExpression> funcArgs = new ArrayList<>();
        funcArgs.add(expr);
        funcArgs.add(numExpr);
        funcArgs.add(ExpressionUtils.getLiteralForOne(stmt));
        return new StringExpression(stmt, stmt.getSQLExpressionFactory().getMappingForType(String.class), "SUBSTR", funcArgs);
    }
}
