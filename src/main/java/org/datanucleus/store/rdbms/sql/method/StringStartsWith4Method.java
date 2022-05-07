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
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.CharacterExpression;
import org.datanucleus.store.rdbms.sql.expression.ParameterLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.util.Localiser;

/**
 * Method for evaluating {strExpr1}.startsWith(strExpr2) for cloud spanner.
 * Returns a StringExpression that equates to <pre>STARTS_WITH(strExpr1, strExpr2)</pre>
 */
public class StringStartsWith4Method implements SQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        if (args == null || args.size() != 1)
        {
            throw new NucleusException(Localiser.msg("060003", "startswith", "StringExpression", 0, "StringExpression/CharacterExpression/Parameter"));
        }

        SQLExpression otherExpr = args.get(0);
        if (!(otherExpr instanceof StringExpression) && !(otherExpr instanceof CharacterExpression) && !(otherExpr instanceof ParameterLiteral))
        {
            throw new NucleusException(Localiser.msg("060003", "startswith", "StringExpression", 0, "StringExpression/CharacterExpression/Parameter"));
        }

        List<SQLExpression> funcArgs = new ArrayList<>();
        funcArgs.add(expr);
        funcArgs.add(otherExpr);
        return new BooleanExpression(stmt, stmt.getSQLExpressionFactory().getMappingForType(String.class), "STARTS_WITH", funcArgs);
    }
}
