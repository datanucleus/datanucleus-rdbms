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
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.util.Localiser;

/**
 * Expression handler to invoke the SQL ATAN2 function.
 * For use in evaluating ATAN2({expr1}, {expr2}) where the RDBMS supports this function.
 * Returns a NumericExpression "ATAN2({numExpr1}, {numExpr2})".
 */
public class Atan2Function implements SQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        if (expr == null)
        {
            if (args == null || args.size() != 2)
            {
                throw new NucleusException(Localiser.msg("060021", "ATAN2", 2));
            }
            return new NumericExpression(stmt, stmt.getSQLExpressionFactory().getMappingForType(double.class), "ATAN2", args);
        }

        throw new NucleusException(Localiser.msg("060002", "ATAN2", expr));
    }

}