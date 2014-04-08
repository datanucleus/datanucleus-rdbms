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

import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.sql.expression.AggregateNumericExpression;
import org.datanucleus.store.rdbms.sql.expression.ObjectExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;

/**
 * Expression handler to invoke the SQL COUNT aggregation function.
 * For use in evaluating COUNT({expr}) where the RDBMS supports this function.
 * Returns a NumericExpression "COUNT({numericExpr})".
 */
public class CountFunction extends AbstractSQLMethod
{
    protected String getFunctionName()
    {
        return "COUNT";
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (expr == null)
        {
            if (args == null || args.size() != 1)
            {
                throw new NucleusException("COUNT is only supported with a single argument");
            }

            SQLExpression argExpr = args.get(0);
            if (argExpr.getNumberOfSubExpressions() > 1 && argExpr instanceof ObjectExpression)
            {
                // Just use first sub-expression since count() doesn't allow more
                ((ObjectExpression)argExpr).useFirstColumnOnly();
            }
            return new AggregateNumericExpression(stmt, getMappingForClass(long.class), "COUNT", args);
        }
        else
        {
            throw new NucleusException(LOCALISER.msg("060002", "COUNT", expr));
        }
    }
}