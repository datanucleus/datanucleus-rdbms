/**********************************************************************
Copyright (c) 2015 Andy Jefferson and others. All rights reserved.
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
import org.datanucleus.store.rdbms.sql.expression.AggregateNumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.util.Localiser;

/**
 * Expression handler to invoke the SQL COUNT(*) aggregation function.
 * Returns a NumericExpression "COUNT(*)".
 */
public class CountStarFunction implements SQLMethod
{
    protected String getFunctionName()
    {
        return "COUNT";
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        if (expr == null)
        {
            if (args != null && args.size() > 0)
            {
                throw new NucleusException("COUNTSTAR takes no argument");
            }

            return new AggregateNumericExpression(stmt, stmt.getSQLExpressionFactory().getMappingForType(long.class, true), "COUNT(*)");
        }

        throw new NucleusException(Localiser.msg("060002", "COUNT", expr));
    }
}