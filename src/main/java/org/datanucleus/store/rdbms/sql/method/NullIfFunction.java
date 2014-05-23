/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.util.Localiser;

/**
 * Expression handler to invoke the SQL NULLIF function.
 * For use in evaluating NULLIF({expr [,expr2[,expr3]]}) where the RDBMS supports this function.
 * Returns a NumericExpression "NULLIF({numericExpr})".
 */
public class NullIfFunction extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (expr == null)
        {
            Class cls = Integer.class;
            int clsLevel = 0;
            // Priority order is Double, Float, BigDecimal, BigInteger, Long, Integer
            for (int i=0;i<args.size();i++)
            {
                SQLExpression argExpr = args.get(i);
                Class argType = argExpr.getJavaTypeMapping().getJavaType();
                if (clsLevel < 5 && (argType == double.class || argType == Double.class))
                {
                    cls = Double.class;
                    clsLevel = 5;
                }
                else if (clsLevel < 4 && (argType == float.class || argType == Float.class))
                {
                    cls = Float.class;
                    clsLevel = 4;
                }
                else if (clsLevel < 3 && argType == BigDecimal.class)
                {
                    cls = BigDecimal.class;
                    clsLevel = 3;
                }
                else if (clsLevel < 2 && argType == BigInteger.class)
                {
                    cls = BigInteger.class;
                    clsLevel = 2;
                }
                else if (clsLevel < 1 && (argType == long.class || argType == Long.class))
                {
                    cls = Long.class;
                    clsLevel = 1;
                }
            }
            return new NumericExpression(stmt, getMappingForClass(cls), "NULLIF", args);
        }
        else
        {
            throw new NucleusException(Localiser.msg("060002", "NULLIF", expr));
        }
    }
}