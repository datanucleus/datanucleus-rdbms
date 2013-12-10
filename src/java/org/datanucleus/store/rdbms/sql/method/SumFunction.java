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
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.expression.AggregateNumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;

/**
 * Expression handler to invoke the SQL SUM aggregation function.
 * For use in evaluating SUM({expr}) where the RDBMS supports this function.
 * Returns a NumericExpression "SUM({numericExpr})".
 */
public class SumFunction extends SimpleNumericAggregateMethod
{
    protected String getFunctionName()
    {
        return "SUM";
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List args)
    {
        if (expr == null)
        {
            if (args == null || args.size() != 1)
            {
                throw new NucleusException(getFunctionName() + " is only supported with a single argument");
            }
            JavaTypeMapping m = null;
            if (args.get(0) instanceof SQLExpression)
            {
                // Use same java type as the argument
                SQLExpression argExpr = (SQLExpression)args.get(0);
                Class cls = argExpr.getJavaTypeMapping().getJavaType();
                if (cls == Integer.class || cls == Short.class || cls == Long.class)
                {
                    m = getMappingForClass(Long.class);
                }
                else if (Number.class.isAssignableFrom(cls))
                {
                    m = getMappingForClass(argExpr.getJavaTypeMapping().getJavaType());
                }
                else
                {
                    throw new NucleusUserException("Cannot perform static SUM with arg of type " + cls.getName());
                }
            }
            else
            {
                // Fallback to the type for this aggregate
                m = getMappingForClass(getClassForMapping());
            }
            return new AggregateNumericExpression(stmt, m, getFunctionName(), args);
        }
        else
        {
            throw new NucleusException(LOCALISER.msg("060002", getFunctionName(), expr));
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SimpleAggregateMethod#getClassForMapping()
     */
    @Override
    protected Class getClassForMapping()
    {
        return double.class;
    }
}