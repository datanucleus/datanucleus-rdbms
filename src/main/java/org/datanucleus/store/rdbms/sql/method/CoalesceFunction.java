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
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.ObjectExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.store.rdbms.sql.expression.TemporalExpression;
import org.datanucleus.util.Localiser;

/**
 * Expression handler to invoke the SQL COALESCE function.
 * For use in evaluating COALESCE({expr [,expr2[,expr3]]}) where the RDBMS supports this function.
 * All args must be of consistent expression types.
 * Returns an SQLExpression "COALESCE({expr})".
 */
public class CoalesceFunction implements SQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        if (expr == null)
        {
            // Find expression type that this handles - all expressions need to be consistent in our implementation
            Class exprType = null;

            Class cls = null;
            int clsLevel = 0;
            for (int i=0;i<args.size();i++)
            {
                SQLExpression argExpr = args.get(i);
                if (exprType == null)
                {
                    if (argExpr instanceof NumericExpression)
                    {
                        exprType = NumericExpression.class;
                        cls = Integer.class;
                    }
                    else if (argExpr instanceof StringExpression)
                    {
                        exprType = StringExpression.class;
                        cls = String.class;
                    }
                    else if (argExpr instanceof TemporalExpression)
                    {
                        exprType = TemporalExpression.class;
                        cls = argExpr.getJavaTypeMapping().getJavaType();
                    }
                    else
                    {
                        exprType = argExpr.getClass();
                        cls = argExpr.getJavaTypeMapping().getJavaType();
                    }
                }
                else
                {
                    if (!exprType.isAssignableFrom(argExpr.getClass()))
                    {
                        throw new NucleusUserException("COALESCE invocation first argument of type " + exprType.getName() + " yet subsequent argument of type " + argExpr.getClass().getName());
                    }
                }

                if (exprType == NumericExpression.class)
                {
                    // Priority order is Double, Float, BigDecimal, BigInteger, Long, Integer
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
            }

            SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
            if (exprType == NumericExpression.class)
            {
                return new NumericExpression(stmt, exprFactory.getMappingForType(cls, true), "COALESCE", args);
            }
            else if (exprType == StringExpression.class)
            {
                return new StringExpression(stmt, exprFactory.getMappingForType(cls, true), "COALESCE", args);
            }
            else if (exprType == TemporalExpression.class)
            {
                return new TemporalExpression(stmt, exprFactory.getMappingForType(cls, true), "COALESCE", args);
            }
            else
            {
                return new ObjectExpression(stmt, exprFactory.getMappingForType(cls, true), "COALESCE", args);
            }
        }

        throw new NucleusException(Localiser.msg("060002", "COALESCE", expr));
    }
}