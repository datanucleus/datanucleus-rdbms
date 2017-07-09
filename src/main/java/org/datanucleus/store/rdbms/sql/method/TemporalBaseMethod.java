/**********************************************************************
Copyright (c) 2016 Andy Jefferson and others. All rights reserved.
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
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.TemporalExpression;
import org.datanucleus.store.rdbms.sql.expression.TypeConverterExpression;
import org.datanucleus.util.Localiser;

/**
 * Base for all temporal methods.
 */
public abstract class TemporalBaseMethod implements SQLMethod
{
    public SQLExpression getInvokedExpression(SQLExpression expr, List<SQLExpression> args, String methodName)
    {
        SQLExpression invokedExpr = expr;
        if (expr == null)
        {
            if (args == null || args.size() != 1)
            {
                throw new NucleusException("Cannot invoke " + methodName + " without 1 argument");
            }
            invokedExpr = args.get(0);
        }

        if (invokedExpr instanceof TemporalExpression)
        {
            // Directly on a TemporalExpression
        }
        else if (invokedExpr instanceof TypeConverterExpression && ((TypeConverterExpression)invokedExpr).getDelegate() instanceof TemporalExpression)
        {
            // Converted using a TypeConverter to a TemporalExpression
        }
        else
        {
            throw new NucleusException(Localiser.msg("060001", methodName, invokedExpr));
        }

        return invokedExpr;
    }
}