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
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;

/**
 * Expression handler to invoke an SQL numeric function that takes in argument(s).
 * <ul>
 * <li>If the expression is null will return a NumericExpression
 *     <pre>{functionName}({args})</pre> and expr isn't used</li>
 * </ul>
 */
public abstract class SimpleNumericMethod extends AbstractSQLMethod
{
    protected abstract String getFunctionName();

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (expr == null)
        {
            // Assume that we have something like "ABS({expr})"
            return new NumericExpression(stmt, getMappingForClass(getClassForMapping()), getFunctionName(), args);
        }
        else
        {
            throw new NucleusException(LOCALISER.msg("060002", getFunctionName(), expr));
        }
    }

    protected abstract Class getClassForMapping();
}