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

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.expression.ObjectExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringLiteral;

/**
 * Method to allow inclusion of any SQL function invocation.
 * The original expression will be of the form
 * <pre>SQL_function("some sql", arg0[, arg1, ...])</pre>
 * which is compiled into
 * <pre>InvokeExpression{STATIC.SQL_function(Literal{some sql}, ...)</pre>
 */
public class SQLFunctionMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression ignore, List args)
    {
        if (args == null || args.size() < 1)
        {
            throw new NucleusUserException("Cannot invoke SQL_function() without first argument defining the function");
        }

        SQLExpression expr = (SQLExpression)args.get(0);
        if (!(expr instanceof StringLiteral))
        {
           throw new NucleusUserException("Cannot use SQL_function() without first argument defining the function");
        }
        String sql = (String)((StringLiteral)expr).getValue();

        List<SQLExpression> funcArgs = new ArrayList<>();
        if (args.size() > 1)
        {
            funcArgs.addAll(args.subList(1, args.size()));
        }

        // Return as ObjectExpression since we don't know the type
        JavaTypeMapping m = exprFactory.getMappingForType(Object.class, false);
        ObjectExpression retExpr = new ObjectExpression(stmt, m, sql, funcArgs);
        return retExpr;
    }
}