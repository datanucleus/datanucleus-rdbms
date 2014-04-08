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

import java.util.List;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringLiteral;

/**
 * Method to allow inclusion of any SQL code that returns a boolean.
 * The original expression will be of the form
 * <pre>SQL_boolean("some sql")</pre>
 * which is compiled into
 * <pre>InvokeExpression{STATIC.SQL_boolean(Literal{some sql})</pre>
 */
public class SQLBooleanMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression ignore, List <SQLExpression>args)
    {
        if (args == null || args.size() != 1)
        {
            throw new NucleusUserException("Cannot invoke SQL_boolean() without a string argument");
        }

        SQLExpression expr = args.get(0);
        if (!(expr instanceof StringLiteral))
        {
           throw new NucleusUserException("Cannot use SQL_boolean() without string argument");
        }
        String sql = (String)((StringLiteral)expr).getValue();

        JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, false);
        BooleanExpression retExpr = new BooleanExpression(stmt, m, sql);
        return retExpr;
    }
}