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

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.store.rdbms.sql.expression.StringLiteral;

/**
 * Method for trimming a String expression using LTRIM and RTRIM SQL functions.
 * Returns a StringExpression that equates to <pre>LTRIM(RTRIM(str))</pre>
 */
public class StringTrim2Method extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (expr instanceof StringLiteral)
        {
            String val = (String)((StringLiteral)expr).getValue();
            return new StringLiteral(stmt, expr.getJavaTypeMapping(), val.trim(), null);
        }

        ArrayList funcArgs = new ArrayList();
        funcArgs.add(expr);
        StringExpression strExpr = new StringExpression(stmt, getMappingForClass(String.class), "RTRIM", funcArgs);
        args.clear();
        args.add(strExpr);
        return new StringExpression(stmt, getMappingForClass(String.class), "LTRIM", args);
    }
}