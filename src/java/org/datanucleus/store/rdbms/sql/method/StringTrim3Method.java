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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;

/**
 * Method for evaluating {strExpr1}.trim() or "TRIM(BOTH trimChar FROM strExpr1)".
 * Returns a StrignExpression that equates to <pre>TRIM([[BOTH] [{trim_char}] FROM] strExpr)</pre>
 */
public class StringTrim3Method extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (args != null && args.size() > 1)
        {
            throw new NucleusException("TRIM has incorrect number of args");
        }
        else
        {
            // {stringExpr}.trim(trimChar)
            SQLExpression trimCharExpr = null;
            if (args != null && args.size() > 0)
            {
                trimCharExpr = args.get(0);
            }

            List trimArgs = new ArrayList();
            if (trimCharExpr == null)
            {
                trimArgs.add(expr);
            }
            else
            {
                StringExpression argExpr = new StringExpression(stmt, expr.getJavaTypeMapping(), "NULL", null);
                SQLText sql = argExpr.toSQLText();
                sql.clearStatement();
                sql.append(getTrimSpecKeyword() + " ");
                sql.append(trimCharExpr);
                sql.append(" FROM ");
                sql.append(expr);
                trimArgs.add(argExpr);
            }

            StringExpression trimExpr = new StringExpression(stmt, expr.getJavaTypeMapping(), "TRIM", trimArgs);
            return trimExpr;
        }
    }

    protected String getTrimSpecKeyword()
    {
        return "BOTH";
    }
}