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
package org.datanucleus.store.rdbms.sql.operation;

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.sql.expression.SQLLiteral;
import org.datanucleus.store.rdbms.sql.expression.StringExpression;
import org.datanucleus.store.rdbms.sql.expression.StringLiteral;

/**
 * Implementation of a conversion from a NumericExpression to a StringExpression.
 * Results in <pre>RTRIM(CHAR(expr))</pre>
 */
public class NumericToString3Operation implements SQLOperation
{
    @Override
    public SQLExpression getExpression(SQLExpression expr, SQLExpression expr2)
    {
        SQLExpressionFactory exprFactory = expr.getSQLStatement().getSQLExpressionFactory();
        JavaTypeMapping m = exprFactory.getMappingForType(String.class, false);
        if (expr instanceof SQLLiteral)
        {
            // Just convert the literal value directly
            if (((SQLLiteral)expr).getValue() == null)
            {
                return new StringLiteral(expr.getSQLStatement(), m, null, null);
            }
            return new StringLiteral(expr.getSQLStatement(), m, ((SQLLiteral)expr).getValue().toString(), null);
        }

        List<SQLExpression> args = new ArrayList<>();
        args.add(expr);
        List<SQLExpression> trimArgs = new ArrayList<>();
        trimArgs.add(new StringExpression(expr.getSQLStatement(), m, "CHAR", args));
        return new StringExpression(expr.getSQLStatement(), m, "RTRIM", trimArgs);
    }
}