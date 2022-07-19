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
package org.datanucleus.store.rdbms.sql.operation;

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;

/**
 * Implementation of MOD, using SQL MOD function.
 * Results in <pre>MOD(expr1, expr2)</pre> with some casts.
 */
public class Mod3Operation implements SQLOperation
{
    @Override
    public SQLExpression getExpression(SQLExpression expr, SQLExpression expr2)
    {
        List<SQLExpression> args = new ArrayList<>();

        List<String> types = new ArrayList<>();
        types.add("BIGINT");

        SQLExpressionFactory exprFactory = expr.getSQLStatement().getSQLExpressionFactory();
        List<SQLExpression> argsOp1 = new ArrayList<>();
        argsOp1.add(expr);
        args.add(new NumericExpression(expr.getSQLStatement(), exprFactory.getMappingForType(int.class), "CAST", argsOp1, types));

        List<SQLExpression> argsOp2 = new ArrayList<>();
        argsOp2.add(expr2);
        args.add(new NumericExpression(expr.getSQLStatement(), exprFactory.getMappingForType(int.class), "CAST", argsOp2, types));

        return new NumericExpression(expr.getSQLStatement(), exprFactory.getMappingForType(int.class), "MOD", args);
    }
}