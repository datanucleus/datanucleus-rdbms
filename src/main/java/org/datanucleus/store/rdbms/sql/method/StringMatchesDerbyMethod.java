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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.ExpressionUtils;
import org.datanucleus.store.rdbms.sql.expression.NumericExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;

/**
 * Expression handler to evaluate {stringExpression}.matches(StringExpression) for Derby.
 * Adds its own handling of the case of "{StringExpression} LIKE {StringExpression}" case
 * using a Java function NUCLEUS_MATCHES in the datastore since Derby doesn't support "LIKE".
 * Note that any input escape character is ignored.
 */
public class StringMatchesDerbyMethod extends StringMatchesMethod
{
    protected BooleanExpression getExpressionForStringExpressionInput(SQLExpression expr, 
            SQLExpression argExpr, SQLExpression escapeExpr)
    {
        // Use Derby "NUCLEUS_MATCHES" function
        List funcArgs = new ArrayList();
        funcArgs.add(expr);
        funcArgs.add(argExpr);
        JavaTypeMapping m = exprFactory.getMappingForType(BigInteger.class, false);
        SQLExpression one = ExpressionUtils.getLiteralForOne(stmt);
        return new NumericExpression(stmt, m, "NUCLEUS_MATCHES", funcArgs).eq(one);
    }
}