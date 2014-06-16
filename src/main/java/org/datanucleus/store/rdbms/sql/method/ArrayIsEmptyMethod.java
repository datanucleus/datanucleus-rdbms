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

import java.lang.reflect.Array;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.expression.ArrayLiteral;
import org.datanucleus.store.rdbms.sql.expression.BooleanLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.util.Localiser;

/**
 * Method for evaluating {arrExpr1}.isEmpty().
 * Returns a BooleanExpression, utilising the size() expression. So the SQL will be something like
 * <PRE>
 * (SELECT COUNT(*) FROM ARRTABLE A0_SUB WHERE A0_SUB.OWNER_ID_OID = A0.OWNER_ID) = 0
 * </PRE>
 */
public class ArrayIsEmptyMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (args != null && args.size() > 0)
        {
            throw new NucleusException(Localiser.msg("060015", "isEmpty", "ArrayExpression"));
        }

        if (expr instanceof ArrayLiteral)
        {
            Object arr = ((ArrayLiteral)expr).getValue();
            boolean isEmpty = (arr == null || Array.getLength(arr) == 0);
            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, false);
            return new BooleanLiteral(stmt, m, isEmpty ? Boolean.TRUE : Boolean.FALSE);
        }

        SQLExpression sizeExpr = exprFactory.invokeMethod(stmt, "ARRAY", "size", expr, args);
        JavaTypeMapping mapping = exprFactory.getMappingForType(Integer.class, true);
        SQLExpression zeroExpr = exprFactory.newLiteral(stmt, mapping, 0);
        return sizeExpr.eq(zeroExpr);
    }
}
