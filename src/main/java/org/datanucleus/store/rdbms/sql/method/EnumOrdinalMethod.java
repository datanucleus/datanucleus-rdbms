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

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.expression.EnumExpression;
import org.datanucleus.store.rdbms.sql.expression.EnumLiteral;
import org.datanucleus.store.rdbms.sql.expression.IntegerLiteral;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.util.Localiser;

/**
 * Expression handler to evaluate {enumExpression}.ordinal().
 * Returns a NumericExpression.
 */
public class EnumOrdinalMethod implements SQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLStatement stmt, SQLExpression expr, List<SQLExpression> args)
    {
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        if (expr instanceof EnumLiteral)
        {
            Enum val = (Enum)((EnumLiteral)expr).getValue();
            return new IntegerLiteral(stmt, exprFactory.getMappingForType(int.class, false), val.ordinal(), null);
        }
        else if (expr instanceof EnumExpression)
        {
            EnumExpression enumExpr = (EnumExpression)expr;
            JavaTypeMapping m = enumExpr.getJavaTypeMapping();
            if (m.getJavaTypeForDatastoreMapping(0).equals(ClassNameConstants.JAVA_LANG_STRING))
            {
                throw new NucleusException("EnumExpression.ordinal is not supported when the enum is stored as a string");
            }

            return enumExpr.getDelegate();
        }
        else
        {
            throw new NucleusException(Localiser.msg("060001", "ordinal", expr));
        }
    }
}
