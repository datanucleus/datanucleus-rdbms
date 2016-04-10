/**********************************************************************
Copyright (c) 2016 Andy Jefferson and others. All rights reserved.
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
import org.datanucleus.store.rdbms.mapping.java.BooleanMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.OptionalMapping;
import org.datanucleus.store.rdbms.mapping.java.StringMapping;
import org.datanucleus.store.rdbms.sql.expression.CaseBooleanExpression;
import org.datanucleus.store.rdbms.sql.expression.CaseExpression;
import org.datanucleus.store.rdbms.sql.expression.CaseNumericExpression;
import org.datanucleus.store.rdbms.sql.expression.CaseStringExpression;
import org.datanucleus.store.rdbms.sql.expression.NullLiteral;
import org.datanucleus.store.rdbms.sql.expression.OptionalExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;

/**
 * Method for evaluating {optionalExpr1}.orElse().
 * Returns a XXXExpression representing "CASE WHEN col IS NOT NULL THEN col ELSE otherVal END".
 */
public class OptionalOrElseMethod extends AbstractSQLMethod
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.method.SQLMethod#getExpression(org.datanucleus.store.rdbms.sql.expression.SQLExpression, java.util.List)
     */
    public SQLExpression getExpression(SQLExpression expr, List<SQLExpression> args)
    {
        if (args != null && args.size() != 1)
        {
            throw new NucleusException("Optional.orElse should be passed 1 argument");
        }

        SQLExpression elseExpr = args.get(0);

        OptionalMapping opMapping = (OptionalMapping) ((OptionalExpression)expr).getJavaTypeMapping();
        JavaTypeMapping javaMapping = opMapping.getWrappedMapping();
        SQLExpression getExpr = exprFactory.newExpression(stmt, expr.getSQLTable(),javaMapping);
        SQLExpression isNotNullExpr = exprFactory.newExpression(stmt, expr.getSQLTable(),javaMapping).ne(new NullLiteral(stmt, javaMapping, null, null));
        if (javaMapping instanceof StringMapping)
        {
            return new CaseStringExpression(new SQLExpression[] {isNotNullExpr}, new SQLExpression[] {getExpr}, elseExpr);
        }
        else if (Number.class.isAssignableFrom(javaMapping.getJavaType()))
        {
            return new CaseNumericExpression(new SQLExpression[] {isNotNullExpr}, new SQLExpression[] {getExpr}, elseExpr);
        }
        else if (javaMapping instanceof BooleanMapping)
        {
            return new CaseBooleanExpression(new SQLExpression[] {isNotNullExpr}, new SQLExpression[] {getExpr}, elseExpr);
        }
        return new CaseExpression(new SQLExpression[] {isNotNullExpr}, new SQLExpression[] {getExpr}, elseExpr);
    }
}