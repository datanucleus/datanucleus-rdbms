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
package org.datanucleus.store.rdbms.sql.expression;

import java.util.List;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLText;

/**
 * Expression for a field/property that can delegate to an internal expression(s).
 */
public abstract class DelegatedExpression extends SQLExpression
{
    /** The delegate expression that we use. MUST BE SET ON CONSTRUCTION. */
    protected SQLExpression delegate;

    public DelegatedExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#eq(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression eq(SQLExpression expr)
    {
        return delegate.eq(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#ne(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression ne(SQLExpression expr)
    {
        return delegate.ne(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#add(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public SQLExpression add(SQLExpression expr)
    {
        return delegate.add(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#div(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public SQLExpression div(SQLExpression expr)
    {
        return delegate.div(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#ge(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression ge(SQLExpression expr)
    {
        return delegate.ge(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#gt(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression gt(SQLExpression expr)
    {
        return delegate.gt(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#le(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression le(SQLExpression expr)
    {
        return delegate.le(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#lt(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression lt(SQLExpression expr)
    {
        return delegate.lt(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#mod(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public SQLExpression mod(SQLExpression expr)
    {
        return delegate.mod(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#mul(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public SQLExpression mul(SQLExpression expr)
    {
        return delegate.mul(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#sub(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public SQLExpression sub(SQLExpression expr)
    {
        return delegate.sub(expr);
    }

    public SQLExpression invoke(String methodName, List args)
    {
        return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, 
            mapping.getJavaType().getName(), methodName, this, args);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#toSQLText(int)
     */
    @Override
    public SQLText toSQLText()
    {
        return delegate.toSQLText();
    }

    /**
     * Accessor for the delegate that represents this enum.
     * Will be either a StringExpression or a NumericExpression depending whether the enum is
     * stored as a string or as a numeric.
     * @return The delegate
     */
    public SQLExpression getDelegate()
    {
        return delegate;
    }

    public boolean isParameter()
    {
        return delegate.isParameter();
    }
}