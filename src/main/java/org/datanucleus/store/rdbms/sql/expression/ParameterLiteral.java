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
package org.datanucleus.store.rdbms.sql.expression;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * Representation of a literal representing a parameter where we don't know the type of the parameter yet.
 * This literal doesn't have a long lifetime, being replaced during the process of conversion to SQL
 * using the compared expression to define the mapping (and hence which literal type) to use.
 */
public class ParameterLiteral extends SQLExpression implements SQLLiteral
{
    /** Parameter name. */
    protected String name;

    protected Object value;

    /**
     * Constructor for an integer literal with a value.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Name of the parameter that this represents if any (as JDBC "?")
     */
    public ParameterLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;
        this.value = value;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public Object getValue()
    {
        return value;
    }

    @Override
    public SQLExpression add(SQLExpression expr)
    {
        if (expr instanceof ParameterLiteral)
        {
            return super.add(expr);
        }

        // Swap it around since we don't know the type of this
        return expr.add(this);
    }

    @Override
    public BooleanExpression eq(SQLExpression expr)
    {
        if (expr instanceof ParameterLiteral)
        {
            return super.eq(expr);
        }

        // Swap it around since we don't know the type of this
        return expr.eq(this);
    }

    @Override
    public BooleanExpression ge(SQLExpression expr)
    {
        if (expr instanceof ParameterLiteral)
        {
            return super.ge(expr);
        }

        // Swap it around since we don't know the type of this
        return expr.lt(this);
    }

    @Override
    public BooleanExpression gt(SQLExpression expr)
    {
        if (expr instanceof ParameterLiteral)
        {
            return super.gt(expr);
        }

        // Swap it around since we don't know the type of this
        return expr.le(this);
    }

    @Override
    public BooleanExpression le(SQLExpression expr)
    {
        if (expr instanceof ParameterLiteral)
        {
            return super.le(expr);
        }

        // Swap it around since we don't know the type of this
        return expr.gt(this);
    }

    @Override
    public BooleanExpression lt(SQLExpression expr)
    {
        if (expr instanceof ParameterLiteral)
        {
            return super.lt(expr);
        }

        // Swap it around since we don't know the type of this
        return expr.ge(this);
    }

    @Override
    public BooleanExpression ne(SQLExpression expr)
    {
        if (expr instanceof ParameterLiteral)
        {
            return super.ne(expr);
        }

        // Swap it around since we don't know the type of this
        return expr.ne(this);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#setNotParameter()
     */
    public void setNotParameter()
    {
        // Do nothing here
        return;
    }
}