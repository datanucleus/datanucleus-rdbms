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

import java.util.Date;
import java.util.List;

import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * Representation of temporal objects in java query languages.
 * Can be used for anything based on java.util.Date (this includes java.time.* since they convert to java.util.Date types at the datastore).
 */
public class TemporalExpression extends SQLExpression
{
    /**
     * Constructor for an SQL expression for a (field) mapping in a specified table.
     * @param stmt The statement
     * @param table The table in the statement
     * @param mapping The mapping for the field
     */
    public TemporalExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
    }

    /**
     * Generates statement as "FUNCTION_NAME(arg [,argN])".
     * @param stmt SQL Statement
     * @param mapping Mapping to use
     * @param functionName Name of function
     * @param args SQLExpression list
     */
    public TemporalExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List args)
    {
        super(stmt, mapping, functionName, args, null);
    }

    /**
     * Generates statement as "FUNCTION_NAME(arg [AS type] [,argN [AS typeN]])".
     * @param stmt SQL Statement
     * @param mapping Mapping to use
     * @param functionName Name of function
     * @param args SQLExpression list
     * @param types Optional String/SQLExpression list of types for the args
     */
    public TemporalExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List args, List types)
    {
        super(stmt, mapping, functionName, args, types);
    }

    protected TemporalExpression(SQLExpression expr1, Expression.DyadicOperator op, SQLExpression expr2)
    {
        super(expr1, op, expr2);
    }

    public BooleanExpression eq(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            return expr.eq(this);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof TemporalExpression)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else
        {
            return super.eq(expr);
        }
    }

    public BooleanExpression ne(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            return expr.ne(this);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof TemporalExpression)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else
        {
            return super.ne(expr);
        }
    }

    public BooleanExpression lt(SQLExpression expr)
    {
        if (expr instanceof TemporalExpression)
        {
            return new BooleanExpression(this, Expression.OP_LT, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_LT, expr);
        }
        else
        {
            return super.lt(expr);
        }
    }

    public BooleanExpression le(SQLExpression expr)
    {
        if (expr instanceof TemporalExpression)
        {
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);
        }
        else
        {
            return super.le(expr);
        }
    }

    public BooleanExpression gt(SQLExpression expr)
    {
        if (expr instanceof TemporalExpression)
        {
            return new BooleanExpression(this, Expression.OP_GT, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_GT, expr);
        }
        else
        {
            return super.gt(expr);
        }
    }

    public BooleanExpression ge(SQLExpression expr)
    {
        if (expr instanceof TemporalExpression)
        {
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);
        }
        else
        {
            return super.ge(expr);
        }
    }

    public BooleanExpression in(SQLExpression expr, boolean not)
    {
        return new BooleanExpression(this, not ? Expression.OP_NOTIN : Expression.OP_IN, expr);
    }

    public SQLExpression add(SQLExpression expr)
    {
        if (expr instanceof TemporalExpression)
        {
            return new TemporalExpression(this, Expression.OP_ADD, expr).encloseInParentheses();
        }
        else if (expr instanceof DelegatedExpression)
        {
            SQLExpression delegate = ((DelegatedExpression)expr).getDelegate();
            if (delegate instanceof TemporalExpression)
            {
                return new TemporalExpression(this, Expression.OP_ADD, delegate).encloseInParentheses();
            }
        }

        return super.add(expr);
    }

    public SQLExpression sub(SQLExpression expr)
    {
        if (expr instanceof TemporalExpression)
        {
            return new TemporalExpression(this, Expression.OP_SUB, expr).encloseInParentheses();
        }
        else if (expr instanceof DelegatedExpression)
        {
            SQLExpression delegate = ((DelegatedExpression)expr).getDelegate();
            if (delegate instanceof TemporalExpression)
            {
                return new TemporalExpression(this, Expression.OP_SUB, delegate).encloseInParentheses();
            }
        }

        return super.sub(expr);
    }

    public SQLExpression invoke(String methodName, List args)
    {
        return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, Date.class.getName(), methodName, this, args);
    }
}