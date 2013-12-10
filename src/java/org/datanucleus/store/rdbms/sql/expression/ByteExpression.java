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

import java.util.List;

import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * Representation of a Byte expression in a Query.
 */
public class ByteExpression extends NumericExpression
{
    /**
     * Constructor for an SQL expression for a (field) mapping in a specified table.
     * @param stmt The statement
     * @param table The table in the statement
     * @param mapping The mapping for the field
     */
    public ByteExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
    }

    /**
     * Perform an operation <pre>op</pre> on expression <pre>expr1</pre>.
     * @param op operator
     * @param expr1 operand
     */
    public ByteExpression(Expression.MonadicOperator op, SQLExpression expr1)
    {
        super(op, expr1);
    }

    /**
     * Perform an operation <pre>op</pre> between <pre>expr1</pre> and <pre>expr2</pre>.
     * @param expr1 the first expression
     * @param op the operator between operands
     * @param expr2 the second expression
     */
    public ByteExpression(SQLExpression expr1, Expression.DyadicOperator op, SQLExpression expr2)
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
        else
        {
            return super.ne(expr);
        }
    }

    public BooleanExpression lt(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            return expr.lt(this);
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
        if (expr instanceof NullLiteral)
        {
            return expr.le(this);
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
        if (expr instanceof NullLiteral)
        {
            return expr.gt(this);
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
        if (expr instanceof NullLiteral)
        {
            return expr.ge(this);
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

    public SQLExpression invoke(String methodName, List args)
    {
        return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, Byte.class.getName(), 
            methodName, this, args);
    }
}