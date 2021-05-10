/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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

import org.datanucleus.store.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * Representation of a Binary expression in a Query.
 */
public class BinaryExpression extends SQLExpression
{
    /**
     * Constructor for an SQL expression for a (field) mapping in a specified table.
     * @param stmt The statement
     * @param table The table in the statement
     * @param mapping The mapping for the field
     */
    public BinaryExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
    }

    /**
     * @param stmt SQL statement
     * @param mapping The mapping
     * @param functionName Function to invoke
     * @param args Function args
     * @param types Function arg types
     */
    public BinaryExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List<SQLExpression> args, List types)
    {
        super(stmt, mapping, functionName, args, types);
    }

    public BooleanExpression eq(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            return expr.eq(this);
        }
        else if (expr instanceof BinaryExpression)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else
        {
            return super.eq(expr);
        }
    }

    public BooleanExpression noteq(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            return expr.ne(this);
        }
        else if (expr instanceof BinaryExpression)
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
        if (expr instanceof BinaryExpression)
        {
            return new BooleanExpression(this, Expression.OP_LT, expr);
        }

        return super.lt(expr);
    }

    public BooleanExpression lteq(SQLExpression expr)
    {
        if (expr instanceof BinaryExpression)
        {
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);
        }

        return super.le(expr);
    }

    public BooleanExpression gt(SQLExpression expr)
    {
        if (expr instanceof BinaryExpression)
        {
            return new BooleanExpression(this, Expression.OP_GT, expr);
        }

        return super.gt(expr);
    }

    public BooleanExpression gteq(SQLExpression expr)
    {
        if (expr instanceof BinaryExpression)
        {
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);
        }

        return super.ge(expr);
    }

    public BooleanExpression in(SQLExpression expr, boolean not)
    {
        return new BooleanExpression(this, not ? Expression.OP_NOTIN : Expression.OP_IN, expr);
    }
}