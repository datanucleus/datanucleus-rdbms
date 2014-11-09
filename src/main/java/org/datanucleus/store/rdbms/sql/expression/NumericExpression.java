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

import java.math.BigInteger;
import java.util.List;

import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * Representation of a Numeric expression in an SQL statement.
 */
public class NumericExpression extends SQLExpression
{
    /**
     * Constructor for a numeric expression for the specified mapping using the specified SQL text.
     * @param stmt The statement
     * @param mapping the mapping associated to this expression
     * @param sql The SQL text that will return a numeric
     */
    public NumericExpression(SQLStatement stmt, JavaTypeMapping mapping, String sql)
    {
        super(stmt, null, mapping);

        st.clearStatement();
        st.append(sql);
    }

    /**
     * Constructor for a numeric expression for the mapping in the specified table.
     * @param stmt the SQLStatement
     * @param table the table where this expression refers to
     * @param mapping the mapping associated to this expression
     */
    public NumericExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
    }

    /**
     * Perform an operation <pre>op</pre> on expression <pre>expr1</pre>.
     * @param op operator
     * @param expr1 operand
     */
    public NumericExpression(Expression.MonadicOperator op, SQLExpression expr1)
    {
        super(op, expr1);
    }

    /**
     * Perform an operation <pre>op</pre> between <pre>expr1</pre> and <pre>expr2</pre>.
     * @param expr1 the first expression
     * @param op the operator between operands
     * @param expr2 the second expression
     */
    public NumericExpression(SQLExpression expr1, Expression.DyadicOperator op, SQLExpression expr2)
    {
        super(expr1, op, expr2);
    }

    /**
     * Generates statement as "FUNCTION_NAME(arg [,argN])".
     * @param stmt The statement
     * @param mapping Mapping to use
     * @param functionName Name of function
     * @param args SQLExpression list
     */
    public NumericExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List args)
    {
        super(stmt, mapping, functionName, args, null);
    }

    /**
     * Generates statement as "FUNCTION_NAME(arg [AS type] [,argN [AS typeN]])".
     * @param stmt The statement
     * @param mapping Mapping to use
     * @param functionName Name of function
     * @param args SQLExpression list
     * @param types Optional String/SQLExpression list of types for the args
     */
    public NumericExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List args, List types)
    {
        super(stmt, mapping, functionName, args, types);
    }

    public BooleanExpression eq(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof NullLiteral)
        {
            return expr.eq(this);
        }
        else if (expr instanceof NumericExpression/* || expr instanceof SubqueryExpression*/)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof CharacterExpression)
        {
            return new BooleanExpression(this, Expression.OP_EQ, ExpressionUtils.getNumericExpression(expr));
        }
        else
        {
            return super.eq(expr);
        }
    }

    public BooleanExpression ne(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof NullLiteral)
        {
            return expr.ne(this);
        }
        else if (expr instanceof NumericExpression/* || expr instanceof SubqueryExpression*/)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof CharacterExpression)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, ExpressionUtils.getNumericExpression(expr));
        }
        else
        {
            return super.ne(expr);
        }
    }

    public BooleanExpression lt(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_LT, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_LT, expr);
        }
        else if (expr instanceof NumericExpression/* || expr instanceof SubqueryExpression*/)
        {
            return new BooleanExpression(this, Expression.OP_LT, expr);
        }
        else if (expr instanceof CharacterExpression)
        {
            return new BooleanExpression(this, Expression.OP_LT, ExpressionUtils.getNumericExpression(expr));
        }            
        else
        {
            return super.lt(expr);
        }
    }

    public BooleanExpression le(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);
        }
        else if (expr instanceof NumericExpression/* || expr instanceof SubqueryExpression*/)
        {
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);
        }
        else if (expr instanceof CharacterExpression)
        {
            return new BooleanExpression(this, Expression.OP_LTEQ, ExpressionUtils.getNumericExpression(expr));
        }              
        else
        {
            return super.le(expr);
        }
    }

    public BooleanExpression gt(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_GT, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_GT, expr);
        }
        else if (expr instanceof NumericExpression/* || expr instanceof SubqueryExpression*/)
        {
            return new BooleanExpression(this, Expression.OP_GT, expr);
        }
        else if (expr instanceof CharacterExpression)
        {
            return new BooleanExpression(this, Expression.OP_GT, ExpressionUtils.getNumericExpression(expr));
        }
        else
        {
            return super.gt(expr);
        }
    }

    public BooleanExpression ge(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);
        }
        else if (expr instanceof NumericExpression/* || expr instanceof SubqueryExpression*/)
        {
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);
        }
        else if (expr instanceof CharacterExpression)
        {
            return new BooleanExpression(this, Expression.OP_GTEQ, ExpressionUtils.getNumericExpression(expr));
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
        if (expr instanceof NumericExpression)
        {
            return new NumericExpression(this, Expression.OP_ADD, expr).encloseInParentheses();
        }
        else if (expr instanceof StringExpression)
        {
            StringExpression strExpr = (StringExpression)stmt.getSQLExpressionFactory().invokeOperation("numericToString", this, null);
            return new StringExpression(strExpr, Expression.OP_CONCAT, expr);
        }
        else if (expr instanceof CharacterExpression)
        {
            return new NumericExpression(this, Expression.OP_ADD, ExpressionUtils.getNumericExpression(expr)).encloseInParentheses();
        }
        else if (expr instanceof NullLiteral)
        {
            return expr;
        }
        else
        {
            return super.add(expr);
        }
    }

    public SQLExpression sub(SQLExpression expr)
    {
        if (expr instanceof NumericExpression)
        {
            return new NumericExpression(this, Expression.OP_SUB, expr).encloseInParentheses();
        }
        else if (expr instanceof CharacterExpression)
        {
            return new NumericExpression(this, Expression.OP_SUB, ExpressionUtils.getNumericExpression(expr)).encloseInParentheses();
        }
        else
        {
            return super.sub(expr);
        }
    }

    public SQLExpression mul(SQLExpression expr)
    {
        if (expr instanceof NumericExpression)
        {
            return new NumericExpression(this, Expression.OP_MUL, expr).encloseInParentheses();
        }
        else if (expr instanceof CharacterExpression)
        {
            return new NumericExpression(this, Expression.OP_MUL, ExpressionUtils.getNumericExpression(expr)).encloseInParentheses();
        }
        else
        {
            return super.mul(expr);
        }
    }

    public SQLExpression div(SQLExpression expr)
    {
        if (expr instanceof NumericExpression)
        {
            return new NumericExpression(this, Expression.OP_DIV, expr).encloseInParentheses();
        }
        else if (expr instanceof CharacterExpression)
        {
            return new NumericExpression(this, Expression.OP_DIV, ExpressionUtils.getNumericExpression(expr)).encloseInParentheses();
        }
        else
        {
            return super.div(expr);
        }
    }

    /**
     * Method to return a modulus expression.
     * @param expr The expression to modulus against
     * @return The modulus expression
     */
    public SQLExpression mod(SQLExpression expr)
    {
        try
        {
            if (expr instanceof NumericExpression)
            {
                return stmt.getSQLExpressionFactory().invokeOperation("mod", this, expr).encloseInParentheses();
            }
            else if (expr instanceof CharacterExpression)
            {
                return stmt.getSQLExpressionFactory().invokeOperation("mod", this, ExpressionUtils.getNumericExpression(expr)).encloseInParentheses();
            }
        }
        catch (UnsupportedOperationException uoe)
        {
            // No defined handler for MOD operator so revert to % operation
        }

        return new NumericExpression(this, Expression.OP_MOD, expr);
    }

    public SQLExpression neg()
    {
        return new NumericExpression(Expression.OP_NEG, this);
    }

    public SQLExpression com()
    {
        return this.neg().sub(new IntegerLiteral(stmt, mapping,BigInteger.ONE, parameterName));
    }
}