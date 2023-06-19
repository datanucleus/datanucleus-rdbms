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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.datanucleus.store.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * Representation of an expression with a string.
 */
public class StringExpression extends SQLExpression
{
    /**
     * Constructor for an SQL expression for a (field) mapping in a specified table.
     * @param stmt The statement
     * @param table The table in the statement
     * @param mapping The mapping for the field
     */
    public StringExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
    }

    /**
     * Perform an operation <pre>op</pre> on expression <pre>expr1</pre>.
     * @param op operator
     * @param expr1 operand
     */
    public StringExpression(Expression.MonadicOperator op, SQLExpression expr1)
    {
        super(op, expr1);
    }

    /**
     * Perform an operation <pre>op</pre> between <pre>expr1</pre> and <pre>expr2</pre>.
     * @param expr1 the first expression
     * @param op the operator between operands
     * @param expr2 the second expression
     */
    public StringExpression(SQLExpression expr1, Expression.DyadicOperator op, SQLExpression expr2)
    {
        super(expr1, op, expr2);
    }

    /**
     * Generates statement as e.g. FUNCTION_NAME(arg[,argN]).
     * @param stmt SQL Statement
     * @param mapping Mapping to use
     * @param functionName Name of the function
     * @param args ScalarExpression list
     */
    public StringExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List<SQLExpression> args)
    {
        super(stmt, mapping, functionName, args, null);
    }

    /**
     * Generates statement as e.g. FUNCTION_NAME(arg AS type[,argN as typeN]).
     * @param stmt SQL Statement
     * @param mapping Mapping to use
     * @param functionName Name of function
     * @param args ScalarExpression list
     * @param types String or ScalarExpression list
     */
    public StringExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List<SQLExpression> args, List types)
    {
        super(stmt, mapping, functionName, args, types);
    }    

    public BooleanExpression eq(SQLExpression expr)
    {
        if (expr instanceof NullLiteral)
        {
            return expr.eq(this);
        }
        else if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof EnumLiteral)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof EnumExpression)
        {
            // Swap and handle in EnumExpression
            return expr.eq(this);
        }
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof ByteLiteral)
        {
            int value = ((BigInteger)((ByteLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);    
            return new BooleanExpression(this, Expression.OP_EQ, literal);
        }                   
        else if (expr instanceof IntegerLiteral)
        {
            int value = ((Number)((IntegerLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);                
            return new BooleanExpression(this, Expression.OP_EQ, literal);
        }
        else if (expr instanceof FloatingPointLiteral)
        {
            int value = ((BigDecimal)((FloatingPointLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_EQ, literal);
        }
        else if (expr instanceof NumericExpression)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof TemporalExpression)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof CaseExpression)
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
        else if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof EnumLiteral)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);   
        }
        else if (expr instanceof EnumExpression)
        {
            // Swap and handle in EnumExpression
            return expr.ne(this);
        }
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);            
        }
        else if (expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);   
        }
        else if (expr instanceof ByteLiteral)
        {
            int value = ((BigInteger)((ByteLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_NOTEQ, literal);
        }                   
        else if (expr instanceof IntegerLiteral)
        {
            int value = ((Number)((IntegerLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_NOTEQ, literal);
        }
        else if (expr instanceof FloatingPointLiteral)
        {
            int value = ((BigDecimal)((FloatingPointLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_NOTEQ, literal);
        }
        else if (expr instanceof NumericExpression)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof TemporalExpression)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof CaseExpression)
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
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_LT, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_LT, expr);
        }
        else if (expr instanceof NullLiteral)
        {
            return expr.lt(this);
        }
        else if (expr instanceof EnumExpression)
        {
            // Swap and handle in EnumExpression
            return expr.ge(this);
        }
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanExpression(this, Expression.OP_LT, expr);            
        }
        else if (expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_LT, expr);   
        }
        else if (expr instanceof ByteLiteral)
        {
            int value = ((BigInteger)((ByteLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_LT, literal);
        }                   
        else if (expr instanceof IntegerLiteral)
        {
            int value = ((Number)((IntegerLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_LT, literal);
        }
        else if (expr instanceof FloatingPointLiteral)
        {
            int value = ((BigDecimal)((FloatingPointLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_LT, literal);
        }
        else if (expr instanceof NumericExpression)
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
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);
        }
        else if (expr instanceof NullLiteral)
        {
            return expr.le(this);
        }
        else if (expr instanceof EnumExpression)
        {
            // Swap and handle in EnumExpression
            return expr.gt(this);
        }
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);            
        }
        else if (expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);   
        }
        else if (expr instanceof ByteLiteral)
        {
            int value = ((BigInteger)((ByteLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_LTEQ, literal);
        }                   
        else if (expr instanceof IntegerLiteral)
        {
            int value = ((Number)((IntegerLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_LTEQ, literal);
        }
        else if (expr instanceof FloatingPointLiteral)
        {
            int value = ((BigDecimal)((FloatingPointLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_LTEQ, literal);
        }
        else if (expr instanceof NumericExpression)
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
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_GT, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_GT, expr);
        }
        else if (expr instanceof NullLiteral)
        {
            return expr.gt(this);
        }
        else if (expr instanceof EnumExpression)
        {
            // Swap and handle in EnumExpression
            return expr.le(this);
        }
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanExpression(this, Expression.OP_GT, expr);            
        }
        else if (expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_GT, expr);   
        }
        else if (expr instanceof ByteLiteral)
        {
            int value = ((BigInteger)((ByteLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_GT, literal);
        }                   
        else if (expr instanceof IntegerLiteral)
        {
            int value = ((Number)((IntegerLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_GT, literal);
        }
        else if (expr instanceof FloatingPointLiteral)
        {
            int value = ((BigDecimal)((FloatingPointLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_GT, literal);
        }
        else if (expr instanceof NumericExpression)
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
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);
        }
        else if (expr instanceof ColumnExpression)
        {
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);
        }
        else if (expr instanceof NullLiteral)
        {
            return expr.ge(this);
        }
        else if (expr instanceof EnumExpression)
        {
            // Swap and handle in EnumExpression
            return expr.lt(this);
        }
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);            
        }
        else if (expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);   
        }
        else if (expr instanceof ByteLiteral)
        {
            int value = ((BigInteger)((ByteLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_GTEQ, literal);
        }                   
        else if (expr instanceof IntegerLiteral)
        {
            int value = ((Number)((IntegerLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_GTEQ, literal);
        }
        else if (expr instanceof FloatingPointLiteral)
        {
            int value = ((BigDecimal)((FloatingPointLiteral)expr).getValue()).intValue(); 
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping, String.valueOf((char)value), null);
            return new BooleanExpression(this, Expression.OP_GTEQ, literal);
        }
        else if (expr instanceof NumericExpression)
        {
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);
        }
        else
        {
            return super.ge(expr);
        }
    }

    public SQLExpression add(SQLExpression expr)
    {
        if (this instanceof SQLLiteral && isParameter() && expr instanceof SQLLiteral && expr.isParameter())
        {
            // Special case : "param + param", so convert both to literals
            stmt.getQueryGenerator().useParameterExpressionAsLiteral((SQLLiteral)this);
            stmt.getQueryGenerator().useParameterExpressionAsLiteral((SQLLiteral)expr);
            return new StringExpression(this, Expression.OP_CONCAT, expr).encloseInParentheses();
        }

        // String.add mapped to concatenation of strings
        if (expr.isParameter())
        {
            return new StringExpression(this, Expression.OP_CONCAT, expr).encloseInParentheses();
        }
        else if (expr instanceof StringLiteral)
        {
            return new StringExpression(this, Expression.OP_CONCAT, 
                new StringLiteral(stmt, expr.mapping, ((StringLiteral)expr).getValue(), null)).encloseInParentheses();
        }
        else if (expr instanceof StringExpression)
        {
            return new StringExpression(this, Expression.OP_CONCAT, expr).encloseInParentheses();
        }
        else if (expr instanceof CharacterExpression)
        {
            return new StringExpression(this, Expression.OP_CONCAT, expr).encloseInParentheses();
        }
        else if (expr instanceof NumericExpression)
        {
            StringExpression strExpr = (StringExpression)stmt.getSQLExpressionFactory().invokeOperation(
                "numericToString", expr, null).encloseInParentheses();
            return new StringExpression(this, Expression.OP_CONCAT, strExpr).encloseInParentheses();
        }
        else if (expr instanceof NullLiteral)
        {
            return expr;
        }
        else
        {
            return new StringExpression(this, Expression.OP_CONCAT, expr).encloseInParentheses();
        }
    }

    public BooleanExpression in(SQLExpression expr, boolean not)
    {
        return new BooleanExpression(this, not ? Expression.OP_NOTIN : Expression.OP_IN, expr);
    }

    @Override
    public SQLExpression invoke(String methodName, List<SQLExpression> args)
    {
        return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, String.class.getName(), methodName, this, args);
    }
}