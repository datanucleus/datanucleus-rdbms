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

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * Representation of a Character expression in a Query
 */
public class CharacterExpression extends SQLExpression
{
    /**
     * Constructor for an SQL expression for a (field) mapping in a specified table.
     * @param stmt The statement
     * @param table The table in the statement
     * @param mapping The mapping for the field
     */
    public CharacterExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
    }

    /**
     * Generates statement as e.g. FUNCTION_NAME(arg[,argN]). 
     * The function returns a character value. This is used where we are invoking some SQL function
     * and it returns a character.
     * @param stmt SQL Statement
     * @param mapping Mapping to use
     * @param functionName Name of the function
     * @param args SQLExpression list
     */
    public CharacterExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List args)
    {
        super(stmt, mapping, functionName, args, null);
    }

    /**
     * Generates statement as e.g. FUNCTION_NAME(arg [AS type] [,argN [AS type]]). 
     * The function returns a character value. This is used where we are invoking some SQL function
     * and it returns a character.
     * @param stmt SQL Statement
     * @param mapping Mapping to use
     * @param functionName Name of the function
     * @param args SQLExpression list
     * @param types Optional types list for the args
     */
    public CharacterExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List args, List types)
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
        else if (expr instanceof NullLiteral)
        {
            return expr.eq(this);
        }
        else if (expr instanceof ColumnExpression || expr instanceof CharacterExpression)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof StringLiteral)
        {
            Object value = ((StringLiteral)expr).getValue();
            if (value instanceof String && ((String)value).length() > 1)
            {
                // Can't compare a character with a String of more than 1 character
                throw new NucleusUserException("Can't perform equality comparison between a character and a String of more than 1 character (" + value + ") !");
            }
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_EQ, expr); 
        }
        else if (expr instanceof NumericExpression)
        {
            return ExpressionUtils.getNumericExpression(this).eq(expr);
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
        else if (expr instanceof NullLiteral)
        {
            return expr.ne(this);
        }
        else if (expr instanceof ColumnExpression || expr instanceof CharacterExpression || expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof NumericExpression)
        {
            return ExpressionUtils.getNumericExpression(this).ne(expr);
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
        else if (expr instanceof NullLiteral)
        {
            return expr.lt(this);
        }
        else if (expr instanceof ColumnExpression || expr instanceof CharacterExpression || expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_LT, expr);
        }
        else if (expr instanceof NumericExpression)
        {
            return ExpressionUtils.getNumericExpression(this).lt(expr);
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
        else if (expr instanceof NullLiteral)
        {
            return expr.le(this);
        }
        else if (expr instanceof ColumnExpression || expr instanceof CharacterExpression || expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);
        }
        else if (expr instanceof NumericExpression)
        {
            return ExpressionUtils.getNumericExpression(this).le(expr);
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
        else if (expr instanceof NullLiteral)
        {
            return expr.gt(this);
        }
        else if (expr instanceof ColumnExpression || expr instanceof CharacterExpression || expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_GT, expr);
        }
        else if (expr instanceof NumericExpression)
        {
            return ExpressionUtils.getNumericExpression(this).gt(expr);
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
        else if (expr instanceof NullLiteral)
        {
            return expr.ge(this);
        }
        else if (expr instanceof ColumnExpression || expr instanceof CharacterExpression || expr instanceof StringExpression)
        {
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);
        }
        else if (expr instanceof NumericExpression)
        {
            return ExpressionUtils.getNumericExpression(this).ge(expr);
        }               
        else
        {
            return super.ge(expr);
        }
    }

    public SQLExpression add(SQLExpression expr)
    {
        if (expr instanceof CharacterExpression)
        {
            return new NumericExpression(ExpressionUtils.getNumericExpression(this), 
                Expression.OP_ADD, ExpressionUtils.getNumericExpression(expr));
        }
        else if (expr instanceof NumericExpression)
        {
            return new NumericExpression(ExpressionUtils.getNumericExpression(this), 
                Expression.OP_ADD, expr);
        }         
        else
        {
            return super.add(expr);
        }
    }

    public SQLExpression sub(SQLExpression expr)
    {
        if (expr instanceof CharacterExpression)
        {
            return new NumericExpression(ExpressionUtils.getNumericExpression(this), 
                Expression.OP_SUB, ExpressionUtils.getNumericExpression(expr));
        }
        else if (expr instanceof NumericExpression)
        {
            return new NumericExpression(ExpressionUtils.getNumericExpression(this), 
                Expression.OP_SUB, expr);
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
            return new NumericExpression(ExpressionUtils.getNumericExpression(this), Expression.OP_MUL, expr);
            //return ExpressionUtils.getNumericExpression(this).mul(expr);
        }
        else if (expr instanceof CharacterExpression)
        {
            return new NumericExpression(ExpressionUtils.getNumericExpression(this), Expression.OP_MUL, ExpressionUtils.getNumericExpression(expr));
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
            return new NumericExpression(ExpressionUtils.getNumericExpression(this), Expression.OP_DIV, expr);
            //return ExpressionUtils.getNumericExpression(this).div(expr);
        }
        else if (expr instanceof CharacterExpression)
        {
            return new NumericExpression(ExpressionUtils.getNumericExpression(this), Expression.OP_DIV, ExpressionUtils.getNumericExpression(expr));
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
            if (expr instanceof CharacterExpression)
            {
                return stmt.getSQLExpressionFactory().invokeOperation("mod", 
                    ExpressionUtils.getNumericExpression(this), ExpressionUtils.getNumericExpression(expr)).encloseInParentheses();
            }        
            else if (expr instanceof NumericExpression)
            {
                return stmt.getSQLExpressionFactory().invokeOperation("mod", ExpressionUtils.getNumericExpression(this), expr);
            }
        }
        catch (UnsupportedOperationException uoe)
        {
            // No defined operation handler, so revert to base level (%)
        }

        return new NumericExpression(this, Expression.OP_MOD, expr);
    }    

    public SQLExpression neg()
    {
        return new NumericExpression(Expression.OP_NEG, ExpressionUtils.getNumericExpression(this));
    }

    public SQLExpression com()
    {
        return ExpressionUtils.getNumericExpression(this).neg().sub(new IntegerLiteral(stmt, mapping, BigInteger.ONE, null));
    }

    public BooleanExpression in(SQLExpression expr, boolean not)
    {
        return new BooleanExpression(this, not ? Expression.OP_NOTIN : Expression.OP_IN, expr);
    }

    public SQLExpression invoke(String methodName, List args)
    {
        if (methodName.equals("toUpperCase") || methodName.equals("toLowerCase"))
        {
            // Use equivalent String method
            return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, String.class.getName(), methodName, this, args);
        }
        return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, Character.class.getName(), methodName, this, args);
    }
}