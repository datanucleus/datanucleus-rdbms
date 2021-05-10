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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * Representation of an Integer literal.
 */
public class IntegerLiteral extends NumericExpression implements SQLLiteral
{
    private final Number value;

    /**
     * Constructor for an integer literal with a value.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Name of the parameter that this represents if any (as JDBC "?")
     */
    public IntegerLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
        }
        else if (value instanceof Number)
        {
            this.value = (Number)value;
        }
        else
        {
            throw new NucleusException("Cannot create " + this.getClass().getName() + 
                " for value of type " + value.getClass().getName());
        }

        if (parameterName != null)
        {
            st.appendParameter(parameterName, mapping, this.value);
        }
        else
        {
            setStatement();
        }
    }

    public BooleanExpression eq(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof IntegerLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                new BigInteger(value.toString()).compareTo(new BigInteger(((IntegerLiteral)expr).value.toString())) == 0);
        }
        else if (expr instanceof CharacterExpression)
        {
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping,
                String.valueOf((char)value.intValue()), null);
            return new BooleanExpression(expr, Expression.OP_EQ, literal);
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
        else if (expr instanceof IntegerLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                new BigInteger(value.toString()).compareTo(
                    new BigInteger(((IntegerLiteral)expr).value.toString())) != 0);
        }
        else if (expr instanceof CharacterExpression)
        {
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping,
                String.valueOf((char)value.intValue()), null);
            return new BooleanExpression(expr, Expression.OP_NOTEQ, literal);
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
        else if (expr instanceof IntegerLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                new BigInteger(value.toString()).compareTo(
                    new BigInteger(((IntegerLiteral)expr).value.toString())) < 0);
        }
        else if (expr instanceof CharacterExpression)
        {
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping,
                String.valueOf((char)value.intValue()), null);
            return new BooleanExpression(literal, Expression.OP_LT, expr);
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
        else if (expr instanceof IntegerLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                new BigInteger(value.toString()).compareTo(
                    new BigInteger(((IntegerLiteral)expr).value.toString())) <= 0);
        }
        else if (expr instanceof CharacterExpression)
        {
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping,
                String.valueOf((char)value.intValue()), null);
            return new BooleanExpression(literal, Expression.OP_LTEQ, expr);
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
        else if (expr instanceof IntegerLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                new BigInteger(value.toString()).compareTo(
                    new BigInteger(((IntegerLiteral)expr).value.toString())) > 0);
        }
        else if (expr instanceof CharacterExpression)
        {
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping,
                String.valueOf((char)value.intValue()), null);
            return new BooleanExpression(literal, Expression.OP_GT, expr);
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
        else if (expr instanceof IntegerLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                new BigInteger(value.toString()).compareTo(
                    new BigInteger(((IntegerLiteral)expr).value.toString())) >= 0);
        }
        else if (expr instanceof CharacterExpression)
        {
            CharacterLiteral literal = new CharacterLiteral(stmt, mapping,
                String.valueOf((char)value.intValue()), null);
            return new BooleanExpression(literal, Expression.OP_GTEQ, expr);
        }            
        else
        {
            return super.ge(expr);
        }
    }

    /**
     * If both operands are instances of IntegerLiteral, the operation results in BigInteger type.
     */
    public SQLExpression add(SQLExpression expr)
    {
        if (expr instanceof StringExpression)
        {
            // Special case where we have "Integer + String" so need to return "String" via concat
            if (isParameter())
            {
                stmt.getQueryGenerator().useParameterExpressionAsLiteral(this);
            }
            StringExpression strExpr = (StringExpression)stmt.getSQLExpressionFactory().invokeOperation(
                "numericToString", this, null);
            return new StringExpression(strExpr, Expression.OP_CONCAT, expr);
        }
        else if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give basic add
            return new NumericExpression(this, Expression.OP_ADD, expr);
        }
        else if (expr instanceof IntegerLiteral)
        {
            return new IntegerLiteral(stmt, mapping, new BigInteger(value.toString()).add(
                new BigInteger(((IntegerLiteral)expr).value.toString())), null);
        }
        else if (expr instanceof CharacterLiteral)
        {
            int v = ((CharacterLiteral)expr).getValue().toString().charAt(0);
            return new IntegerLiteral(stmt, mapping, new BigInteger(value.toString()).add(
                new BigInteger(""+v)), null);
        }        
        else
        {
            return super.add(expr);
        }
    }

    /**
     * If both operands are instances of IntegerLiteral, the operation results in BigInteger type.
     */
    public SQLExpression sub(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give basic subtract
            return new NumericExpression(this, Expression.OP_SUB, expr);
        }
        else if (expr instanceof IntegerLiteral)
        {
            return new IntegerLiteral(stmt, mapping, new BigInteger(value.toString()).subtract(
                new BigInteger(((IntegerLiteral)expr).value.toString())), null);
        }
        else if (expr instanceof CharacterLiteral)
        {
            int v = ((CharacterLiteral)expr).getValue().toString().charAt(0);
            return new IntegerLiteral(stmt, mapping, new BigInteger(value.toString()).subtract(
                new BigInteger(""+v)), null);
        }         
        else
        {
            return super.sub(expr);
        }
    }

    /**
     * If both operands are instances of IntegerLiteral, the operation results in BigInteger type. 
     */
    public SQLExpression mul(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give basic multiply
            return new NumericExpression(this, Expression.OP_MUL, expr);
        }
        else if (expr instanceof IntegerLiteral)
        {
            return new IntegerLiteral(stmt, mapping, new BigInteger(value.toString()).multiply(
                new BigInteger(((IntegerLiteral)expr).value.toString())), null);
        }
        else if (expr instanceof CharacterLiteral)
        {
            int v = ((CharacterLiteral)expr).getValue().toString().charAt(0);
            return new IntegerLiteral(stmt, mapping, new BigInteger(value.toString()).multiply(
                new BigInteger(""+v)), null);
        }        
        else
        {
            return super.mul(expr);
        }
    }

    /**
     * If both operands are instances of IntegerLiteral, the operation results in BigInteger type.
     */
    public SQLExpression div(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give basic divide
            return new NumericExpression(this, Expression.OP_DIV, expr);
        }
        else if (expr instanceof IntegerLiteral)
        {
            return new IntegerLiteral(stmt, mapping, new BigInteger(value.toString()).divide(
                new BigInteger(((IntegerLiteral)expr).value.toString())), null);
        }
        else if (expr instanceof CharacterLiteral)
        {
            int v = ((CharacterLiteral)expr).getValue().toString().charAt(0);
            return new IntegerLiteral(stmt, mapping, new BigInteger(value.toString()).divide(
                new BigInteger(""+v)), null);
        }        
        else
        {
            return super.div(expr);
        }
    }

    /**
     * If both operands are instances of IntegerLiteral, the operation results in BigInteger type.
     */
    public SQLExpression mod(SQLExpression expr)
    {
        if (expr instanceof IntegerLiteral)
        {
            return new IntegerLiteral(stmt, mapping, new BigInteger(value.toString()).mod(
                new BigInteger(((IntegerLiteral)expr).value.toString())), null);
        }
        else if (expr instanceof CharacterLiteral)
        {
            int v = ((CharacterLiteral)expr).getValue().toString().charAt(0);
            return new IntegerLiteral(stmt, mapping, new BigInteger(value.toString()).mod(
                new BigInteger(""+v)), null);
        }
        else
        {
            return super.mod(expr);
        }
    }

    /**
     * Negate operation. Results in BigInteger type.
     */
    public SQLExpression neg()
    {
        return new IntegerLiteral(stmt, mapping, new BigInteger(value.toString()).negate(), parameterName);
    }

    public Object getValue()
    {
        return value;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#setNotParameter()
     */
    public void setNotParameter()
    {
        if (parameterName == null)
        {
            return;
        }
        parameterName = null;
        st.clearStatement();
        setStatement();
    }

    protected void setStatement()
    {
        st.append(String.valueOf(value));
    }
}