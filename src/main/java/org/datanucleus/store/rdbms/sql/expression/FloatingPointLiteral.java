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
import java.math.RoundingMode;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.util.StringUtils;

/**
 * Representation of a FloatPoint literal in a query.
 */
public class FloatingPointLiteral extends NumericExpression implements SQLLiteral
{
    private final BigDecimal value;

    /**
     * Constructor for a floating point literal with a value.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Name of the parameter that this represents if any (as JDBC "?")
     */
    public FloatingPointLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
        }
        else if (value instanceof Float)
        {
            this.value = new BigDecimal(((Float)value).toString());
        }
        else if (value instanceof Double)
        {
            this.value = new BigDecimal(((Double)value).toString());
        }
        else if (value instanceof BigDecimal)
        {
            this.value = (BigDecimal)value;
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
        else if (expr instanceof FloatingPointLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((FloatingPointLiteral)expr).value) == 0);
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
        else if (expr instanceof FloatingPointLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((FloatingPointLiteral)expr).value) != 0);
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
        else if (expr instanceof FloatingPointLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((FloatingPointLiteral)expr).value) < 0);
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
        else if (expr instanceof FloatingPointLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((FloatingPointLiteral)expr).value) <= 0);
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
        else if (expr instanceof FloatingPointLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((FloatingPointLiteral)expr).value) > 0);
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
        else if (expr instanceof FloatingPointLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((FloatingPointLiteral)expr).value) >= 0);
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

    public SQLExpression add(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give basic addition
            return new NumericExpression(this, Expression.OP_ADD, expr);
        }
        else if (expr instanceof FloatingPointLiteral)
        {
            return new FloatingPointLiteral(stmt, mapping,
                value.add(((FloatingPointLiteral)expr).value), null);
        }
        else
        {
            return super.add(expr);
        }
    }

    public SQLExpression sub(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give basic subtract
            return new NumericExpression(this, Expression.OP_SUB, expr);
        }
        else if (expr instanceof FloatingPointLiteral)
        {
            return new FloatingPointLiteral(stmt, mapping,
                value.subtract(((FloatingPointLiteral)expr).value), null);
        }
        else
        {
            return super.sub(expr);
        }
    }

    public SQLExpression mul(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give basic multiply
            return new NumericExpression(this, Expression.OP_MUL, expr);
        }
        else if (expr instanceof FloatingPointLiteral)
        {
            return new FloatingPointLiteral(stmt, mapping,
                value.multiply(((FloatingPointLiteral)expr).value), null);
        }
        else
        {
            return super.mul(expr);
        }
    }

    public SQLExpression div(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give basic divide
            return new NumericExpression(this, Expression.OP_DIV, expr);
        }
        else if (expr instanceof FloatingPointLiteral)
        {
            return new FloatingPointLiteral(stmt, mapping, value.divide(((FloatingPointLiteral)expr).value, RoundingMode.DOWN), null);
        }
        else
        {
            return super.div(expr);
        }
    }

    public SQLExpression neg()
    {
        return new FloatingPointLiteral(stmt, mapping, value.negate(), null);
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
        st.append(StringUtils.exponentialFormatBigDecimal(this.value));
    }
}