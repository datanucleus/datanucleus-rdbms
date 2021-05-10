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
 * Representation of a Byte literal.
 */
public class ByteLiteral extends NumericExpression implements SQLLiteral
{
    private final BigInteger value;

    /**
     * Creates a byte literal.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Name of the parameter that this represents (as JDBC "?")
     */
    public ByteLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
        }
        else if (value instanceof BigInteger)
        {
            this.value = (BigInteger)value;
        }
        else if (value instanceof Byte)
        {
            this.value = BigInteger.valueOf(((Byte)value).longValue());
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
        else if (expr instanceof ByteLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((ByteLiteral)expr).value) == 0);
        }
        else if (expr instanceof CharacterExpression)
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
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof ByteLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((ByteLiteral)expr).value) != 0);
        }
        else if (expr instanceof CharacterExpression)
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
        else if (expr instanceof ByteLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((ByteLiteral)expr).value) < 0);
        }
        else if (expr instanceof CharacterExpression)
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
        else if (expr instanceof ByteLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((ByteLiteral)expr).value) <= 0);
        }
        else if (expr instanceof CharacterExpression)
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
        else if (expr instanceof ByteLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((ByteLiteral)expr).value) > 0);
        }
        else if (expr instanceof CharacterExpression)
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
        else if (expr instanceof ByteLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((ByteLiteral)expr).value) >= 0);
        }
        else if (expr instanceof CharacterExpression)
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
        if (expr instanceof ByteLiteral)
        {
            return new ByteLiteral(stmt, mapping, value.add(((ByteLiteral)expr).value), null);
        }

        return super.add(expr);
    }

    public SQLExpression sub(SQLExpression expr)
    {
        if (expr instanceof ByteLiteral)
        {
            return new ByteLiteral(stmt, mapping, value.subtract(((ByteLiteral)expr).value), null);
        }

        return super.sub(expr);
    }

    public SQLExpression mul(SQLExpression expr)
    {
        if (expr instanceof ByteLiteral)
        {
            return new ByteLiteral(stmt, mapping, value.multiply(((ByteLiteral)expr).value), null);
        }

        return super.mul(expr);
    }

    public SQLExpression div(SQLExpression expr)
    {
        if (expr instanceof ByteLiteral)
        {
            return new ByteLiteral(stmt, mapping, value.divide(((ByteLiteral)expr).value), null);
        }

        return super.div(expr);
    }

    public SQLExpression mod(SQLExpression expr)
    {
        if (expr instanceof ByteLiteral)
        {
            return new ByteLiteral(stmt, mapping, value.mod(((ByteLiteral)expr).value), null);
        }

        return super.mod(expr);
    }

    public SQLExpression neg()
    {
        return new ByteLiteral(stmt, mapping, value.negate(), null);
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