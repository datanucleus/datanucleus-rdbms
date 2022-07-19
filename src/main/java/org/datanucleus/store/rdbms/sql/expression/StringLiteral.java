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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Representation of a string literal.
 */
public class StringLiteral extends StringExpression implements SQLLiteral
{
    private final String value;

    /**
     * Constructor for a String literal with a value.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Name of the parameter that this represents (as JDBC "?")
     */
    public StringLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
        }
        else if (value instanceof String)
        {
            this.value = (String)value;
        }
        else if (value instanceof Character)
        {
            this.value = ((Character)value).toString();
        }
        else
        {
            Class<?> type = value.getClass();
            if (mapping != null)
            {
                type = mapping.getJavaType();
            }
            TypeConverter converter = stmt.getRDBMSManager().getNucleusContext().getTypeManager().getTypeConverterForType(type, String.class);
            if (converter != null)
            {
                // Use converter
                this.value = (String)converter.toDatastoreType(value);
            }
            else
            {
                throw new NucleusException("Cannot create " + this.getClass().getName() +
                    " for value of type " + value.getClass().getName());
            }
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

    /**
     * Convenience method to generate the statement without any quotes.
     * This is called when we create a literal using a mapping, and don't want quotes
     * because the string is an SQL keyword.
     */
    public void generateStatementWithoutQuotes()
    {
        st.clearStatement();
        st.append(value.replace("'", "''"));
    }

    public Object getValue()
    {
        return value;
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
        else if (expr instanceof StringLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.equals(((StringLiteral)expr).value));
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
        else if (expr instanceof StringLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                !value.equals(((StringLiteral)expr).value));
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
        else if (expr instanceof StringLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((StringLiteral)expr).value) < 0);
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
        else if (expr instanceof StringLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((StringLiteral)expr).value) <= 0);
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
        else if (expr instanceof StringLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((StringLiteral)expr).value) > 0);
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
        else if (expr instanceof StringLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((StringLiteral)expr).value) >= 0);
        }
        else
        {
            return super.ge(expr);
        }
    }

    public SQLExpression add(SQLExpression expr)
    {
        // String.add mapped to concatenation of strings
        if (expr.isParameter() || isParameter())
        {
            return super.add(expr);
        }
        else if (expr instanceof StringLiteral)
        {
            return new StringLiteral(stmt, mapping, value.concat(((StringLiteral)expr).value), null);
        }
        else if (expr instanceof CharacterLiteral)
        {
            return new StringLiteral(stmt, mapping, value.concat(((SQLLiteral)expr).getValue().toString()), null);
        }
        else if (expr instanceof IntegerLiteral || expr instanceof FloatingPointLiteral || expr instanceof BooleanLiteral)
        {
            return new StringLiteral(stmt, mapping, value.concat(((SQLLiteral)expr).getValue().toString()), null);
        }        
        else
        {
            return super.add(expr);
        }
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
        // Escape any single-quotes
        if (value == null)
        {
            st.append('\'').append('\'');
        }
        else
        {
            st.append('\'').append(this.value.replace("'", "''")).append('\'');
        }
    }
}