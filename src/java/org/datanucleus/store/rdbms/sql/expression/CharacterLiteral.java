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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.datastore.DatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * Representation of a Character literal in a Query.
 */
public class CharacterLiteral extends CharacterExpression implements SQLLiteral
{
    private final String value;

    /**
     * Constructor for a character literal with a value.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Name of the parameter represented if any (JDBC "?")
     */
    public CharacterLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
        }
        else if (value instanceof Character)
        {
            this.value = ((Character)value).toString();
        }
        else if (value instanceof String)
        {
            this.value = (String)value;
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
        else if (expr instanceof ByteExpression)
        {
            return expr.eq(this);
        }
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.equals(((CharacterLiteral)expr).value));
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
        else if (expr instanceof ByteExpression)
        {
            return expr.ne(this);
        }
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                !value.equals(((CharacterLiteral)expr).value));
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
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((CharacterLiteral)expr).value) < 0);
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
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((CharacterLiteral)expr).value) <= 0);
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
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((CharacterLiteral)expr).value) > 0);
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
        else if (expr instanceof CharacterLiteral)
        {
            return new BooleanLiteral(stmt,
                stmt.getSQLExpressionFactory().getMappingForType(boolean.class, false),
                value.compareTo(((CharacterLiteral)expr).value) >= 0);
        }
        else
        {
            return super.ge(expr);
        }
    }

    public SQLExpression add(SQLExpression expr)
    {
        if (expr instanceof CharacterLiteral)
        {
            int v = value.charAt(0)+((CharacterLiteral)expr).value.charAt(0);
            return new IntegerLiteral(stmt, mapping, Integer.valueOf(v), null);
        }
        else if (expr instanceof IntegerLiteral)
        {
            int v = value.charAt(0)+((Number)((IntegerLiteral)expr).getValue()).intValue();
            return new IntegerLiteral(stmt, mapping, Integer.valueOf(v), null);
        }
        else
        {
            return super.add(expr);
        }
    }
    
    public SQLExpression sub(SQLExpression expr)
    {
        if (expr instanceof CharacterLiteral)
        {
            int v = value.charAt(0)-((CharacterLiteral)expr).value.charAt(0);
            return new IntegerLiteral(stmt, mapping, Integer.valueOf(v), null);
        }
        else if (expr instanceof IntegerLiteral)
        {
            int v = value.charAt(0)-((Number)((IntegerLiteral)expr).getValue()).intValue();
            return new IntegerLiteral(stmt, mapping, Integer.valueOf(v), null);
        }
        else
        {
            return super.sub(expr);
        }
    }    

    public SQLExpression mod(SQLExpression expr)
    {
        if (expr instanceof CharacterLiteral)
        {
            int v = value.charAt(0)%((CharacterLiteral)expr).value.charAt(0);
            return new IntegerLiteral(stmt, mapping, Integer.valueOf(v), null);
        }
        else if (expr instanceof IntegerLiteral)
        {
            int v = value.charAt(0)%((Number)((IntegerLiteral)expr).getValue()).intValue();
            return new IntegerLiteral(stmt, mapping, Integer.valueOf(v), null);
        }       
        else
        {
            return super.mod(expr);
        }
    }

    public SQLExpression neg()
    {
        int v = -(value.charAt(0));
        return new IntegerLiteral(stmt, mapping, Integer.valueOf(v), null);
    }    

    public SQLExpression com()
    {
        int v = ~(value.charAt(0));
        return new IntegerLiteral(stmt, mapping, Integer.valueOf(v), null);
    }

    public SQLExpression invoke(String methodName, List args)
    {
        // TODO Move these into "sql.method.*"
        if (methodName.equals("toUpperCase"))
        {
            return new CharacterLiteral(stmt, mapping, value.toUpperCase(), parameterName);
        }
        else if (methodName.equals("toLowerCase"))
        {
            return new CharacterLiteral(stmt, mapping, value.toLowerCase(), parameterName);
        }

        return super.invoke(methodName, args);
    }

    public Object getValue()
    {
        if (value == null)
        {
            return null;
        }
        return Character.valueOf(value.charAt(0));
    }

    public void setJavaTypeMapping(JavaTypeMapping m)
    {
        super.setJavaTypeMapping(m);
        if (!isParameter())
        {
            // Update the statement in case switched from char to numeric mapping
            setStatement();
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
        setStatement();
    }

    protected void setStatement()
    {
        st.clearStatement();
        DatastoreMapping colMapping = mapping.getDatastoreMapping(0);
        if (colMapping.isIntegerBased())
        {
            st.append("" + (int)this.value.charAt(0));
        }
        else
        {
            st.append('\'').append(this.value).append('\'');
        }
    }
}