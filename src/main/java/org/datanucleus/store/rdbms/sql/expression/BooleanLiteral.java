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
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.mapping.column.ColumnMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * Representation of a Boolean literal in a Query.
 */
public class BooleanLiteral extends BooleanExpression implements SQLLiteral
{
    private final Boolean value;

    /**
     * Creates a boolean literal with the specified value, using the provided mapping.
     * The boolean literal DOESN'T have closure using this constructor.
     * This constructor will use the provided mapping in determining whether to put "TRUE"/"1=0", 
     * or "1"/"0" or "Y"/"N" in the SQL.
     * @param stmt The SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Parameter name (represented as JDBC "?")
     */
    public BooleanLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
        }
        else if (value instanceof Boolean)
        {
            this.value = (Boolean)value;
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

    /**
     * Creates a boolean literal with the specified value, using the provided mapping.
     * The boolean expression has closure using this constructor.
     * This constructor doesn't take into account the type of the mapping and how a boolean may be stored,
     * just creating a literal (TRUE) and the SQL will simply be "TRUE"/"1=0".
     * @param stmt The SQL statement
     * @param mapping the mapping
     * @param value the boolean value
     */
    public BooleanLiteral(SQLStatement stmt, JavaTypeMapping mapping, Boolean value)
    {
        super(stmt, null, mapping);
        this.value = value;
        this.hasClosure = true;
        setStatement();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#setJavaTypeMapping(org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping)
     */
    @Override
    public void setJavaTypeMapping(JavaTypeMapping mapping)
    {
        super.setJavaTypeMapping(mapping);

        // Update statement since mapping can change how the literal is represented in the SQL
        st.clearStatement();
        if (parameterName != null)
        {
            st.appendParameter(parameterName, mapping, this.value);
        }
        else
        {
            setStatement();
        }
    }

    public Object getValue()
    {
        return Boolean.valueOf(value);
    }

    public BooleanExpression and(SQLExpression expr)
    {
        if (expr instanceof BooleanExpression)
        {
            return value ? (BooleanExpression)expr : this;
        }

        return super.and(expr);
    }

    public BooleanExpression eor(SQLExpression expr)
    {
        if (expr instanceof BooleanExpression)
        {
            return value ? expr.not() : (BooleanExpression)expr;
        }

        return super.eor(expr);
    }

    public BooleanExpression ior(SQLExpression expr)
    {
        if (expr instanceof BooleanExpression)
        {
            return value ? this : (BooleanExpression)expr;
        }

        return super.ior(expr);
    }

    public BooleanExpression not()
    {
        if (hasClosure)
        {
            return new BooleanLiteral(stmt, mapping, !value);
        }

        return new BooleanLiteral(stmt, mapping, !value, null);
    }

    public BooleanExpression eq(SQLExpression expr)
    {
        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof BooleanLiteral)
        {
            // Return a BooleanLiteral with closure for this clause
            BooleanLiteral exprLit = (BooleanLiteral)expr;
            return new BooleanLiteral(stmt, mapping, value == exprLit.value);
        }
        else if (expr instanceof BooleanExpression)
        {
            ColumnMapping datastoreMapping = expr.mapping.getColumnMapping(0);
            if (datastoreMapping.isStringBased())
            {
                // Expression uses "Y", "N"
                return new BooleanExpression(expr, Expression.OP_EQ,
                    new CharacterLiteral(stmt, mapping, value ? "Y" : "N", null));
            }
            else if (datastoreMapping.isIntegerBased() || (datastoreMapping.isBitBased() &&
                    !stmt.getDatastoreAdapter().supportsOption(DatastoreAdapter.BIT_IS_REALLY_BOOLEAN)))
            {
                // Expression uses "1", "0"
                return new BooleanExpression(expr, Expression.OP_EQ,
                    new IntegerLiteral(stmt, mapping, value ? 1 : 0, null));
            }
            else if (stmt.getDatastoreAdapter().supportsOption(DatastoreAdapter.BOOLEAN_COMPARISON))
            {
                return new BooleanExpression(this, Expression.OP_EQ, expr);
            }
            else
            {
                return and(expr).ior(not().and(expr.not()));
            }
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
        else if (expr instanceof BooleanLiteral)
        {
            // Return a BooleanLiteral with closure for this clause
            BooleanLiteral exprLit = (BooleanLiteral)expr;
            return new BooleanLiteral(stmt, mapping, value != exprLit.value);
        }
        else if (expr instanceof BooleanExpression)
        {
            ColumnMapping datastoreMapping = expr.mapping.getColumnMapping(0);
            if (datastoreMapping.isStringBased())
            {
                // Expression uses "Y", "N"
                return new BooleanExpression(expr, Expression.OP_NOTEQ,
                    new CharacterLiteral(stmt, mapping, value ? "Y" : "N", null));
            }
            else if (datastoreMapping.isIntegerBased() || (datastoreMapping.isBitBased() &&
                    !stmt.getDatastoreAdapter().supportsOption(DatastoreAdapter.BIT_IS_REALLY_BOOLEAN)))
            {
                // Expression uses "1", "0"
                return new BooleanExpression(expr, Expression.OP_NOTEQ,
                    new IntegerLiteral(stmt, mapping, value ? 1 : 0, null));
            }
            else if (stmt.getDatastoreAdapter().supportsOption(DatastoreAdapter.BOOLEAN_COMPARISON))
            {
                return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
            }
            return and(expr.not()).ior(not().and(expr));
        }
        else
        {
            return super.ne(expr);
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
        if (hasClosure)
        {
            // Provide complete boolean clause for SQL inclusion
            st.append(this.value ? "TRUE" : "(1=0)");
        }
        else
        {
            // Provide SQL representing the boolean component only
            ColumnMapping datastoreMapping = mapping.getColumnMapping(0);
            if (datastoreMapping.isStringBased())
            {
                // Persisted using "Y", "N"
                st.append(this.value ? "'Y'" : "'N'");
            }
            else if (datastoreMapping.isIntegerBased() || 
                (datastoreMapping.isBitBased() && !stmt.getDatastoreAdapter().supportsOption(DatastoreAdapter.BIT_IS_REALLY_BOOLEAN)))
            {
                // Persisted using "1", "0"
                st.append(this.value ? "1" : "0");
            }
            else
            {
                st.append(this.value ? "TRUE" : "(1=0)");
            }
        }
    }
}