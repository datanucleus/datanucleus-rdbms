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

import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.table.Column;

/**
 * Representation of a column expression.
 * Used within ObjectExpression for handling particular columns representing an object.
 */
public class ColumnExpression extends SQLExpression
{
    /** The column this represents. Only used when not a parameter. */
    Column column;

    Object value;

    boolean omitTableFromString = false;

    boolean appendedSelfToStatement = false;
    
    /**
     * Constructor for an SQL expression for a parameter.
     * @param stmt The statement
     * @param parameterName Name of the parameter
     * @param mapping Mapping for the column
     * @param value The value for the parameter for this column
     * @param colNumber Column number of the mapping being represented here
     */
    protected ColumnExpression(SQLStatement stmt, String parameterName, JavaTypeMapping mapping, 
            Object value, int colNumber)
    {
        super(stmt, null, mapping);
        st.appendParameter(parameterName, mapping, value, colNumber);
        appendedSelfToStatement = true;
    }

    /**
     * Constructor for an SQL expression for a column.
     * @param stmt The statement
     * @param table The table in the statement
     * @param col The column
     */
    protected ColumnExpression(SQLStatement stmt, SQLTable table, Column col)
    {
        super(stmt, table, null);
        this.column = col;
        // Remove append b/c setOmitTableFromString requires that we don't generate
        // the expression for this column until we actually need it (and the caller
        // has adjusted setOmitTableFromString appropriately).
    }

    /**
     * Constructor for an SQL expression for a literal value.
     * @param stmt The statement
     * @param value The literal value
     */
    protected ColumnExpression(SQLStatement stmt, Object value)
    {
        super(stmt, null, null);
        this.value = value;
        // Remove append b/c setOmitTableFromString requires that we don't generate
        // the expression for this column until we actually need it (and the caller
        // has adjusted setOmitTableFromString appropriately).
    }

    /**
     * B/c of setOmitTableFromString, we dynamically call toString when a caller
     * asks for it, not at construction time. We only do this once.
     * 
     * @return The SQL
     */
    @Override
    public SQLText toSQLText()
    {   
        if (!appendedSelfToStatement) {
            st.append(toString());
            appendedSelfToStatement = true;
        }
        
        return st;
    }
    
    public BooleanExpression eq(SQLExpression expr)
    {
        return new BooleanExpression(this, Expression.OP_EQ, expr);
    }

    public BooleanExpression noteq(SQLExpression expr)
    {
        return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
    }

    public void setOmitTableFromString(boolean omitTable)
    {
        this.omitTableFromString = omitTable;
        
        // XXX: The "appended" list in the SQLText will now be wrong b/c it was generated
        // at construction time using toString. We need this to be dynamic.
    }

    /**
     * Stringifier method to return this "column" in a form for use in SQL statements.
     * This can be of the following form(s)
     * <pre>
     * TABLEALIAS.MYCOLUMN
     * MYTABLE.MYCOLUMN
     * </pre>
     * @return The String form for use
     */
    public String toString()
    {
        if (value != null)
        {
            if (value instanceof String || value instanceof Character)
            {
                return "'" + value + "'";
            }
            else
            {
                return "" + value;
            }
        }
        if (table == null)
        {
            // Column parameter
            return "?";
        }
        else
        {
            if (omitTableFromString)
            {
                return column.getIdentifier().toString();
            }
            else
            {
                if (table.getAlias() != null)
                {
                    return table.getAlias() + "." + column.getIdentifier().toString();
                }
                else
                {
                    return table.getTable() + "." + column.getIdentifier().toString();
                }
            }
        }
    }
}