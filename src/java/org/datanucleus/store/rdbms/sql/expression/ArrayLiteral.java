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

import java.lang.reflect.Array;
import java.util.ArrayList;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * Representation of array literal.
 */
public class ArrayLiteral extends ArrayExpression implements SQLLiteral
{
    /** value of the array **/
    final Object value;

    /**
     * Constructor for an array literal with a value.
     * @param stmt The SQL statement
     * @param mapping the mapping to use
     * @param value the array value
     * @param parameterName Name of the parameter that this represents if any (as JDBC "?")
     */
    public ArrayLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        this.value = value;
        if (value != null && !value.getClass().isArray())
        {
            throw new NucleusUserException("Invalid argument literal : " + value);
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#getValue()
     */
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
        if (value != null && Array.getLength(value) > 0)
        {
            RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
            elementExpressions = new ArrayList();
            st.append("(");

            boolean hadPrev = false;
            for (int i=0;i<Array.getLength(value);i++)
            {
                Object current = Array.get(value, i);
                if (current != null)
                {
                    JavaTypeMapping m = storeMgr.getSQLExpressionFactory().getMappingForType(current.getClass(), false);
                    SQLExpression expr = storeMgr.getSQLExpressionFactory().newLiteral(stmt, m, current);

                    // Append the SQLExpression (should be a literal) for the current element.
                    st.append(hadPrev ? "," : "");
                    st.append(expr);
                    elementExpressions.add(expr);

                    hadPrev = true;
                }
            }

            st.append(")");
        }
    }
}