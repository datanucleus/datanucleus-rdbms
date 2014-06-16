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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * An SQL expression that will test if a column of a table falls within the given Collection of values.
 * This is used for queries where a transient Collection is passed in as a parameter.
 */
public class CollectionLiteral extends CollectionExpression implements SQLLiteral
{
    private final Collection value;

    /** Expressions for all elements in the Collection **/ 
    private List<SQLExpression> elementExpressions;

    /**
     * Constructor for a collection literal with a value.
     * @param stmt SQL statement
     * @param mapping The mapping to the Collection
     * @param value The transient Collection that is the value.
     * @param parameterName Name of the parameter that this represents if any (as JDBC "?")
     */
    public CollectionLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
        }
        else if (parameterName != null)
        {
            if (value instanceof Collection)
            {
                this.value = (Collection)value;
            }
            else
            {
                this.value = null;
            }
            st.appendParameter(parameterName, mapping, this.value);
        }
        else if (value instanceof Collection)
        {
            this.value = (Collection)value;
            setStatement();
        }
        else
        {
            throw new NucleusException("Cannot create " + this.getClass().getName() + 
                " for value of type " + value.getClass().getName());
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#getValue()
     */
    public Object getValue()
    {
        return value;
    }

    public List<SQLExpression> getElementExpressions()
    {
        return elementExpressions;
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
        if (value != null && value.size() > 0)
        {
            RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
            elementExpressions = new ArrayList();
            st.append("(");

            boolean hadPrev = false;

            for (Iterator it=value.iterator(); it.hasNext();)
            {
                Object current = it.next();
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

    public SQLExpression invoke(String methodName, List args)
    {
        if (methodName.equals("get") && args.size() == 1 && value instanceof List)
        {
            // Map.get(expr)
            SQLExpression argExpr = (SQLExpression)args.get(0);
            if (argExpr instanceof SQLLiteral)
            {
                Object val = ((List)value).get((Integer)((SQLLiteral)argExpr).getValue());
                if (val == null)
                {
                    return new NullLiteral(stmt, null, null, null);
                }
                JavaTypeMapping m = 
                    stmt.getRDBMSManager().getSQLExpressionFactory().getMappingForType(val.getClass(), false);
                return new ObjectLiteral(stmt, m, val, null);
            }

            // Don't support List.get(SQLExpression)
            throw new IllegalExpressionOperationException(this, "get", argExpr);
        }

        return super.invoke(methodName, args);
    }
}