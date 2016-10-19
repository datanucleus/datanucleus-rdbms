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
import java.util.Map;
import java.util.Set;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * An SQL expression that will test if a column of a table falls within the given Map. 
 * This is used for queries where a Map is passed in as a parameter.
 */
public class MapLiteral extends MapExpression implements SQLLiteral
{
    private final Map value;

    private final MapValueLiteral mapValueLiteral;
    private final MapKeyLiteral mapKeyLiteral; 

    /**
     * Constructor for a map literal with a value.
     * @param stmt  The SQLStatement the MapLiteral will be used in.
     * @param mapping The mapping to the Map
     * @param value The Map that is the value.
     * @param parameterName Name of the parameter that this represents if any (JDBC "?")
     */
    public MapLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
            this.mapKeyLiteral = null;
            this.mapValueLiteral = null;
        }
        else if (value instanceof Map)
        {
            Map mapValue = (Map)value;
            this.value = mapValue;

            if (parameterName != null)
            {
                this.mapKeyLiteral = null;
                this.mapValueLiteral = null;
                st.appendParameter(parameterName, mapping, this.value);
            }
            else
            {
                this.mapValueLiteral = new MapValueLiteral(stmt, mapping, value);
                this.mapKeyLiteral = new MapKeyLiteral(stmt, mapping, value);
            }
        }
        else
        {
            throw new NucleusException("Cannot create " + this.getClass().getName() + " for value of type " + value.getClass().getName());
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#getValue()
     */
    public Object getValue()
    {
        return value;
    }

    /**
     * An SQL expression that will test if a column of a table falls within the given Map's keys.
     */
    public static class MapKeyLiteral extends SQLExpression implements SQLLiteral
    {
        private final Map value;

        /** Expressions for all keys in the Map **/ 
        private List<SQLExpression> keyExpressions;

        /**
         * Constructor.
         * @param stmt SQL statement
         * @param mapping The mapping for the Map
         * @param value The transient Map that is the value.
         */
        public MapKeyLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value)
        {
            super(stmt, null, mapping);

            if (value instanceof Map)
            {
                Map mapValue = (Map)value;
                this.value = mapValue;
                setStatement();
            }
            else
            {
                throw new NucleusException("Cannot create " + this.getClass().getName() + 
                    " for value of type " + (value != null ? value.getClass().getName() : null));
            }
        }

        public List<SQLExpression> getKeyExpressions()
        {
            return keyExpressions;
        }

        public SQLExpression invoke(String methodName, List args)
        {
            if (methodName.equals("get") && args.size() == 1)
            {
                // Map.get(expr)
                SQLExpression argExpr = (SQLExpression)args.get(0);
                if (argExpr instanceof SQLLiteral)
                {
                    Object val = value.get(((SQLLiteral)argExpr).getValue());
                    if (val == null)
                    {
                        return new NullLiteral(stmt, null, null, null);
                    }
                    JavaTypeMapping m = stmt.getRDBMSManager().getSQLExpressionFactory().getMappingForType(val.getClass(), false);
                    return new ObjectLiteral(stmt, m, val, null);
                }

                // Don't support Map.get(SQLExpression)
                throw new IllegalExpressionOperationException(this, "get", argExpr);
            }

            return super.invoke(methodName, args);
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
            boolean isEmpty = (value == null || value.size() == 0);
            if (!isEmpty)
            {
                RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
                st.append("(");
                keyExpressions = new ArrayList();

                boolean hadPrev = false;
                Set keys = value.keySet();
                for (Iterator it=keys.iterator(); it.hasNext();)
                {
                    Object current = it.next();
                    if (null != current)
                    {
                        JavaTypeMapping keyMapping = storeMgr.getSQLExpressionFactory().getMappingForType(current.getClass(), false);
                        SQLExpression keyExpr = storeMgr.getSQLExpressionFactory().newLiteral(stmt, keyMapping, current);

                        // Append the SQLExpression (should be a literal) for the current key
                        st.append(hadPrev ? "," : "");
                        st.append(keyExpr);
                        keyExpressions.add(keyExpr);

                        hadPrev = true;
                    }
                }

                st.append(")");
            }
        }
    }

    /**
     * An SQL expression that will test if a column of a table falls within the given Map's values.
     */
    public static class MapValueLiteral extends SQLExpression implements SQLLiteral
    {
        private final Map value;

        /** Expressions for all values in the Map **/ 
        private List<SQLExpression> valueExpressions;

        /**
         * Constructor.
         * @param stmt SQL Statement
         * @param mapping The mapping to the Map
         * @param value The transient Map that is the value.
         */
        public MapValueLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value)
        {
            super(stmt, null, mapping);

            if (value instanceof Map)
            {
                Map mapValue = (Map)value;
                this.value = mapValue;
                setStatement();
            }
            else
            {
                throw new NucleusException("Cannot create " + this.getClass().getName() + " for value of type " + (value != null ? value.getClass().getName() : null));
            }
        }

        public List<SQLExpression> getValueExpressions()
        {
            return valueExpressions;
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
            boolean isEmpty = (value == null || value.size() == 0);
            if (!isEmpty)
            {
                RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
                valueExpressions = new ArrayList();
                st.append("(");

                boolean hadPrev = false;
                Collection values = value.values();
                for (Iterator it=values.iterator(); it.hasNext();)
                {
                    Object current = it.next();
                    if (null != current)
                    {
                        JavaTypeMapping valueMapping = storeMgr.getSQLExpressionFactory().getMappingForType(current.getClass(), false);
                        SQLExpression valueExpr = storeMgr.getSQLExpressionFactory().newLiteral(stmt, valueMapping, current);

                        // Append the SQLExpression (should be a literal) for the current element.
                        st.append(hadPrev ? "," : "");
                        st.append(valueExpr);
                        valueExpressions.add(valueExpr);

                        hadPrev = true;
                    }
                }

                st.append(")");
            }
        }
    }

    public MapKeyLiteral getKeyLiteral()
    {
        return mapKeyLiteral;
    }

    public MapValueLiteral getValueLiteral()
    {
        return mapValueLiteral;
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
    }
}