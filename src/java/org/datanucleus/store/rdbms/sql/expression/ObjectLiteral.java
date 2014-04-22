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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Representation of an Object literal in a query.
 */
public class ObjectLiteral extends ObjectExpression implements SQLLiteral
{
    private Object value;

    /**
     * Constructor for an Object literal with a value.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the Object value
     * @param parameterName Name of the parameter that this represents if any (as JDBC "?")
     */
    public ObjectLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.value = value;
        this.parameterName = parameterName;

        if (parameterName != null)
        {
            if (value != null)
            {
                this.subExprs = new ColumnExpressionList();
                addSubexpressionsForValue(this.value, mapping);
                // Note : not added anything to the "st" since this will be used in comparison
                // and the comparison defines the SQL to use.
            }
            if (mapping.getNumberOfDatastoreMappings() == 1)
            {
                st.appendParameter(parameterName, mapping, this.value);
            }
        }
        else
        {
            this.subExprs = new ColumnExpressionList();
            if (value != null)
            {
                addSubexpressionsForValue(this.value, mapping);
                // Note : not added anything to the "st" since this will be used in comparison
                // and the comparison defines the SQL to use.
            }
            st.append(subExprs.toString());
        }
    }

    /**
     * Method to add subExprs for the supplied mapping, consistent with the supplied value.
     * The value can be a persistent object, or an identity (datastore/application).
     * @param value The value
     * @param mapping The mapping
     */
    private void addSubexpressionsForValue(Object value, JavaTypeMapping mapping)
    {
        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        ClassLoaderResolver clr = stmt.getClassLoaderResolver();
        String objClassName = value.getClass().getName();
        if (mapping instanceof PersistableMapping)
        {
            objClassName = mapping.getType();
        }
        else if (IdentityUtils.isDatastoreIdentity(value))
        {
            objClassName = IdentityUtils.getTargetClassNameForIdentitySimple(value);
        }
        AbstractClassMetaData cmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(objClassName, clr);
        if (cmd != null)
        {
            // Literal representing a persistable object (or its identity)
            int numCols = mapping.getNumberOfDatastoreMappings();
            for (int i=0;i<numCols;i++)
            {
                ColumnExpression colExpr = null;
                if (parameterName == null && mapping instanceof PersistableMapping)
                {
                    // Literal is a persistable object
                    Object colValue = ((PersistableMapping)mapping).getValueForDatastoreMapping(stmt.getRDBMSManager().getNucleusContext(), i, value);
                    colExpr = new ColumnExpression(stmt, colValue);
                }
                else
                {
                    // Literal is a parameter
                    colExpr = new ColumnExpression(stmt, parameterName, mapping, value, i);
                }
                subExprs.addExpression(colExpr);
            }
        }
        else
        {
            // TODO Support OID
            NucleusLogger.GENERAL.error(">> ObjectLiteral doesn't yet cater for values of type " + StringUtils.toJVMIDString(value));
        }
    }

    public Object getValue()
    {
        return value;
    }

    /**
     * Method called when the query contains "object == value".
     * @param expr The expression
     * @return The resultant expression for this query relation
     */
    public BooleanExpression eq(SQLExpression expr)
    {
        addSubexpressionsToRelatedExpression(expr);

        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (value == null)
        {
            return new NullLiteral(stmt, null, null, null).eq(expr);
        }
        else if (expr instanceof ObjectExpression)
        {
            return ExpressionUtils.getEqualityExpressionForObjectExpressions(this, (ObjectExpression)expr, true);
        }
        else
        {
            return super.eq(expr);
        }
    }

    /**
     * Method called when the query contains "object NOTEQUALS value".
     * @param expr The expression
     * @return The resultant expression for this query relation
     */
    public BooleanExpression ne(SQLExpression expr)
    {
        addSubexpressionsToRelatedExpression(expr);

        if (isParameter() || expr.isParameter())
        {
            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (value == null)
        {
            return new NullLiteral(stmt, null, null, null).ne(expr);
        }
        else if (expr instanceof ObjectExpression)
        {
            return ExpressionUtils.getEqualityExpressionForObjectExpressions(this, (ObjectExpression)expr, false);
        }
        else
        {
            return super.ne(expr);
        }
    }

    public String toString()
    {
        if (value != null)
        {
            return super.toString() + " = " + value.toString();
        }
        else
        {
            return super.toString() + " = NULL";
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
        if (parameterName == null)
        {
            this.subExprs = new ColumnExpressionList();
            if (value != null)
            {
                addSubexpressionsForValue(this.value, mapping);
            }
            st.append(subExprs.toString());
        }
        else
        {
            st.appendParameter(parameterName, mapping, this.value);
        }
    }
}