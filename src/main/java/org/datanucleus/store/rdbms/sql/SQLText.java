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
package org.datanucleus.store.rdbms.sql;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression.ColumnExpressionList;

/**
 * Representation of a snippet of an SQL statement. May contain parameters.
 * A 'parameter' in this context is an input parameter to the query (which will map on to a JDBC '?'
 * in the resultant SQL).
 * Call "applyParametersToStatement()" to set the parameter values in the PreparedStatement.
 */
public class SQLText
{
    /** Cached SQL if already generated. */
    private String sql;

    private List<SQLStatementParameter> parameters = null;

    private boolean encloseInParentheses = false;
    private String postpend;
    private List appended;

    /**
     * Constructor
     */
    public SQLText()
    {
        appended = new ArrayList();
    }

    /**
     * Constructor
     * @param initialSQLText SQL text to start from
     */
    public SQLText(String initialSQLText)
    {
        this();
        append(initialSQLText);
    }
    
    /**
     * Convenience method to reset the SQL for the statement.
     * This is used when updating an expression internally, and need to regenerate
     * the statement.
     */
    public void clearStatement()
    {
        sql = null;
        appended.clear();
    }

    /**
     * Set to enclose this SQL in parentheses.
     */
    public void encloseInParentheses()
    {
        sql = null;
        this.encloseInParentheses = true;
    }

    /**
     * Set the String to append at the end of the SQL.
     * @param s the string
     * @return the SQLText
     */
    public SQLText postpend(String s)
    {
        sql = null;
        postpend = s;
        return this;
    }

    /**
     * Prepend some SQL as a string.
     * @param s The string
     * @return The SQLText
     */
    public SQLText prepend(String s)
    {
        sql = null;
        appended.add(0, s);
        return this;
    }

    /**
     * Append a character to the SQL.
     * @param c the char
     * @return the SQLText
     */    
    public SQLText append(char c)
    {
        sql = null;
        appended.add(Character.valueOf(c));
        return this;
    }

    /**
     * Append some SQL as a string.
     * @param s the String
     * @return the SQLText
     */    
    public SQLText append(String s)
    {
        sql = null;
        appended.add(s);
        return this;
    }

    /**
     * Append an SQLStatement.
     * @param stmt the SQL Statement
     * @return the SQLText
     */    
    public SQLText append(SQLStatement stmt)
    {
        sql = null;
        appended.add(stmt);
        return this;
    }

    /**
     * Append a ColumnExpressionList.
     * @param exprList the ColumnExpression list
     * @return the SQLText
     */    
    public SQLText append(ColumnExpressionList exprList)
    {
        sql = null;
        appended.add(exprList);
        return this;
    }

    /**
     * Append a SQLText
     * @param st the SQLText
     * @return the SQLText
     */    
    public SQLText append(SQLText st)
    {
        sql = null;
        appended.add(st.toSQL());
        if (st.parameters != null)
        {
            if (parameters == null)
            {
                parameters = new ArrayList();
            }
            parameters.addAll(st.parameters);
        }
        return this;
    }

    /**
     * Append an SQLExpression.
     * @param expr the SQLExpression
     * @return the SQLText
     */    
    public SQLText append(SQLExpression expr)
    {
        sql = null;
        appended.add(expr);
        return this;
    }

    /**
     * Append a parameter represented by a mapping (single datastore column).
     * @param name The parameter name
     * @param mapping the mapping
     * @param value the parameter value
     * @return the SQLText
     */
    public SQLText appendParameter(String name, JavaTypeMapping mapping, Object value)
    {
        return appendParameter(name, mapping, value, -1);
    }

    /**
     * Append a parameter represented by a mapping, for a column of a multi-column mapping.
     * @param name The parameter name
     * @param mapping the mapping
     * @param value the parameter value
     * @param columnNumber Number of the column represented here
     * @return the SQLText
     */
    public SQLText appendParameter(String name, JavaTypeMapping mapping, Object value, int columnNumber)
    {
        sql = null;
        appended.add(new SQLStatementParameter(name, mapping, value, columnNumber));
        return this;
    }

    /**
     * Convenience method to change the mapping used for a parameter, if it is referenced by this
     * SQL text object.
     * @param parameterName Name of the parameter
     * @param mapping The mapping to use instead
     */
    public void changeMappingForParameter(String parameterName, JavaTypeMapping mapping)
    {
        Iterator iter = appended.iterator();
        while (iter.hasNext())
        {
            Object obj = iter.next();
            if (obj instanceof SQLStatementParameter)
            {
                SQLStatementParameter param = (SQLStatementParameter)obj;
                if (param.getName().equalsIgnoreCase(parameterName))
                {
                    param.setMapping(mapping);
                }
            }
        }
    }

    /**
     * Method to set the parameters in the supplied PreparedStatement using their mappings and
     * provided values.
     * @param ec execution context
     * @param ps The PreparedStatement
     */
    public void applyParametersToStatement(ExecutionContext ec, PreparedStatement ps)
    {
        if (parameters != null)
        {
            int num = 1;

            Iterator<SQLStatementParameter> i = parameters.iterator();
            while (i.hasNext())
            {
                SQLStatementParameter param = i.next();
                JavaTypeMapping mapping = param.getMapping();
                if (mapping != null)
                {
                    Object value = param.getValue();
                    if (param.getColumnNumber() >= 0)
                    {
                        Object colValue = null;
                        if (value != null)
                        {
                            // Parameter is for a particular column of the overall object/mapping
                            // so assume that the object is persistable (or id of persistable)
                            ClassLoaderResolver clr = ec.getClassLoaderResolver();
                            AbstractClassMetaData cmd =
                                ec.getMetaDataManager().getMetaDataForClass(mapping.getType(), clr);
                            RDBMSStoreManager storeMgr = mapping.getStoreManager();
                            if (cmd.getIdentityType() == IdentityType.DATASTORE)
                            {
                                colValue = mapping.getValueForDatastoreMapping(ec.getNucleusContext(), 
                                    param.getColumnNumber(), value);
                            }
                            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                            {
                                colValue = SQLStatementHelper.getValueForPrimaryKeyIndexOfObjectUsingReflection(
                                    value, param.getColumnNumber(), cmd, storeMgr, clr);
                            }
                        }
                        mapping.getDatastoreMapping(param.getColumnNumber()).setObject(ps, num, colValue);
                        num++;
                    }
                    else
                    {
                        mapping.setObject(ec, ps, MappingHelper.getMappingIndices(num, mapping), value);
                        if (mapping.getNumberOfDatastoreMappings() > 0)
                        {
                            num += mapping.getNumberOfDatastoreMappings();
                        }
                        else
                        {
                            num += 1;
                        }
                    }
                }
            }
        }
    }

    /**
     * Accessor for the parameters for this SQLText (including all sub SQLText)
     * @return The list of parameters (in the order they appear in the SQL)
     */
    public List<SQLStatementParameter> getParametersForStatement()
    {
        return parameters;
    }

    /**
     * Accessor for the SQL of the statement.
     * @return The SQL text
     */
    public String toSQL()
    {
        if (sql != null)
        {
            // Use cached
            return sql;
        }

        StringBuilder sql = new StringBuilder();
        if (encloseInParentheses)
        {
            sql.append("(");
        }
        for (int i = 0; i < appended.size(); i++)
        {
            Object item = appended.get(i);
            if (item instanceof SQLExpression)
            {
                SQLExpression expr = (SQLExpression) item;
                SQLText st = expr.toSQLText();
                sql.append(st.toSQL());

                if (st.parameters != null)
                {
                    if (parameters == null)
                    {
                        parameters = new ArrayList();
                    }
                    parameters.addAll(st.parameters);
                }
            }
            else if (item instanceof SQLStatementParameter)
            {
                SQLStatementParameter param = (SQLStatementParameter) item;
                sql.append('?');

                if (parameters == null)
                {
                    parameters = new ArrayList();
                }
                parameters.add(param);
            }
            else if (item instanceof SQLStatement)
            {
                SQLStatement stmt = (SQLStatement) item;
                SQLText st = stmt.getSQLText();
                sql.append(st.toSQL());
                if (st.parameters != null)
                {
                    if (parameters == null)
                    {
                        parameters = new ArrayList();
                    }
                    parameters.addAll(st.parameters);
                }
            }
            else if (item instanceof SQLText)
            {
                SQLText st = (SQLText) item;
                sql.append(st.toSQL());
                if (st.parameters != null)
                {
                    if (parameters == null)
                    {
                        parameters = new ArrayList();
                    }
                    parameters.addAll(st.parameters);
                }
            }
            else
            {
                sql.append(item);
            }
        }
        if (encloseInParentheses)
        {
            sql.append(")");
        }
        sql.append((postpend == null ? "" : postpend));
        this.sql = sql.toString();

        return this.sql;
    }

    /**
     * Accessor for the string form of the statement.
     * @return String form of the statement
     */
    public String toString()
    {
        return toSQL();
    }
}