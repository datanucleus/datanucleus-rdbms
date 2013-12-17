/**********************************************************************
Copyright (c) 2006 Jorg von Frantzius and others. All rights reserved.
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
2006 Andy Jefferson - cater for batched statements
2010 Andy Jefferson - clean up output of parameters, and allow for non-printable forms
    ...
**********************************************************************/
package org.datanucleus.store.rdbms;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.datanucleus.exceptions.NucleusUserException;

/**
 * Wrapper for a PreparedStatement, providing access to a String representation of the statement 
 * with replaced actual parameters.
 */
class ParamLoggingPreparedStatement implements PreparedStatement
{
    /** the wrapped PreparedStatement */
    private final PreparedStatement ps;

    /** The current "sub" statement. */
    private SubStatement currentStatement = null;

    /** List of sub-statements (to cater for batched statements). Null if normal PreparedStatement. */
    private List subStatements = null;

    private boolean paramAngleBrackets = true;

    private static final String DN_UNPRINTABLE = "DN_UNPRINTABLE";

    /** Inner class representing a sub-part of the PreparedStatement (to allow for batched statements). */
    static class SubStatement
    {
        public final Map parameters = new HashMap();
        public final String statementText;
        
        public SubStatement(String statementText)
        {
            this.statementText = statementText;
        }
    }

    /**
     * Constructor. This will use an angle bracket around parameter values.
     * @param ps The underlying PreparedStatement
     * @param jdbcSql The statement text (including "?" placeholders)
     */
    public ParamLoggingPreparedStatement(PreparedStatement ps, String jdbcSql)
    {
        this.ps = ps;
        currentStatement = new SubStatement(jdbcSql);
    }

    public void setParamsInAngleBrackets(boolean flag)
    {
        this.paramAngleBrackets = flag;
    }

    /**
     * Return the given SQL, with JDBC parameters replaced by the actual parameter values.
     * @return the statement text with replaced parameter values
     */
    public String getStatementWithParamsReplaced()
    {
        StringBuilder statementWithParams = new StringBuilder();
        if (subStatements == null)
        {
            return getStatementWithParamsReplacedForSubStatement(currentStatement);
        }
        else
        {
            // Batched PreparedStatement
            statementWithParams.append("BATCH [");
            Iterator iter = subStatements.iterator();
            while (iter.hasNext())
            {
                SubStatement stParams = (SubStatement)iter.next();
                String stmt = getStatementWithParamsReplacedForSubStatement(stParams);
                
                statementWithParams.append(stmt);
                if (iter.hasNext())
                {
                    statementWithParams.append("; ");
                }
            }
            statementWithParams.append("]");
            return statementWithParams.toString();
        }
    }

    /**
     * Convenience method to return the statement text (with params replaced) for a "SubStatement" object.
     * @param stParams The SubStatement
     * @return The statement text string
     */
    private String getStatementWithParamsReplacedForSubStatement(SubStatement stParams)
    {
        StringBuilder statementWithParams = new StringBuilder();
        StringTokenizer tokenizer = new StringTokenizer(stParams.statementText, "?", true);

        int i = 1;
        while (tokenizer.hasMoreTokens())
        {
            String token = tokenizer.nextToken();
            if (token.equals("?"))
            {
                Object paramValue = DN_UNPRINTABLE;
                Integer paramPos = Integer.valueOf(i++);
                if (stParams.parameters.containsKey(paramPos))
                {
                    paramValue = stParams.parameters.get(paramPos);
                }
                appendParamValue(statementWithParams, paramValue);
            }
            else
            {
                statementWithParams.append(token);
            }
        }

        // did we really replace anything?
        if (i > 1)
        {
            return statementWithParams.toString();
        }
        else
        {
            return stParams.statementText;
        }
    }

    private void appendParamValue(StringBuilder statementWithParams, final Object paramValue)
    {
        if (paramAngleBrackets)
        {
            if (paramValue instanceof String)
            {
                if (paramValue.equals(DN_UNPRINTABLE))
                {
                    statementWithParams.append("<UNPRINTABLE>");
                }
                else
                {
                    statementWithParams.append("<'" + paramValue + "'>");
                }
            }
            else
            {
                statementWithParams.append("<" + paramValue + ">");
            }
        }
        else
        {
            if (paramValue instanceof String)
            {
                if (paramValue.equals(DN_UNPRINTABLE))
                {
                    statementWithParams.append("<UNPRINTABLE'>");
                }
                else
                {
                    statementWithParams.append("'" + paramValue + "'");
                }
            }
            else
            {
                statementWithParams.append("" + paramValue);
            }
        }
    }

    private void setParameter(int i, Object p)
    {
        currentStatement.parameters.put(Integer.valueOf(i), p);
    }

    /**
     * Return the value for the parameter with index i (1-based, as usual with
     * PreparedStatement)
     * @param i The position number
     * @return The parameter value
     */
    public Object getParameter(int i)
    {
        return currentStatement.parameters.get(Integer.valueOf(i));
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#addBatch()
     */
    public void addBatch() throws SQLException
    {
        // New SubStatement, copying in all current statement and parameters
        SubStatement newSubStmt = new SubStatement(currentStatement.statementText);
        newSubStmt.parameters.putAll(currentStatement.parameters);
        if (subStatements == null)
        {
            subStatements = new ArrayList();
        }
        subStatements.add(newSubStmt);
        ps.addBatch();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#addBatch(java.lang.String)
     */
    public void addBatch(String sql) throws SQLException
    {
        SubStatement newSubStmt = new SubStatement(sql);
        newSubStmt.parameters.putAll(currentStatement.parameters);
        if (subStatements == null)
        {
            // New SubStatement, using the specified SQL
            subStatements = new ArrayList();
        }
        subStatements.add(newSubStmt);
        ps.addBatch(sql);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#cancel()
     */
    public void cancel() throws SQLException
    {
        ps.cancel();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#clearBatch()
     */
    public void clearBatch() throws SQLException
    {
        if (subStatements != null)
        {
            subStatements.clear();
        }
        ps.clearBatch();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#clearParameters()
     */
    public void clearParameters() throws SQLException
    {
        currentStatement.parameters.clear();
        if (subStatements != null)
        {
            // Clear all sub-statements
            Iterator statementsIter = subStatements.iterator();
            while (statementsIter.hasNext())
            {
                SubStatement subStmt = (SubStatement)statementsIter.next();
                subStmt.parameters.clear();
            }
        }

        ps.clearParameters();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#clearWarnings()
     */
    public void clearWarnings() throws SQLException
    {
        ps.clearWarnings();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#close()
     */
    public void close() throws SQLException
    {
        ps.close();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#execute()
     */
    public boolean execute() throws SQLException
    {
        return ps.execute();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#execute(java.lang.String, int)
     */
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        return ps.execute(sql, autoGeneratedKeys);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#execute(java.lang.String, int[])
     */
    public boolean execute(String sql, int[] columnIndexes) throws SQLException
    {
        return ps.execute(sql, columnIndexes);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
     */
    public boolean execute(String sql, String[] columnNames) throws SQLException
    {
        return ps.execute(sql, columnNames);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#execute(java.lang.String)
     */
    public boolean execute(String sql) throws SQLException
    {
        return ps.execute(sql);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#executeBatch()
     */
    public int[] executeBatch() throws SQLException
    {
        return ps.executeBatch();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#executeQuery()
     */
    public ResultSet executeQuery() throws SQLException
    {
        return ps.executeQuery();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#executeQuery(java.lang.String)
     */
    public ResultSet executeQuery(String sql) throws SQLException
    {
        return ps.executeQuery(sql);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#executeUpdate()
     */
    public int executeUpdate() throws SQLException
    {
        return ps.executeUpdate();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#executeUpdate(java.lang.String, int)
     */
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        return ps.executeUpdate(sql, autoGeneratedKeys);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
     */
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
    {
        return ps.executeUpdate(sql, columnIndexes);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#executeUpdate(java.lang.String,
     * java.lang.String[])
     */
    public int executeUpdate(String sql, String[] columnNames) throws SQLException
    {
        return ps.executeUpdate(sql, columnNames);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#executeUpdate(java.lang.String)
     */
    public int executeUpdate(String sql) throws SQLException
    {
        return ps.executeUpdate(sql);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getConnection()
     */
    public Connection getConnection() throws SQLException
    {
        return ps.getConnection();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getFetchDirection()
     */
    public int getFetchDirection() throws SQLException
    {
        return ps.getFetchDirection();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getFetchSize()
     */
    public int getFetchSize() throws SQLException
    {
        return ps.getFetchSize();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getGeneratedKeys()
     */
    public ResultSet getGeneratedKeys() throws SQLException
    {
        return ps.getGeneratedKeys();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getMaxFieldSize()
     */
    public int getMaxFieldSize() throws SQLException
    {
        return ps.getMaxFieldSize();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getMaxRows()
     */
    public int getMaxRows() throws SQLException
    {
        return ps.getMaxRows();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#getMetaData()
     */
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return ps.getMetaData();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getMoreResults()
     */
    public boolean getMoreResults() throws SQLException
    {
        return ps.getMoreResults();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getMoreResults(int)
     */
    public boolean getMoreResults(int current) throws SQLException
    {
        return ps.getMoreResults(current);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#getParameterMetaData()
     */
    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        return ps.getParameterMetaData();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getQueryTimeout()
     */
    public int getQueryTimeout() throws SQLException
    {
        return ps.getQueryTimeout();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getResultSet()
     */
    public ResultSet getResultSet() throws SQLException
    {
        return ps.getResultSet();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getResultSetConcurrency()
     */
    public int getResultSetConcurrency() throws SQLException
    {
        return ps.getResultSetConcurrency();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getResultSetHoldability()
     */
    public int getResultSetHoldability() throws SQLException
    {
        return ps.getResultSetHoldability();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getResultSetType()
     */
    public int getResultSetType() throws SQLException
    {
        return ps.getResultSetType();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getUpdateCount()
     */
    public int getUpdateCount() throws SQLException
    {
        return ps.getUpdateCount();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#getWarnings()
     */
    public SQLWarning getWarnings() throws SQLException
    {
        return ps.getWarnings();
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
     */
    public void setArray(int i, Array x) throws SQLException
    {
        setParameter(i, x);
        ps.setArray(i, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream,
     * int)
     */
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        ps.setAsciiStream(parameterIndex, x, length);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
     */
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setBigDecimal(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream,
     * int)
     */
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        ps.setBinaryStream(parameterIndex, x, length);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
     */
    public void setBlob(int i, Blob x) throws SQLException
    {
        ps.setBlob(i, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setBoolean(int, boolean)
     */
    public void setBoolean(int parameterIndex, boolean x) throws SQLException
    {
        setParameter(parameterIndex, Boolean.valueOf(x));
        ps.setBoolean(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setByte(int, byte)
     */
    public void setByte(int parameterIndex, byte x) throws SQLException
    {
        setParameter(parameterIndex, Byte.valueOf(x));
        ps.setByte(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setBytes(int, byte[])
     */
    public void setBytes(int parameterIndex, byte[] x) throws SQLException
    {
        ps.setBytes(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader,
     * int)
     */
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
    {
        ps.setCharacterStream(parameterIndex, reader, length);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
     */
    public void setClob(int i, Clob x) throws SQLException
    {
        ps.setClob(i, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#setCursorName(java.lang.String)
     */
    public void setCursorName(String name) throws SQLException
    {
        ps.setCursorName(name);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setDate(int, java.sql.Date,
     * java.util.Calendar)
     */
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setDate(parameterIndex, x, cal);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
     */
    public void setDate(int parameterIndex, Date x) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setDate(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setDouble(int, double)
     */
    public void setDouble(int parameterIndex, double x) throws SQLException
    {
        setParameter(parameterIndex, Double.valueOf(x));
        ps.setDouble(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#setEscapeProcessing(boolean)
     */
    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        ps.setEscapeProcessing(enable);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#setFetchDirection(int)
     */
    public void setFetchDirection(int direction) throws SQLException
    {
        ps.setFetchDirection(direction);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#setFetchSize(int)
     */
    public void setFetchSize(int rows) throws SQLException
    {
        ps.setFetchSize(rows);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setFloat(int, float)
     */
    public void setFloat(int parameterIndex, float x) throws SQLException
    {
        setParameter(parameterIndex, Float.valueOf(x));
        ps.setFloat(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setInt(int, int)
     */
    public void setInt(int parameterIndex, int x) throws SQLException
    {
        setParameter(parameterIndex, Integer.valueOf(x));
        ps.setInt(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setLong(int, long)
     */
    public void setLong(int parameterIndex, long x) throws SQLException
    {
        setParameter(parameterIndex, Long.valueOf(x));
        ps.setLong(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#setMaxFieldSize(int)
     */
    public void setMaxFieldSize(int max) throws SQLException
    {
        ps.setMaxFieldSize(max);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#setMaxRows(int)
     */
    public void setMaxRows(int max) throws SQLException
    {
        ps.setMaxRows(max);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
     */
    public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException
    {
        setParameter(paramIndex, null);
        ps.setNull(paramIndex, sqlType, typeName);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setNull(int, int)
     */
    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        setParameter(parameterIndex, null);
        ps.setNull(parameterIndex, sqlType);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int,
     * int)
     */
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setObject(parameterIndex, x, targetSqlType, scale);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
     */
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setObject(parameterIndex, x, targetSqlType);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
     */
    public void setObject(int parameterIndex, Object x) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setObject(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.Statement#setQueryTimeout(int)
     */
    public void setQueryTimeout(int seconds) throws SQLException
    {
        ps.setQueryTimeout(seconds);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
     */
    public void setRef(int i, Ref x) throws SQLException
    {
        setParameter(i, x);
        ps.setRef(i, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setShort(int, short)
     */
    public void setShort(int parameterIndex, short x) throws SQLException
    {
        setParameter(parameterIndex, Short.valueOf(x));
        ps.setShort(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setString(int, java.lang.String)
     */
    public void setString(int parameterIndex, String x) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setString(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setTime(int, java.sql.Time,
     * java.util.Calendar)
     */
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setTime(parameterIndex, x, cal);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
     */
    public void setTime(int parameterIndex, Time x) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setTime(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp,
     * java.util.Calendar)
     */
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setTimestamp(parameterIndex, x, cal);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
     */
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setTimestamp(parameterIndex, x);
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setUnicodeStream(int,
     * java.io.InputStream, int)
     */
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new NucleusUserException("Not supported");
    }

    /*
     * (non-Javadoc)
     * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
     */
    public void setURL(int parameterIndex, URL x) throws SQLException
    {
        setParameter(parameterIndex, x);
        ps.setURL(parameterIndex, x);
    }

    // JDK 1.6 methods

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setNString(int parameterIndex, String value) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public boolean isClosed() throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isPoolable() throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    public void setPoolable(boolean poolable) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setNClob(int parameterIndex, java.sql.NClob value) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setRowId(int parameterIndex, java.sql.RowId x) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public void setSQLXML(int parameterIndex, java.sql.SQLXML xmlObject) throws SQLException
    {
        // TODO Auto-generated method stub
    }

    // Implementation of JDBC 4.0's Wrapper interface

    public boolean isWrapperFor(Class iface) throws SQLException
    {
        return PreparedStatement.class.equals(iface);
    }

    public Object unwrap(Class iface) throws SQLException
    {
        if (!PreparedStatement.class.equals(iface))
        {
            throw new SQLException("PreparedStatement of type [" + getClass().getName() +
                   "] can only be unwrapped as [java.sql.PreparedStatement], not as [" + iface.getName() + "]");
        }
        return this;
    }

    public void closeOnCompletion() throws SQLException
    {
//        ps.closeOnCompletion();
    }

    public boolean isCloseOnCompletion() throws SQLException
    {
//        return ps.isCloseOnCompletion();
        return false;
    }
}
