/**********************************************************************
Copyright (c) 2012 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.query;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.StoredProcQueryParameterMode;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.query.AbstractStoredProcedureQuery;
import org.datanucleus.store.query.NoQueryResultsException;
import org.datanucleus.store.query.QueryNotUniqueException;
import org.datanucleus.store.query.QueryResult;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.util.NucleusLogger;

/**
 * Query using a stored procedure.
 */
public class StoredProcedureQuery extends AbstractStoredProcedureQuery
{
    /** The callable statement used to execute the stored proc. */
    CallableStatement stmt;

    /**
     * @param storeMgr StoreManager
     * @param ec Execution Context
     * @param query Existing query to base this one
     */
    public StoredProcedureQuery(StoreManager storeMgr, ExecutionContext ec, StoredProcedureQuery query)
    {
        super(storeMgr, ec, query);
    }

    /**
     * @param storeMgr StoreManager
     * @param ec ExecutionContext
     */
    public StoredProcedureQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        super(storeMgr, ec, (String)null);
    }

    /**
     * Constructs a new query instance for the specified stored procedure name.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param procName Name of the stored procedure
     */
    public StoredProcedureQuery(StoreManager storeMgr, ExecutionContext ec, String procName)
    {
        super(storeMgr, ec, procName);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.Query#compileInternal(java.util.Map)
     */
    @Override
    protected void compileInternal(Map parameterValues)
    {
        DatastoreAdapter dba = ((RDBMSStoreManager)storeMgr).getDatastoreAdapter();
        if (!dba.supportsOption(DatastoreAdapter.STORED_PROCEDURES))
        {
            throw new NucleusUserException("This RDBMS does not support stored procedures!");
        }
        // Nothing to compile since we only have a procedure name (and params)
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.Query#processesRangeInDatastoreQuery()
     */
    @Override
    public boolean processesRangeInDatastoreQuery()
    {
        return true;
    }

    /**
     * Method to execute the actual query. Overrides the version in Query since that will handle result set processing
     * and assumes that this is a normal query, which it isn't.
     * @param parameters Map of parameter values keyed by parameter name
     * @return Boolean, which is true if there is a result set, and false if an update count.
     */
    protected Object executeQuery(final Map parameters)
    {
        this.inputParameters = new HashMap();
        if (implicitParameters != null)
        {
            inputParameters.putAll(implicitParameters);
        }
        if (parameters != null)
        {
            inputParameters.putAll(parameters);
        }

        // Make sure the datastore is prepared (new objects flushed as required)
        prepareDatastore();

        boolean failed = true; // flag to use for checking the state of the execution results
        long start = 0;
        if (ec.getStatistics() != null)
        {
            start = System.currentTimeMillis();
            ec.getStatistics().queryBegin();
        }

        try
        {
            // Execute the query
            return performExecute(inputParameters);
        }
        finally
        {
            if (ec.getStatistics() != null)
            {
                if (failed)
                {
                    ec.getStatistics().queryExecutedWithError();
                }
                else
                {
                    ec.getStatistics().queryExecuted(System.currentTimeMillis()-start);
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.Query#performExecute(java.util.Map)
     */
    @Override
    protected Object performExecute(Map parameters)
    {
        try
        {
            RDBMSStoreManager storeMgr = (RDBMSStoreManager)getStoreManager();
            ManagedConnection mconn = storeMgr.getConnection(ec);

            try
            {
                Connection conn = (Connection) mconn.getConnection();
                StringBuilder stmtStr = new StringBuilder("CALL " + procedureName);
                stmtStr.append("(");
                if (storedProcParams != null && !storedProcParams.isEmpty())
                {
                    Iterator<StoredProcedureParameter> paramIter = storedProcParams.iterator();
                    while (paramIter.hasNext())
                    {
                        paramIter.next();
                        stmtStr.append("?");
                        if (paramIter.hasNext())
                        {
                            stmtStr.append(",");
                        }
                    }
                }
                stmtStr.append(")");
                this.stmt = conn.prepareCall(stmtStr.toString());

                boolean hasOutputParams = false;
                if (storedProcParams != null && !storedProcParams.isEmpty())
                {
                    Iterator<StoredProcedureParameter> paramIter = storedProcParams.iterator();
                    while (paramIter.hasNext())
                    {
                        StoredProcedureParameter param = paramIter.next();
                        if (param.getMode() == StoredProcQueryParameterMode.IN ||
                            param.getMode() == StoredProcQueryParameterMode.INOUT)
                        {
                            if (param.getType() == Integer.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.setInt(param.getName(), (Integer) parameters.get(param.getName()));
                                }
                                else
                                {
                                    stmt.setInt(param.getPosition(), (Integer) parameters.get(param.getPosition()));
                                }
                            }
                            else if (param.getType() == Long.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.setLong(param.getName(), (Long) parameters.get(param.getName()));
                                }
                                else
                                {
                                    stmt.setLong(param.getPosition(), (Long) parameters.get(param.getPosition()));
                                }
                            }
                            else if (param.getType() == Short.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.setShort(param.getName(), (Short) parameters.get(param.getName()));
                                }
                                else
                                {
                                    stmt.setShort(param.getPosition(), (Short) parameters.get(param.getPosition()));
                                }
                            }
                            else if (param.getType() == Double.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.setDouble(param.getName(), (Double) parameters.get(param.getName()));
                                }
                                else
                                {
                                    stmt.setDouble(param.getPosition(), (Double) parameters.get(param.getPosition()));
                                }
                            }
                            else if (param.getType() == Float.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.setFloat(param.getName(), (Float) parameters.get(param.getName()));
                                }
                                else
                                {
                                    stmt.setFloat(param.getPosition(), (Float) parameters.get(param.getPosition()));
                                }
                            }
                            else if (param.getType() == Boolean.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.setBoolean(param.getName(), (Boolean) parameters.get(param.getName()));
                                }
                                else
                                {
                                    stmt.setBoolean(param.getPosition(), (Boolean) parameters.get(param.getPosition()));
                                }
                            }
                            else if (param.getType() == String.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.setString(param.getName(), (String) parameters.get(param.getName()));
                                }
                                else
                                {
                                    stmt.setString(param.getPosition(), (String) parameters.get(param.getPosition()));
                                }
                            }
                            else if (param.getType() == Date.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.setDate(param.getName(), (java.sql.Date) parameters.get(param.getName()));
                                }
                                else
                                {
                                    stmt.setDate(param.getPosition(), (java.sql.Date) parameters.get(param.getPosition()));
                                }
                            }
                            else if (param.getType() == BigInteger.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.setLong(param.getName(), ((BigInteger) parameters.get(param.getName())).longValue());
                                }
                                else
                                {
                                    stmt.setLong(param.getPosition(), ((BigInteger) parameters.get(param.getPosition())).longValue());
                                }
                            }
                            else if (param.getType() == BigDecimal.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.setDouble(param.getName(), ((BigDecimal) parameters.get(param.getName())).doubleValue());
                                }
                                else
                                {
                                    stmt.setDouble(param.getPosition(), ((BigDecimal) parameters.get(param.getPosition())).doubleValue());
                                }
                            }
                            else
                            {
                                throw new NucleusException("Dont currently support stored proc input params of type " + param.getType());
                            }
                        }

                        if (param.getMode() == StoredProcQueryParameterMode.OUT || 
                            param.getMode() == StoredProcQueryParameterMode.INOUT)
                        {
                            // Register output params
                            if (param.getType() == Integer.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.registerOutParameter(param.getName(), Types.INTEGER);
                                }
                                else
                                {
                                    stmt.registerOutParameter(param.getPosition(), Types.INTEGER);
                                }
                            }
                            else if (param.getType() == Long.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.registerOutParameter(param.getName(), Types.INTEGER);
                                }
                                else
                                {
                                    stmt.registerOutParameter(param.getPosition(), Types.INTEGER);
                                }
                            }
                            else if (param.getType() == Short.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.registerOutParameter(param.getName(), Types.INTEGER);
                                }
                                else
                                {
                                    stmt.registerOutParameter(param.getPosition(), Types.INTEGER);
                                }
                            }
                            else if (param.getType() == Double.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.registerOutParameter(param.getName(), Types.DOUBLE);
                                }
                                else
                                {
                                    stmt.registerOutParameter(param.getPosition(), Types.DOUBLE);
                                }
                            }
                            else if (param.getType() == Float.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.registerOutParameter(param.getName(), Types.FLOAT);
                                }
                                else
                                {
                                    stmt.registerOutParameter(param.getPosition(), Types.FLOAT);
                                }
                            }
                            else if (param.getType() == Boolean.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.registerOutParameter(param.getName(), Types.BOOLEAN);
                                }
                                else
                                {
                                    stmt.registerOutParameter(param.getPosition(), Types.BOOLEAN);
                                }
                            }
                            else if (param.getType() == String.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.registerOutParameter(param.getName(), Types.VARCHAR);
                                }
                                else
                                {
                                    stmt.registerOutParameter(param.getPosition(), Types.VARCHAR);
                                }
                            }
                            else if (param.getType() == Date.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.registerOutParameter(param.getName(), Types.DATE);
                                }
                                else
                                {
                                    stmt.registerOutParameter(param.getPosition(), Types.DATE);
                                }
                            }
                            else if (param.getType() == BigInteger.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.registerOutParameter(param.getName(), Types.BIGINT);
                                }
                                else
                                {
                                    stmt.registerOutParameter(param.getPosition(), Types.BIGINT);
                                }
                            }
                            else if (param.getType() == BigDecimal.class)
                            {
                                if (param.getName() != null)
                                {
                                    stmt.registerOutParameter(param.getName(), Types.DOUBLE);
                                }
                                else
                                {
                                    stmt.registerOutParameter(param.getPosition(), Types.DOUBLE);
                                }
                            }
                            else
                            {
                                throw new NucleusException("Dont currently support stored proc output params of type " + param.getType());
                            }
                            hasOutputParams = true;
                        }
                    }
                }

                // TODO Provide ParamLoggingCallableStatement and use here
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug(stmtStr.toString());
                }

                boolean hasResultSet = stmt.execute();

                // For any output parameters, get their values
                if (hasOutputParams)
                {
                    Iterator<StoredProcedureParameter> paramIter = storedProcParams.iterator();
                    while (paramIter.hasNext())
                    {
                        StoredProcedureParameter param = paramIter.next();
                        if (param.getMode() == StoredProcQueryParameterMode.OUT || 
                            param.getMode() == StoredProcQueryParameterMode.INOUT)
                        {
                            Object value = null;
                            if (param.getType() == Integer.class)
                            {
                                if (param.getName() != null)
                                {
                                    value = stmt.getInt(param.getName());
                                }
                                else
                                {
                                    value = stmt.getInt(param.getPosition());
                                }
                            }
                            else if (param.getType() == Long.class)
                            {
                                if (param.getName() != null)
                                {
                                    value = stmt.getLong(param.getName());
                                }
                                else
                                {
                                    value = stmt.getLong(param.getPosition());
                                }
                            }
                            else if (param.getType() == Short.class)
                            {
                                if (param.getName() != null)
                                {
                                    value = stmt.getShort(param.getName());
                                }
                                else
                                {
                                    value = stmt.getShort(param.getPosition());
                                }
                            }
                            else if (param.getType() == Double.class)
                            {
                                if (param.getName() != null)
                                {
                                    value = stmt.getDouble(param.getName());
                                }
                                else
                                {
                                    value = stmt.getDouble(param.getPosition());
                                }
                            }
                            else if (param.getType() == Float.class)
                            {
                                if (param.getName() != null)
                                {
                                    value = stmt.getFloat(param.getName());
                                }
                                else
                                {
                                    value = stmt.getFloat(param.getPosition());
                                }
                            }
                            else if (param.getType() == Boolean.class)
                            {
                                if (param.getName() != null)
                                {
                                    value = stmt.getBoolean(param.getName());
                                }
                                else
                                {
                                    value = stmt.getBoolean(param.getPosition());
                                }
                            }
                            else if (param.getType() == String.class)
                            {
                                if (param.getName() != null)
                                {
                                    value = stmt.getString(param.getName());
                                }
                                else
                                {
                                    value = stmt.getString(param.getPosition());
                                }
                            }
                            else if (param.getType() == Date.class)
                            {
                                if (param.getName() != null)
                                {
                                    value = stmt.getDate(param.getName());
                                }
                                else
                                {
                                    value = stmt.getDate(param.getPosition());
                                }
                            }
                            else if (param.getType() == BigInteger.class)
                            {
                                if (param.getName() != null)
                                {
                                    value = stmt.getLong(param.getName());
                                }
                                else
                                {
                                    value = stmt.getLong(param.getPosition());
                                }
                            }
                            else if (param.getType() == BigDecimal.class)
                            {
                                if (param.getName() != null)
                                {
                                    value = stmt.getDouble(param.getName());
                                }
                                else
                                {
                                    value = stmt.getDouble(param.getPosition());
                                }
                            }
                            else
                            {
                                throw new NucleusUserException("Dont currently support output parameters of type=" + param.getType());
                            }

                            if (outputParamValues == null)
                            {
                                outputParamValues = new HashMap();
                            }
                            if (param.getName() != null)
                            {
                                outputParamValues.put(param.getName(), value);
                            }
                            else
                            {
                                outputParamValues.put(param.getPosition(), value);
                            }
                        }
                    }
                }

                return hasResultSet;
            }
            finally
            {
                mconn.release();
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER.msg("059027", procedureName), e);
        }
    }

    @Override
    public boolean hasMoreResults()
    {
        if (stmt == null)
        {
            throw new NucleusUserException("Cannot check for more results until the stored procedure has been executed");
        }
        try
        {
            return stmt.getMoreResults();
        }
        catch (SQLException sqle)
        {
            // Some JDBC drivers close the statement when completed
            return false;
        }
    }

    @Override
    public int getUpdateCount()
    {
        if (stmt == null)
        {
            throw new NucleusUserException("Cannot check for update count until the stored procedure has been executed");
        }

        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            resultSetNumber++;
            return stmt.getUpdateCount();
        }
        catch (SQLException sqle)
        {
            throw new NucleusDataStoreException("Exception from CallableStatement.getUpdateCount", sqle);
        }
        finally
        {
            mconn.release();
        }
    }

    @Override
    public Object getNextResults()
    {
        if (stmt == null)
        {
            throw new NucleusUserException("Cannot check for more results until the stored procedure has been executed");
        }

        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            resultSetNumber++;
            ResultSet rs = stmt.getResultSet();
            QueryResult qr = getResultsForResultSet((RDBMSStoreManager)storeMgr, rs, mconn);
            if (shouldReturnSingleRow())
            {
                // Single row only needed so just take first row
                try
                {
                    if (qr == null || qr.size() == 0)
                    {
                        throw new NoQueryResultsException("No query results were returned");
                    }
                    else
                    {
                        Iterator qrIter = qr.iterator();
                        Object firstRow = qrIter.next();
                        if (qrIter.hasNext())
                        {
                            throw new QueryNotUniqueException();
                        }
                        return firstRow;
                    }
                }
                finally
                {
                    // can close results right now because we don't return it
                    if (qr != null)
                    {
                        close(qr);
                    }
                }
            }
            else
            {
                // Apply range?
            }
            return qr;
        }
        catch (SQLException sqle)
        {
            throw new NucleusDataStoreException("Exception from CallableStatement.getResultSet", sqle);
        }
        finally
        {
            mconn.release();
        }
    }

    protected QueryResult getResultsForResultSet(RDBMSStoreManager storeMgr, ResultSet rs, ManagedConnection mconn)
    throws SQLException
    {
        ResultObjectFactory rof = null;
        if (resultMetaDatas != null)
        {
            // Each row of the ResultSet is defined by MetaData
            rof = new ResultMetaDataROF(storeMgr, resultMetaDatas[resultSetNumber]);
        }
        else
        {
            // Each row of the ResultSet is either an instance of resultClass, or Object[]
            rof = RDBMSQueryUtils.getResultObjectFactoryForNoCandidateClass(storeMgr, rs, 
                resultClasses != null ? resultClasses[resultSetNumber] : null);
        }

        // Create the required type of QueryResult
        String resultSetType = RDBMSQueryUtils.getResultSetTypeForQuery(this);
        AbstractRDBMSQueryResult qr = null;
        if (resultSetType.equals("scroll-insensitive") || resultSetType.equals("scroll-sensitive"))
        {
            qr = new ScrollableQueryResult(this, rof, rs, null);
        }
        else
        {
            qr = new ForwardQueryResult(this, rof, rs, null);
        }
        qr.initialise();

        final QueryResult qr1 = qr;
        final ManagedConnection mconn1 = mconn;
        ManagedConnectionResourceListener listener = new ManagedConnectionResourceListener()
        {
            public void transactionFlushed(){}
            public void transactionPreClose()
            {
                // Tx : disconnect query from ManagedConnection (read in unread rows etc)
                qr1.disconnect();
            }
            public void managedConnectionPreClose()
            {
                if (!ec.getTransaction().isActive())
                {
                    // Non-Tx : disconnect query from ManagedConnection (read in unread rows etc)
                    qr1.disconnect();
                }
            }
            public void managedConnectionPostClose(){}
            public void resourcePostClose()
            {
                mconn1.removeListener(this);
            }
        };
        mconn.addListener(listener);
        qr.addConnectionListener(listener);

        return qr;
    }
}