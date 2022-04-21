/**********************************************************************
Copyright (c) 2006 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms;

import static java.util.Collections.emptyList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.store.rdbms.query.RDBMSQueryUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Controller for execution of SQL statements to the underlying RDBMS datastore.
 * Currently provides access to PreparedStatements and Connections.
 * <p>
 * Separates statements into 2 categories.
 * <ul>
 * <li><b>Updates</b> : INSERT/UPDATE/DELETE statements that can potentially (depending on the JDBC driver capabilities) 
 * be batched and hence give much speed benefit.</li>
 * <li><b>Queries</b> : SELECT statements that can provide flexibility in the capabilities of ResultSet.</li>
 * </ul>
 * 
 * <p>
 * An "update" statement can be batched. When getting the PreparedStatement the calling method passes in an argument
 * to signify if it can be batched (whether it will use the statement before any other statement can execute), and if so
 * will be cached against the Connection in use. When the statement is to be processed by the calling method it would
 * pass in the "processNow" argument as false. This will leave the statement in the cache as "ready-to-be-processed"
 * in case anything else comes in in the meantime. If the next statement to be requested has the same text and can also be
 * batched then it will be returned the current statement so that it can populate it with its values. When the statement
 * is ready for processing since it is in the cache the new values are added to the batch. Again the statement could be
 * executed at that point or could continue batching.
 *
 * <p>
 * Typical (unbatched) invocation would be as follows :-
 * <pre>
 * SQLController sql = rdbmsMgr.getSQLController();
 * PreparedStatement ps = sql.getStatementForUpdate(conn, myStmt, false); // No batching
 * sql.executeStatementUpdate(myStmt, ps, true); // Execute now, dont wait
 * sql.closeStatement(ps);
 * </pre>
 *
 * <p>
 * Typical (batched) invocation would be as follows :-
 * <pre>
 * SQLController sql = rdbmsMgr.getSQLController();
 * PreparedStatement ps = sql.getStatementForUpdate(conn, myStmt, true); // Batching
 * sql.executeStatementUpdate(myStmt, ps, false); // Execute later
 * sql.closeStatement(ps);
 * ...
 * PreparedStatement ps = sql.getStatementForUpdate(conn, myStmt, true); // Batching (same statement as first)
 * sql.executeStatementUpdate(myStmt, ps, false); // Execute later
 * sql.closeStatement(ps);
 * </pre>
 * 
 * <p>
 * Things to note :-
 * <ul>
 * <li>Batching only takes place if the underlying datastore supports it, and if the maximum batch size is not reached.</li>
 * <li>When ExecutionContext.flush() is called the RDBMSManager will call "processStatementsForConnection"
 * so that any batched statement is pushed to the datastore.</li>
 * </ul>
 */
public class SQLController
{
    /** Whether batching is supported for this controller (datastore). */
    protected boolean supportsBatching = false;

    /** Maximum batch size (-1 implies no limit). */
    protected int maxBatchSize = -1;

    /** Timeout to apply to queries (where required) in milliseconds. */
    protected int queryTimeout = 0;

    /** Statement logging type. */
    protected StatementLoggingType stmtLogType = StatementLoggingType.JDBC;

    /**
     * State of a connection.
     * Maintains an update statement in "wait" state. Stores the statement, the text,
     * the current batch level on this statement, and whether it is processable right now.
     */
    static class ConnectionStatementState
    {
        /** The current update PreparedStatement. */
        PreparedStatement stmt = null;

        /** The text for the statement. */
        String stmtText = null;

        /** Number of statements currently batched (1 or more means that this is batchable). */
        int batchSize = 0;

        /** Whether this statement has been allocated only, or whether it is processable now. */
        boolean processable = false;

        /** Whether to close the statement on processing */
        boolean closeStatementOnProcess = false;

        public String toString()
        {
            return "StmtState : stmt=" + StringUtils.toJVMIDString(stmt) + " sql=" + stmtText + " batch=" + batchSize + " closeOnProcess=" + closeStatementOnProcess;
        }
    }

    public enum StatementLoggingType
    {
        JDBC,
        PARAMS_INLINE,
        PARAMS_IN_BRACKETS
    }

    /** Map of the ConnectionStatementState keyed by the Connection */
    Map<ManagedConnection, ConnectionStatementState> connectionStatements = new ConcurrentHashMap<>();

    /**
     * Constructor.
     * @param supportsBatching Whether batching is to be supported.
     * @param maxBatchSize The maximum batch size
     * @param queryTimeout Timeout for queries (ms)
     * @param stmtLoggingType Setting for statement logging
     */
    public SQLController(boolean supportsBatching, int maxBatchSize, int queryTimeout, StatementLoggingType stmtLoggingType)
    {
        this.supportsBatching = supportsBatching;
        this.maxBatchSize = maxBatchSize;
        this.queryTimeout = queryTimeout;
        if (maxBatchSize == 0)
        {
            // User doesn't want batches so lets just say it isnt supported.
            this.supportsBatching = false;
        }

        this.stmtLogType = stmtLoggingType;
    }

    /**
     * Convenience method to create a new PreparedStatement for an update.
     * @param conn The Connection to use for the statement
     * @param stmtText Statement text
     * @param batchable Whether this statement is batchable. Whether we will process the statement before any other statement
     * @return The PreparedStatement
     * @throws SQLException thrown if an error occurs creating the statement
     */
    public PreparedStatement getStatementForUpdate(ManagedConnection conn, String stmtText, boolean batchable)
    throws SQLException
    {
        return getStatementForUpdate(conn, stmtText, batchable, false, emptyList());
    }

    /**
     * Convenience method to create a new PreparedStatement for an update.
     * @param conn The Connection to use for the statement
     * @param stmtText Statement text
     * @param batchable Whether this statement is batchable. Whether we will process the statement before any other statement
     * @param getGeneratedKeysFlag whether to request getGeneratedKeys for this statement
     * @param pkColumnNames list of auto-generated primary key names to return from an insert statement. May be empty.
     * @return The PreparedStatement
     * @throws SQLException thrown if an error occurs creating the statement
     */
    @SuppressWarnings("resource")
    public PreparedStatement getStatementForUpdate(ManagedConnection conn, String stmtText, boolean batchable, boolean getGeneratedKeysFlag, List<String> pkColumnNames)
    throws SQLException
    {
        Connection c = (Connection) conn.getConnection();
        if (supportsBatching)
        {
            ConnectionStatementState state = getConnectionStatementState(conn);
            if (state != null)
            {
                if (state.processable)
                {
                    // We have a batchable statement in the queue that could be processed now if necessary
                    if (!batchable)
                    {
                        // This new statement isnt batchable so process the existing one before returning our new statement
                        processConnectionStatement(conn);
                    }
                    else
                    {
                        // Check if we could batch onto this existing statement
                        if (state.stmtText.equals(stmtText))
                        {
                            // We can batch onto this statement
                            if (maxBatchSize == -1 || state.batchSize < maxBatchSize)
                            {
                                state.batchSize++;
                                state.processable = false; // Have to wait til we process this part til processable again
                                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                                {
                                    NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("052100", stmtText, "" + state.batchSize));
                                }
                                return state.stmt;
                            }

                            // Reached max batch size so process it now and start again for this one
                            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                            {
                                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("052101", state.stmtText));
                            }
                            processConnectionStatement(conn);
                        }
                        else
                        {
                            // We cant batch using the current batch statement so process it first and return our new one
                            processConnectionStatement(conn);
                        }
                    }
                }
                else
                {
                    if (batchable)
                    {
                        // The current statement is being batched so we cant batch this since cant process the current statement now
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("052102", state.stmtText, stmtText));
                        }
                        batchable = false;
                    }
                }
            }
        }

        PreparedStatement ps = getGeneratedKeysFlag ?
            pkColumnNames.isEmpty() ?
                c.prepareStatement(stmtText, Statement.RETURN_GENERATED_KEYS) : c.prepareStatement(stmtText, pkColumnNames.toArray(new String[0])) :
            c.prepareStatement(stmtText);
        ps.clearBatch(); // In case using statement caching and given one with batched statements left hanging (C3P0)
        if (stmtLogType != StatementLoggingType.JDBC)
        {
            // Wrap with our parameter logger
            ps = new ParamLoggingPreparedStatement(ps, stmtText);
            ((ParamLoggingPreparedStatement)ps).setParamsInAngleBrackets(stmtLogType == StatementLoggingType.PARAMS_IN_BRACKETS);
        }
        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE.debug(Localiser.msg("052109", ps, StringUtils.toJVMIDString(c)));
        }

        if (batchable && supportsBatching)
        {
            // This statement is batchable so save it since we support batching
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("052103", stmtText));
            }
            ConnectionStatementState state = new ConnectionStatementState();
            state.stmt = ps;
            state.stmtText = stmtText;
            state.batchSize = 1;
            setConnectionStatementState(conn, state);
        }

        return ps;
    }

    /**
     * Convenience method to create a new PreparedStatement for a query.
     * @param conn The Connection to use for the statement
     * @param stmtText Statement text
     * @return The PreparedStatement
     * @throws SQLException thrown if an error occurs creating the statement
     */
    public PreparedStatement getStatementForQuery(ManagedConnection conn, String stmtText)
    throws SQLException
    {
        return getStatementForQuery(conn, stmtText, null, null);
    }

    /**
     * Convenience method to create a new PreparedStatement for a query.
     * @param conn The Connection to use for the statement
     * @param stmtText Statement text
     * @param resultSetType Type of result set
     * @param resultSetConcurrency Concurrency for the result set
     * @return The PreparedStatement
     * @throws SQLException thrown if an error occurs creating the statement
     */
    public PreparedStatement getStatementForQuery(ManagedConnection conn, String stmtText, String resultSetType, String resultSetConcurrency)
    throws SQLException
    {
        Connection c = (Connection) conn.getConnection();
        if (supportsBatching)
        {
            // Check for a waiting batched statement that is ready for processing
            ConnectionStatementState state = getConnectionStatementState(conn);
            if (state != null && state.processable)
            {
                // Process the batch statement before returning our new query statement
                processConnectionStatement(conn);
            }
        }

        // Create a new PreparedStatement for this query
        PreparedStatement ps = null;
        if (resultSetType != null || resultSetConcurrency != null)
        {
            int rsTypeValue = ResultSet.TYPE_FORWARD_ONLY;
            if (resultSetType != null)
            {
                if (resultSetType.equals(RDBMSQueryUtils.QUERY_RESULTSET_TYPE_SCROLL_SENSITIVE))
                {
                    rsTypeValue = ResultSet.TYPE_SCROLL_SENSITIVE;
                }
                else if (resultSetType.equals(RDBMSQueryUtils.QUERY_RESULTSET_TYPE_SCROLL_INSENSITIVE))
                {
                    rsTypeValue = ResultSet.TYPE_SCROLL_INSENSITIVE;
                }
            }
            
            int rsConcurrencyValue = ResultSet.CONCUR_READ_ONLY;
            if (resultSetConcurrency != null && resultSetConcurrency.equals(RDBMSQueryUtils.QUERY_RESULTSET_CONCURRENCY_UPDATEABLE))
            {
                rsConcurrencyValue = ResultSet.CONCUR_UPDATABLE;
            }
            ps = c.prepareStatement(stmtText, rsTypeValue, rsConcurrencyValue);
            ps.clearBatch(); // In case using statement caching and given one with batched statements left hanging (C3P0)
        }
        else
        {
            ps = c.prepareStatement(stmtText);
            ps.clearBatch(); // In case using statement caching and given one with batched statements left hanging (C3P0)
        }

        if (queryTimeout > 0)
        {
            // Apply any query timeout
            ps.setQueryTimeout(queryTimeout/1000); // queryTimeout is in milliseconds
        }

        if (stmtLogType != StatementLoggingType.JDBC)
        {
            // Wrap with our parameter logger
            ps = new ParamLoggingPreparedStatement(ps, stmtText);
            ((ParamLoggingPreparedStatement)ps).setParamsInAngleBrackets(stmtLogType == StatementLoggingType.PARAMS_IN_BRACKETS);
        }
        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE.debug(Localiser.msg("052109", ps, StringUtils.toJVMIDString(c)));
        }

        return ps;
    }

    /**
     * Method to execute a PreparedStatement update.
     * Prints logging information about timings.
     * @param ec ExecutionContext
     * @param conn The connection (required since the one on PreparedStatement is not always the same so we cant use it)
     * @param stmt The statement text
     * @param ps The Prepared Statement
     * @param processNow Whether to process this statement now (only applies if is batched)
     * @return The number of rows affected (as per PreparedStatement.executeUpdate)
     * @throws SQLException Thrown if an error occurs
     */
    public int[] executeStatementUpdate(ExecutionContext ec, ManagedConnection conn, String stmt, PreparedStatement ps, boolean processNow)
    throws SQLException
    {
        ConnectionStatementState state = getConnectionStatementState(conn);
        if (state != null)
        {
            if (state.stmt == ps)
            {
                // Mark as processable
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("052104", state.stmtText, "" + state.batchSize));
                }
                state.processable = true;
                state.stmt.addBatch();

                if (processNow)
                {
                    // Process the batch now
                    state.closeStatementOnProcess = false; // user method has requested execution so they can close it themselves now
                    return processConnectionStatement(conn);
                }

                // Leave processing til later
                return null;
            }

            // There is a waiting batch yet it is a different statement, so process that one now since we need
            // our statement executing
            processConnectionStatement(conn);
        }

        // Process the normal update statement
        long startTime = System.currentTimeMillis();
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            if (ps instanceof ParamLoggingPreparedStatement)
            {
                NucleusLogger.DATASTORE_NATIVE.debug(((ParamLoggingPreparedStatement)ps).getStatementWithParamsReplaced());
            }
            else
            {
                NucleusLogger.DATASTORE_NATIVE.debug(stmt);
            }
        }

        int ind = ps.executeUpdate();
        if (ec != null && ec.getStatistics() != null)
        {
            // Add to statistics
            ec.getStatistics().incrementNumWrites();
        }

        ps.clearBatch();
        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("045001", "" + (System.currentTimeMillis() - startTime), "" + ind, StringUtils.toJVMIDString(ps)));
        }

        return new int[] {ind};
    }

    /**
     * Method to execute a PreparedStatement (using PreparedStatement.execute()).
     * Prints logging information about timings.
     * @param ec Execution Context
     * @param conn The connection (required since the one on PreparedStatement is not always the same so we can't use it)
     * @param stmt The statement text
     * @param ps The Prepared Statement
     * @return The number of rows affected (as per PreparedStatement.execute)
     * @throws SQLException Thrown if an error occurs
     */
    public boolean executeStatement(ExecutionContext ec, ManagedConnection conn, String stmt, PreparedStatement ps)
    throws SQLException
    {
        if (supportsBatching)
        {
            // Check for a waiting batched statement that is ready for processing
            ConnectionStatementState state = getConnectionStatementState(conn);
            if (state != null && state.processable)
            {
                // Process the batch statement before returning our new query statement
                processConnectionStatement(conn);
            }
        }

        // Process the normal execute statement
        long startTime = System.currentTimeMillis();
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            if (ps instanceof ParamLoggingPreparedStatement)
            {
                NucleusLogger.DATASTORE_NATIVE.debug(((ParamLoggingPreparedStatement)ps).getStatementWithParamsReplaced());
            }
            else
            {
                NucleusLogger.DATASTORE_NATIVE.debug(stmt);
            }
        }

        boolean flag = ps.execute();
        if (ec != null && ec.getStatistics() != null)
        {
            // Add to statistics
            ec.getStatistics().incrementNumWrites();
        }

        ps.clearBatch();
        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("045002", "" + (System.currentTimeMillis() - startTime), StringUtils.toJVMIDString(ps)));
        }

        return flag;
    }

    /**
     * Method to execute a PreparedStatement query, and return the ResultSet.
     * Prints logging information about timings.
     * @param ec ExecutionContext
     * @param conn The connection (required since the one on PreparedStatement is not always the same so we can't use it)
     * @param stmt The statement text
     * @param ps The Prepared Statement
     * @return The ResultSet from the query
     * @throws SQLException Thrown if an error occurs
     */
    public ResultSet executeStatementQuery(ExecutionContext ec, ManagedConnection conn, String stmt, PreparedStatement ps)
    throws SQLException
    {
        if (supportsBatching)
        {
            ConnectionStatementState state = getConnectionStatementState(conn);
            if (state != null)
            {
                if (state.processable)
                {
                    // Current batch statement is processable now so lets just process it before processing our query
                    processConnectionStatement(conn);
                }
                else
                {
                    // Current wait statement is not processable now so leave it in wait state
                    if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("052106", state.stmtText, stmt));
                    }
                }
            }
        }

        // Execute this query
        long startTime = System.currentTimeMillis();
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            if (ps instanceof ParamLoggingPreparedStatement)
            {
                NucleusLogger.DATASTORE_NATIVE.debug(((ParamLoggingPreparedStatement)ps).getStatementWithParamsReplaced());
            }
            else
            {
                NucleusLogger.DATASTORE_NATIVE.debug(stmt);
            }
        }

        ResultSet rs = ps.executeQuery();
        if (ec != null && ec.getStatistics() != null)
        {
            // Add to statistics
            ec.getStatistics().incrementNumReads();
        }

        ps.clearBatch();
        if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("045000", System.currentTimeMillis() - startTime));
        }

        return rs;
    }

    /**
     * Method to call to remove the current batched statement for this connection and close it
     * due to an error preventing continuation.
     * @param conn The connection
     * @param ps The statement
     */
    public void abortStatementForConnection(ManagedConnection conn, PreparedStatement ps)
    {
        ConnectionStatementState state = getConnectionStatementState(conn);
        if (state != null && state.stmt == ps)
        {
            try
            {
                removeConnectionStatementState(conn);
                ps.close();
            }
            catch (SQLException sqe)
            {
                // Do nothing
            }
        }
    }

    /**
     * Convenience method to close a PreparedStatement.
     * If the statement is currently being used as a batch, will register it for closing when executing the batch
     * @param conn The Connection
     * @param ps The PreparedStatement
     * @throws SQLException if an error occurs closing the statement
     */
    public void closeStatement(ManagedConnection conn, PreparedStatement ps)
    throws SQLException
    {
        ConnectionStatementState state = getConnectionStatementState(conn);
        if (state != null && state.stmt == ps)
        {
            // Statement to be closed is the current batch, so register it for closing when it gets processed
            state.closeStatementOnProcess = true;
        }
        else
        {
            try
            {
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(Localiser.msg("052110", StringUtils.toJVMIDString(ps)));
                }
                ps.close();
            }
            catch (SQLException sqle)
            {
                // workaround for DBCP bug: even though PreparedStatement.close() is defined as having no effect if already closed, DBCP will throw SQLException
                if (!sqle.getMessage().equals("Already closed"))
                {
                    throw sqle;
                }
            }
        }
    }

    /**
     * Convenience method to process any batched statement for the specified connection.
     * Typically called when flush() or commit() are called.
     * @param conn The connection
     * @throws SQLException Thrown if an error occurs on processing of the batch
     */
    public void processStatementsForConnection(ManagedConnection conn)
    throws SQLException
    {
        if (!supportsBatching || getConnectionStatementState(conn) == null)
        {
            return;
        }
        processConnectionStatement(conn);
    }

    /**
     * Convenience method to process the currently waiting statement for the passed Connection.
     * Only processes the statement if it is in processable state.
     * @param conn The connection
     * @return The return codes from the statement batch
     * @throws SQLException if an error occurs processing the batch
     */
    protected int[] processConnectionStatement(ManagedConnection conn)
    throws SQLException
    {
        ConnectionStatementState state = getConnectionStatementState(conn);
        if (state == null || !state.processable)
        {
            return null;
        }

        long startTime = System.currentTimeMillis();
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            if (state.stmt instanceof ParamLoggingPreparedStatement)
            {
                NucleusLogger.DATASTORE_NATIVE.debug(((ParamLoggingPreparedStatement)state.stmt).getStatementWithParamsReplaced());
            }
            else
            {
                NucleusLogger.DATASTORE_NATIVE.debug(state.stmtText);
            }
        }

        int[] ind = state.stmt.executeBatch();
        state.stmt.clearBatch();

        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE.debug(Localiser.msg("045001",""+(System.currentTimeMillis() - startTime), StringUtils.intArrayToString(ind), StringUtils.toJVMIDString(state.stmt)));
        }

        // Remove the current connection statement
        removeConnectionStatementState(conn);

        // Close the statement if it is registered for closing after processing
        if (state.closeStatementOnProcess)
        {
            state.stmt.close();
        }

        return ind;
    }

    /**
     * Convenience method to remove the state for this connection.
     * This is typically called when a Connection is closed.
     * @param conn The Connection
     */
    protected void removeConnectionStatementState(ManagedConnection conn)
    {
        connectionStatements.remove(conn);
    }

    /**
     * Convenience method to get the state for this connection.
     * @param conn The Connection
     * @return The state (if any)
     */
    protected ConnectionStatementState getConnectionStatementState(ManagedConnection conn)
    {
        return connectionStatements.get(conn);
    }

    /**
     * Convenience method to set the state for this connection.
     * @param conn The Connection
     * @param state The state
     */
    protected void setConnectionStatementState(final ManagedConnection conn, ConnectionStatementState state)
    {
        connectionStatements.put(conn, state);
        conn.addListener(new ManagedConnectionResourceListener()
        {
            public void transactionFlushed()
            {
                try
                {
                    processStatementsForConnection(conn);
                }
                catch (SQLException e)
                {
                    // cleanup state
                    ConnectionStatementState state = getConnectionStatementState(conn);
                    if (state != null)
                    {
                        // Remove the current connection statement
                        removeConnectionStatementState(conn);
    
                        // Close the statement if it is registered for closing after processing
                        if (state.closeStatementOnProcess)
                        {
                            try
                            {
                                state.stmt.close();
                            }
                            catch (SQLException ex)
                            {
                                //ignore
                            }
                        }
                    }

                    throw new NucleusDataStoreException(Localiser.msg("052108"), e);
                }
            }
            public void transactionPreClose(){}
            public void managedConnectionPreClose(){}
            public void managedConnectionPostClose(){}
            public void resourcePostClose(){}
        });
    }
}