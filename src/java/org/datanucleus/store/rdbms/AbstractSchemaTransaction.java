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
package org.datanucleus.store.rdbms;

import java.sql.Connection;
import java.sql.SQLException;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.transaction.TransactionIsolation;
import org.datanucleus.transaction.TransactionUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * An abstract base class for RDBMSManager transactions that perform some schema operation on the database.
 * <p>
 * Management transactions may be retried in the face of SQL exceptions to work around failures caused by 
 * transient conditions, such as DB deadlocks.
 * </p>
 */
public abstract class AbstractSchemaTransaction
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_RDBMS = Localiser.getInstance(
        "org.datanucleus.store.rdbms.Localisation", RDBMSStoreManager.class.getClassLoader());

    protected RDBMSStoreManager rdbmsMgr;

    protected final int isolationLevel;
    protected final int maxRetries;
    protected ManagedConnection mconn;
    private Connection conn;

    /**
     * Constructs a new management transaction having the given isolation level.
     * @param rdbmsMgr RDBMSManager to use
     * @param isolationLevel One of the isolation level constants from java.sql.Connection.
     */
    public AbstractSchemaTransaction(RDBMSStoreManager rdbmsMgr, int isolationLevel)
    {
        this.rdbmsMgr = rdbmsMgr;
        this.isolationLevel = isolationLevel;
        maxRetries = rdbmsMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CLASS_ADDER_MAX_RETRIES);
    }

    /**
     * Returns a description of the management transaction. Subclasses should override this method so that 
     * transaction failures are given an appropriate exception message.
     * @return A description of the management transaction.
     */
    public abstract String toString();

    /**
     * Implements the body of the transaction.
     * @param clr the ClassLoaderResolver
     * @exception SQLException Thrown if the transaction fails due to a database error that should allow 
     * the entire transaction to be retried.
     */
    protected abstract void run(ClassLoaderResolver clr)
    throws SQLException;

    /**
     * Returns the current connection for the schema transaction. Creates one if needed
     * @return the connection
     */
    protected Connection getCurrentConnection() 
    throws SQLException 
    {
        if (conn == null)
        {
            mconn = rdbmsMgr.getConnection(isolationLevel);
            conn = (Connection) mconn.getConnection();
            if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER_RDBMS.msg("050057", StringUtils.toJVMIDString(conn),
                    TransactionUtils.getNameForTransactionIsolationLevel(isolationLevel)));
            }
        }
        return conn;
    }
    
    /**
     * Executes the schema transaction.
     * A database connection is acquired and the {@link #execute(ClassLoaderResolver)}method is invoked.
     * If the selected isolation level is not Connection.TRANSACTION_NONE, then commit() or rollback() is 
     * called on the connection according to whether the invocation succeeded or not. If the invocation 
     * failed the sequence is repeated, up to a maximum of <var>maxRetries </var> times, configurable by 
     * the persistence property "datanucleus.rdbms.classAdditionMaxRetries".
     * @param clr the ClassLoaderResolver
     * @exception NucleusDataStoreException If a SQL exception occurred even after "maxRetries" attempts.
     */
    public final void execute(ClassLoaderResolver clr)
    {
        int attempts = 0;
        for (;;)
        {
            try
            {
                try
                {
                    boolean succeeded = false;
                    try
                    {
                        run(clr);
                        succeeded = true;
                    }
                    finally
                    {
                        if (conn != null)
                        {
                            if (isolationLevel != TransactionIsolation.TRANSACTION_NONE)
                            {
                                if (!conn.getAutoCommit())
                                {
                                    if (succeeded)
                                    {
                                        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                                        {
                                            NucleusLogger.DATASTORE_SCHEMA.debug(
                                                LOCALISER_RDBMS.msg("050053", StringUtils.toJVMIDString(conn)));
                                        }
                                        conn.commit();
                                    }
                                    else
                                    {
                                        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                                        {
                                            NucleusLogger.DATASTORE_SCHEMA.debug(
                                                LOCALISER_RDBMS.msg("050054", StringUtils.toJVMIDString(conn)));
                                        }
                                        conn.rollback();
                                    }
                                }
                            }
                        }
                    }
                }
                finally
                {
                    if (conn != null)
                    {
                        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug(
                                LOCALISER_RDBMS.msg("050055", StringUtils.toJVMIDString(conn)));
                        }
                        mconn.release();

                        conn = null;
                    }
                }
                break;
            }
            catch (SQLException e)
            {
                if (++attempts >= maxRetries)
                {
                    throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("050056", this), e);
                }
            }
        }
    }
}