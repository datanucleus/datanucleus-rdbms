/**********************************************************************
Copyright (c) 2007 Erik Bengtson and others. All rights reserved.
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

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.HashMap;
import java.util.Map;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.ConnectionFactoryNotFoundException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.exceptions.UnsupportedConnectionFactoryException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractEmulatedXAResource;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.connectionpool.BoneCPConnectionPoolFactory;
import org.datanucleus.store.rdbms.connectionpool.C3P0ConnectionPoolFactory;
import org.datanucleus.store.rdbms.connectionpool.ConnectionPool;
import org.datanucleus.store.rdbms.connectionpool.ConnectionPoolFactory;
import org.datanucleus.store.rdbms.connectionpool.DBCP2BuiltinConnectionPoolFactory;
import org.datanucleus.store.rdbms.connectionpool.DBCP2ConnectionPoolFactory;
import org.datanucleus.store.rdbms.connectionpool.DefaultConnectionPoolFactory;
import org.datanucleus.store.rdbms.connectionpool.HikariCPConnectionPoolFactory;
import org.datanucleus.store.rdbms.connectionpool.TomcatConnectionPoolFactory;
import org.datanucleus.transaction.Transaction;
import org.datanucleus.transaction.TransactionIsolation;
import org.datanucleus.transaction.TransactionUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * ConnectionFactory for RDBMS datastores.
 * Each instance is a factory of transactional or non-transactional connections, obtained through a javax.sql.DataSource.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    /** DataSource from which to get the connections. */
    DataSource dataSource;

    /**
     * Optional locally-managed pool of connections from which we get our connections (when using URL), that backs the dataSource. 
     * This is stored so that we can close the pool upon close.
     */
    ConnectionPool pool = null;

    /**
     * Constructor.
     * @param storeMgr Store Manager
     * @param resourceName either tx or nontx
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceName)
    {
        super(storeMgr, resourceName);
        if (resourceName.equals(RESOURCE_NAME_TX))
        {
            // Primary DataSource to be present always
            initialiseDataSource();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.connection.AbstractConnectionFactory#close()
     */
    @Override
    public void close()
    {
        if (pool != null)
        {
            // Close any DataNucleus-created connection pool
            if (NucleusLogger.CONNECTION.isDebugEnabled())
            {
                NucleusLogger.CONNECTION.debug(Localiser.msg("047010", getResourceName()));
            }
            pool.close();
        }
        super.close();
    }

    /**
     * Method to initialise the DataSource used by this ConnectionFactory.
     * Only invoked when the request for the first connection comes in.
     */
    protected synchronized void initialiseDataSource()
    {
        if (getResourceName().equals(RESOURCE_NAME_TX))
        {
            // Transactional
            String requiredPoolingType = storeMgr.getStringProperty(PropertyNames.PROPERTY_CONNECTION_POOLINGTYPE);
            Object connDS = storeMgr.getConnectionFactory();
            String connJNDI = storeMgr.getConnectionFactoryName();
            String connURL = storeMgr.getConnectionURL();
            dataSource = generateDataSource(storeMgr, connDS, connJNDI, getResourceName(), requiredPoolingType, connURL);
            if (dataSource == null)
            {
                throw new NucleusUserException(Localiser.msg("047009", "transactional")).setFatal();
            }
        }
        else
        {
            // Non-transactional
            String requiredPoolingType = storeMgr.getStringProperty(PropertyNames.PROPERTY_CONNECTION_POOLINGTYPE2);
            if (requiredPoolingType == null)
            {
                requiredPoolingType = storeMgr.getStringProperty(PropertyNames.PROPERTY_CONNECTION_POOLINGTYPE);
            }
            Object connDS = storeMgr.getConnectionFactory2();
            String connJNDI = storeMgr.getConnectionFactory2Name();
            String connURL = storeMgr.getConnectionURL();
            dataSource = generateDataSource(storeMgr, connDS, connJNDI, getResourceName(), requiredPoolingType, connURL);
            if (dataSource == null)
            {
                // Fallback to transactional settings
                connDS = storeMgr.getConnectionFactory();
                connJNDI = storeMgr.getConnectionFactoryName();
                dataSource = generateDataSource(storeMgr, connDS, connJNDI, getResourceName(), requiredPoolingType, connURL);
            }
            if (dataSource == null)
            {
                throw new NucleusUserException(Localiser.msg("047009", "non-transactional")).setFatal();
            }
        }
    }

    /**
     * Method to generate the datasource used by this connection factory.
     * Searches initially for a provided DataSource then, if not found, for JNDI DataSource, and finally for the DataSource at a connection URL.
     * @param storeMgr Store Manager
     * @param connDS Factory data source object
     * @param connJNDI DataSource JNDI name(s)
     * @param resourceName Resource name
     * @param requiredPoolingType Type of connection pool
     * @param connURL URL for connections
     * @return The DataSource
     */
    private DataSource generateDataSource(StoreManager storeMgr, Object connDS, String connJNDI, String resourceName, String requiredPoolingType, String connURL)
    {
        DataSource dataSource = null;
        if (connDS != null)
        {
            if (!(connDS instanceof DataSource) && !(connDS instanceof XADataSource))
            {
                throw new UnsupportedConnectionFactoryException(connDS);
            }
            dataSource = (DataSource) connDS;
        }
        else if (connJNDI != null)
        {
            String connectionFactoryName = connJNDI.trim();
            try
            {
                Object obj = new InitialContext().lookup(connectionFactoryName);
                if (!(obj instanceof DataSource) && !(obj instanceof XADataSource))
                {
                    throw new UnsupportedConnectionFactoryException(obj);
                }
                dataSource = (DataSource) obj;
            }
            catch (NamingException e)
            {
                throw new ConnectionFactoryNotFoundException(connectionFactoryName, e);
            }
        }
        else if (connURL != null)
        {
            String poolingType = requiredPoolingType;
            if (StringUtils.isWhitespace(requiredPoolingType))
            {
                // Default to dbcp2-builtin when nothing specified
                poolingType = "dbcp2-builtin";
            }

            // User has requested internal database connection pooling so check the registered plugins
            try
            {
                // Create the ConnectionPool to be used
                ConnectionPoolFactory connPoolFactory = null;

                // Try built-in pools first
                if (poolingType.equalsIgnoreCase("dbcp2-builtin"))
                {
                    connPoolFactory = new DBCP2BuiltinConnectionPoolFactory();
                }
                else if (poolingType.equalsIgnoreCase("HikariCP"))
                {
                    connPoolFactory = new HikariCPConnectionPoolFactory();
                }
                else if (poolingType.equalsIgnoreCase("BoneCP"))
                {
                    connPoolFactory = new BoneCPConnectionPoolFactory();
                }
                else if (poolingType.equalsIgnoreCase("C3P0"))
                {
                    connPoolFactory = new C3P0ConnectionPoolFactory();
                }
                else if (poolingType.equalsIgnoreCase("Tomcat"))
                {
                    connPoolFactory = new TomcatConnectionPoolFactory();
                }
                else if (poolingType.equalsIgnoreCase("DBCP2"))
                {
                    connPoolFactory = new DBCP2ConnectionPoolFactory();
                }
                else if (poolingType.equalsIgnoreCase("None"))
                {
                    connPoolFactory = new DefaultConnectionPoolFactory();
                }
                else
                {
                    // Fallback to the plugin mechanism
                    connPoolFactory = (ConnectionPoolFactory)storeMgr.getNucleusContext().getPluginManager().createExecutableExtension(
                        "org.datanucleus.store.rdbms.connectionpool", "name", poolingType, "class-name", null, null);
                    if (connPoolFactory == null)
                    {
                        // User has specified a pool plugin that has not registered
                        throw new NucleusUserException(Localiser.msg("047003", poolingType)).setFatal();
                    }
                }

                // Create the ConnectionPool and get the DataSource
                pool = connPoolFactory.createConnectionPool(storeMgr);
                dataSource = pool.getDataSource();
                if (NucleusLogger.CONNECTION.isDebugEnabled())
                {
                    NucleusLogger.CONNECTION.debug(Localiser.msg("047008", resourceName, poolingType));
                }
            }
            catch (ClassNotFoundException cnfe)
            {
                throw new NucleusUserException(Localiser.msg("047003", poolingType), cnfe).setFatal();
            }
            catch (Exception e)
            {
                if (e instanceof InvocationTargetException)
                {
                    InvocationTargetException ite = (InvocationTargetException)e;
                    throw new NucleusException(Localiser.msg("047004", poolingType, ite.getTargetException().getMessage()), ite.getTargetException()).setFatal();
                }

                throw new NucleusException(Localiser.msg("047004", poolingType, e.getMessage()),e).setFatal();
            }
        }
        return dataSource;
    }

    /**
     * Method to create a new ManagedConnection.
     * @param ec the object that is bound the connection during its lifecycle (if for a PM/EM operation)
     * @param options Options for creating the connection (optional)
     * @return The ManagedConnection
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map options)
    {
        if (dataSource == null)
        {
            // Lazy initialisation of DataSource
            initialiseDataSource();
        }

        ManagedConnection mconn = new ManagedConnectionImpl(ec, options);
        boolean singleConnection = storeMgr.getBooleanProperty(PropertyNames.PROPERTY_CONNECTION_SINGLE_CONNECTION);
        boolean releaseAfterUse = storeMgr.getBooleanProperty(PropertyNames.PROPERTY_CONNECTION_NONTX_RELEASE_AFTER_USE);
        if (ec != null && !ec.getTransaction().isActive() && (!releaseAfterUse || singleConnection))
        {
            // Non-transactional connection and requested not to close on release
            mconn.setCloseOnRelease(false);
        }
        return mconn;
    }

    class ManagedConnectionImpl extends AbstractManagedConnection
    {
        ExecutionContext ec;
        XAResource xaRes = null;
        int isolation;
        boolean needsCommitting = false;

        ManagedConnectionImpl(ExecutionContext ec, Map options)
        {
            this.ec = ec;
            if (options != null && options.get(Transaction.TRANSACTION_ISOLATION_OPTION) != null)
            {
                isolation = ((Number) options.get(Transaction.TRANSACTION_ISOLATION_OPTION)).intValue();
            }
            else
            {
                isolation = TransactionUtils.getTransactionIsolationLevelForName(storeMgr.getStringProperty(PropertyNames.PROPERTY_TRANSACTION_ISOLATION));
            }
        }

        /**
         * Release this connection.
         * Releasing this connection will allow this managed connection to be used one or more times inside the same transaction. 
         * If this managed connection is managed by a transaction manager, release is a no-op, otherwise the physical connection is closed
         */
        @Override
        public void release()
        {
            if (commitOnRelease)
            {
                // Nontransactional context, and need to commit the connection
                try
                {
                    DatastoreAdapter dba = ((RDBMSStoreManager)storeMgr).getDatastoreAdapter();
                    if (!dba.supportsOption(DatastoreAdapter.HOLD_CURSORS_OVER_COMMIT))
                    {
                        // Call mcPreClose since this datastore doesn't hold open results when calling conn.commit (Firebird)
                        for (int i=0;i<listeners.size();i++)
                        {
                            listeners.get(i).managedConnectionPreClose();
                        }
                    }

                    Connection conn = getSqlConnection();
                    if (conn != null && !conn.isClosed() && !conn.getAutoCommit())
                    {
                        // Make sure any remaining statements are executed and commit the connection
                        ((RDBMSStoreManager)storeMgr).getSQLController().processConnectionStatement(this);
                        this.needsCommitting = false;
                        if (NucleusLogger.CONNECTION.isDebugEnabled())
                        {
                            NucleusLogger.CONNECTION.debug(Localiser.msg("009015", this.toString()));
                        }
                        conn.commit();
                    }
                }
                catch (SQLException sqle)
                {
                    throw new NucleusDataStoreException(sqle.getMessage(), sqle);
                }
            }

            super.release();
        }

        /**
         * Obtain an XAResource which can be enlisted in a transaction
         */
        @Override
        public XAResource getXAResource()
        {
            if (xaRes != null)
            {
                return xaRes;
            }
            if (getConnection() instanceof Connection)
            {
                xaRes = new EmulatedXAResource(this);
            }
            else
            {
                try
                {
                    xaRes = ((XAConnection)getConnection()).getXAResource();
                }
                catch (SQLException e)
                {
                    throw new NucleusDataStoreException(e.getMessage(),e);
                }
            }
            return xaRes;
        }

        /**
         * Create a connection to the resource
         */
        public Object getConnection()
        {
            if (this.conn == null)
            {
                Connection cnx = null;
                try
                {
                    RDBMSStoreManager rdbmsMgr = (RDBMSStoreManager)storeMgr;
                    DatastoreAdapter dba = rdbmsMgr.getDatastoreAdapter();
                    boolean readOnly = ec != null ? ec.getBooleanProperty(PropertyNames.PROPERTY_DATASTORE_READONLY) : storeMgr.getBooleanProperty(PropertyNames.PROPERTY_DATASTORE_READONLY);
                    if (dba != null)
                    {
                        // Create Connection following DatastoreAdapter capabilities
                        cnx = dataSource.getConnection();
                        boolean succeeded = false;
                        try
                        {
                            if (cnx.isReadOnly() != readOnly)
                            {
                                NucleusLogger.CONNECTION.debug("Setting readonly=" + readOnly + " for connection: " + cnx.toString());
                                cnx.setReadOnly(readOnly);
                            }

                            int reqdIsolationLevel = (dba.getRequiredTransactionIsolationLevel() >= 0) ? dba.getRequiredTransactionIsolationLevel() : isolation;
                            if (reqdIsolationLevel == TransactionIsolation.NONE)
                            {
                                if (!cnx.getAutoCommit())
                                {
                                    cnx.setAutoCommit(true);
                                }
                            }
                            else
                            {
                                if (cnx.getAutoCommit())
                                {
                                    cnx.setAutoCommit(false);
                                }

                                if (dba.supportsTransactionIsolation(reqdIsolationLevel))
                                {
                                    int currentIsolationLevel = cnx.getTransactionIsolation();
                                    if (currentIsolationLevel != reqdIsolationLevel)
                                    {
                                        cnx.setTransactionIsolation(reqdIsolationLevel);
                                    }
                                }
                                else
                                {
                                    NucleusLogger.CONNECTION.warn(Localiser.msg("051008", reqdIsolationLevel));
                                }
                            }
                            if (reqdIsolationLevel != isolation && isolation == TransactionIsolation.NONE)
                            {
                                // User asked for a level that implies auto-commit so make sure it has that
                                if (!cnx.getAutoCommit())
                                {
                                    NucleusLogger.CONNECTION.debug("Setting autocommit=true for connection: "+StringUtils.toJVMIDString(cnx));
                                    cnx.setAutoCommit(true);
                                }
                            }

                            this.conn = cnx;
                            succeeded = true;

                            if (NucleusLogger.CONNECTION.isDebugEnabled())
                            {
                                NucleusLogger.CONNECTION.debug(Localiser.msg("009012", this.toString(), getResourceName(),
                                    TransactionUtils.getNameForTransactionIsolationLevel(reqdIsolationLevel), cnx.getAutoCommit()));
                            }
                        }
                        catch (SQLException e)
                        {
                            throw new NucleusDataStoreException(e.getMessage(),e);
                        }
                        finally
                        {
                            if (!succeeded)
                            {
                                try
                                {
                                    cnx.close();
                                }
                                catch (SQLException e)
                                {
                                }

                                if (NucleusLogger.CONNECTION.isDebugEnabled())
                                {
                                    NucleusLogger.CONNECTION.debug(Localiser.msg("009013", this.toString()));
                                }
                            }
                        }
                    }
                    else
                    {
                        // Create Connection from DataSource since no DatastoreAdapter created yet
                        cnx = dataSource.getConnection();
                        if (cnx == null)
                        {
                            String msg = Localiser.msg("009010", dataSource);
                            NucleusLogger.CONNECTION.error(msg);
                            throw new NucleusDataStoreException(msg);
                        }
                        if (NucleusLogger.CONNECTION.isDebugEnabled())
                        {
                            NucleusLogger.CONNECTION.debug(Localiser.msg("009011", this.toString(), getResourceName()));
                        }

                        this.conn = cnx;
                    }
                }
                catch (SQLException e)
                {
                    throw new NucleusDataStoreException(e.getMessage(),e);
                }
            }
            needsCommitting = true;
            return this.conn;
        }

        /**
         * Close the connection
         */
        public void close()
        {
            for (int i=0;i<listeners.size();i++)
            {
                listeners.get(i).managedConnectionPreClose();
            }

            Connection conn = getSqlConnection();
            if (conn != null)
            {
                try
                {
                    if (commitOnRelease && needsCommitting)
                    {
                        // Non-transactional context, so need to commit the connection
                        if (!conn.isClosed() && !conn.getAutoCommit())
                        {
                            // Make sure any remaining statements are executed and commit the connection
                            SQLController sqlController = ((RDBMSStoreManager)storeMgr).getSQLController();
                            if (sqlController != null)
                            {
                                sqlController.processConnectionStatement(this);
                            }

                            if (NucleusLogger.CONNECTION.isDebugEnabled())
                            {
                                NucleusLogger.CONNECTION.debug(Localiser.msg("009015", this.toString()));
                            }
                            conn.commit();

                            needsCommitting = false;
                        }
                    }
                }
                catch (SQLException sqle)
                {
                    throw new NucleusDataStoreException(sqle.getMessage(), sqle);
                }
                finally
                {
                    try
                    {
                        if (!conn.isClosed())
                        {
                            if (NucleusLogger.CONNECTION.isDebugEnabled())
                            {
                                NucleusLogger.CONNECTION.debug(Localiser.msg("009013", this.toString()));
                            }
                            conn.close();
                        }
                        else
                        {
                            if (NucleusLogger.CONNECTION.isDebugEnabled())
                            {
                                NucleusLogger.CONNECTION.debug(Localiser.msg("009014", this.toString()));
                            }
                        }
                    }
                    catch (SQLException sqle)
                    {
                        throw new NucleusDataStoreException(sqle.getMessage(), sqle);
                    }
                }
            }

            for (int i=0;i<listeners.size();i++)
            {
                listeners.get(i).managedConnectionPostClose();
            }

            if (savepoints != null)
            {
                savepoints.clear();
                savepoints = null;
            }
            this.xaRes = null;
            this.ec = null;

            super.close();
        }

        /**
         * Convenience accessor for the java.sql.Connection in use (if any).
         * @return SQL Connection
         */
        private Connection getSqlConnection()
        {
            if (this.conn != null && this.conn instanceof Connection)
            {
                return (Connection) this.conn;
            }
            else if (this.conn != null && this.conn instanceof XAConnection) // TODO How is this possible? this.conn is only set in getConnection to java.sql.Connection
            {
                try
                {
                    return ((XAConnection) this.conn).getConnection();
                }
                catch (SQLException e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }
            }
            return null;
        }

        private Map<String, Savepoint> savepoints = null;

        /* (non-Javadoc)
         * @see org.datanucleus.store.connection.AbstractManagedConnection#setSavepoint(java.lang.String)
         */
        @Override
        public void setSavepoint(String name)
        {
            try
            {
                Savepoint sp = ((Connection)conn).setSavepoint(name);
                if (savepoints == null)
                {
                    savepoints = new HashMap<>();
                }
                savepoints.put(name, sp);
            }
            catch (SQLException sqle)
            {
                throw new NucleusDataStoreException("Exception setting savepoint " + name, sqle);
            }
        }

        /* (non-Javadoc)
         * @see org.datanucleus.store.connection.AbstractManagedConnection#releaseSavepoint(java.lang.String)
         */
        @Override
        public void releaseSavepoint(String name)
        {
            try
            {
                if (savepoints == null)
                {
                    return;
                }
                Savepoint sp = savepoints.remove(name);
                if (sp == null)
                {
                    throw new IllegalStateException("No savepoint with name " + name);
                }
                ((Connection)conn).releaseSavepoint(sp);
            }
            catch (SQLException sqle)
            {
                throw new NucleusDataStoreException("Exception releasing savepoint " + name, sqle);
            }
        }

        /* (non-Javadoc)
         * @see org.datanucleus.store.connection.AbstractManagedConnection#rollbackToSavepoint(java.lang.String)
         */
        @Override
        public void rollbackToSavepoint(String name)
        {
            try
            {
                if (savepoints == null)
                {
                    return;
                }
                Savepoint sp = savepoints.get(name);
                if (sp == null)
                {
                    throw new IllegalStateException("No savepoint with name " + name);
                }
                ((Connection)conn).rollback(sp);
            }
            catch (SQLException sqle)
            {
                throw new NucleusDataStoreException("Exception rolling back to savepoint " + name, sqle);
            }
        }

        /* (non-Javadoc)
         * @see org.datanucleus.store.connection.AbstractManagedConnection#closeAfterTransactionEnd()
         */
        @Override
        public boolean closeAfterTransactionEnd()
        {
            if (storeMgr.getBooleanProperty(PropertyNames.PROPERTY_CONNECTION_SINGLE_CONNECTION))
            {
                return false;
            }
            return super.closeAfterTransactionEnd();
        }
    }

    /**
     * Emulate the two phase protocol for non XA
     */
    static class EmulatedXAResource extends AbstractEmulatedXAResource
    {
        Connection conn;

        EmulatedXAResource(ManagedConnection mconn)
        {
            super(mconn);
            this.conn = (Connection) mconn.getConnection();
        }

        public void commit(Xid xid, boolean onePhase) throws XAException
        {
            super.commit(xid, onePhase);
            try
            {
                NucleusLogger.CONNECTION.debug(Localiser.msg("009015", mconn.toString()));
                conn.commit();
                ((ManagedConnectionImpl)mconn).xaRes = null;
            }
            catch (SQLException e)
            {
                NucleusLogger.CONNECTION.debug(Localiser.msg("009020", mconn.toString(), xid.toString(), onePhase));
                XAException xe = new XAException(StringUtils.getStringFromStackTrace(e));
                xe.initCause(e);
                throw xe;
            }
        }

        public void rollback(Xid xid) throws XAException
        {
            super.rollback(xid);
            try
            {
                NucleusLogger.CONNECTION.debug(Localiser.msg("009016", mconn.toString()));
                conn.rollback();
                ((ManagedConnectionImpl)mconn).xaRes = null;
            }
            catch (SQLException e)
            {
                NucleusLogger.CONNECTION.debug(Localiser.msg("009022", mconn.toString(), xid.toString()));
                XAException xe = new XAException(StringUtils.getStringFromStackTrace(e));
                xe.initCause(e);
                throw xe;
            }
        }

        public void end(Xid xid, int flags) throws XAException
        {
            super.end(xid, flags);
            ((ManagedConnectionImpl)mconn).xaRes = null;
        }
    }
}