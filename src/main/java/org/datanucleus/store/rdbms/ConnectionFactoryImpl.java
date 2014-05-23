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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.Transaction;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.ConnectionFactoryNotFoundException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.exceptions.UnsupportedConnectionFactoryException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractEmulatedXAResource;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.connectionpool.ConnectionPool;
import org.datanucleus.store.rdbms.connectionpool.ConnectionPoolFactory;
import org.datanucleus.transaction.TransactionIsolation;
import org.datanucleus.transaction.TransactionUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * ConnectionFactory for RDBMS datastores.
 * Each instance is a factory of Transactional connection or NonTransactional connection.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    /** Datasources. */
    DataSource[] dataSources;

    ConnectionPool pool = null;

    /**
     * Constructor.
     * @param storeMgr Store Manager
     * @param resourceType either tx or nontx
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);
        if (resourceType.equals("tx"))
        {
            // JTA needs the primary DataSource to be present always
            initialiseDataSources();
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
                NucleusLogger.CONNECTION.debug(Localiser.msg("047010", resourceType));
            }
            pool.close();
        }
        super.close();
    }

    /**
     * Method to initialise the DataSource(s) used by this ConnectionFactory.
     * Only invoked when the request for the first connection comes in.
     */
    protected synchronized void initialiseDataSources()
    {
        if (resourceType.equals("tx"))
        {
            // Transactional
            // TODO Remove this, now part of AbstractConnectionFactory
            String configuredResourceTypeProperty = storeMgr.getStringProperty(DATANUCLEUS_CONNECTION_RESOURCE_TYPE);
            if (configuredResourceTypeProperty != null)
            {
                if (options == null)
                {
                    options = new HashMap();
                }
                options.put(ConnectionFactory.RESOURCE_TYPE_OPTION, configuredResourceTypeProperty);
            }

            String requiredPoolingType = storeMgr.getStringProperty(PropertyNames.PROPERTY_CONNECTION_POOLINGTYPE);
            Object connDS = storeMgr.getConnectionFactory();
            String connJNDI = storeMgr.getConnectionFactoryName();
            String connURL = storeMgr.getConnectionURL();
            dataSources = generateDataSources(storeMgr, connDS, connJNDI, resourceType, requiredPoolingType, connURL);
            if (dataSources == null)
            {
                throw new NucleusUserException(Localiser.msg("047009", "transactional")).setFatal();
            }
        }
        else
        {
            // Non-transactional
            // TODO Remove this, now part of AbstractConnectionFactory
            String configuredResourceTypeProperty = storeMgr.getStringProperty(DATANUCLEUS_CONNECTION2_RESOURCE_TYPE);
            if (configuredResourceTypeProperty!=null)
            {
                if (options == null)
                {
                    options = new HashMap();
                }
                options.put(ConnectionFactory.RESOURCE_TYPE_OPTION, configuredResourceTypeProperty);
            }

            String requiredPoolingType = storeMgr.getStringProperty(PropertyNames.PROPERTY_CONNECTION_POOLINGTYPE2);
            if (requiredPoolingType == null)
            {
                requiredPoolingType = storeMgr.getStringProperty(PropertyNames.PROPERTY_CONNECTION_POOLINGTYPE);
            }
            Object connDS = storeMgr.getConnectionFactory2();
            String connJNDI = storeMgr.getConnectionFactory2Name();
            String connURL = storeMgr.getConnectionURL();
            dataSources = generateDataSources(storeMgr, connDS, connJNDI, resourceType, requiredPoolingType, connURL);
            if (dataSources == null)
            {
                // Fallback to transactional settings
                connDS = storeMgr.getConnectionFactory();
                connJNDI = storeMgr.getConnectionFactoryName();
                dataSources = generateDataSources(storeMgr, connDS, connJNDI, resourceType, requiredPoolingType, connURL);
            }
            if (dataSources == null)
            {
                throw new NucleusUserException(Localiser.msg("047009", "non-transactional")).setFatal();
            }
        }
    }

    /**
     * Method to generate the datasource(s) used by this connection factory.
     * Searches initially for a provided DataSource, then if not found, for JNDI DataSource(s), and finally
     * for the DataSource at a connection URL.
     * @param storeMgr Store Manager
     * @param connDS Factory data source object
     * @param connJNDI DataSource JNDI name(s)
     * @param connURL URL for connections
     * @return The DataSource(s)
     */
    private DataSource[] generateDataSources(StoreManager storeMgr, Object connDS, String connJNDI, 
            String resourceType, String requiredPoolingType, String connURL)
    {
        DataSource[] dataSources = null;
        if (connDS != null)
        {
            if (!(connDS instanceof DataSource) && !(connDS instanceof XADataSource))
            {
                throw new UnsupportedConnectionFactoryException(connDS);
            }
            dataSources = new DataSource[1];
            dataSources[0] = (DataSource) connDS;
        }
        else if (connJNDI != null)
        {
            String[] connectionFactoryNames = StringUtils.split(connJNDI, ",");
            dataSources = new DataSource[connectionFactoryNames.length];
            for (int i=0; i<connectionFactoryNames.length; i++)
            {
                Object obj;
                try
                {
                    obj = new InitialContext().lookup(connectionFactoryNames[i]);
                }
                catch (NamingException e)
                {
                    throw new ConnectionFactoryNotFoundException(connectionFactoryNames[i], e);
                }
                if (!(obj instanceof DataSource) && !(obj instanceof XADataSource))
                {
                    throw new UnsupportedConnectionFactoryException(obj);
                }
                dataSources[i] = (DataSource) obj;
            }
        }
        else if (connURL != null)
        {
            dataSources = new DataSource[1];
            String poolingType = calculatePoolingType(storeMgr, requiredPoolingType);

            // User has requested internal database connection pooling so check the registered plugins
            try
            {
                // Create the ConnectionPool to be used
                ConnectionPoolFactory connPoolFactory =
                    (ConnectionPoolFactory)storeMgr.getNucleusContext().getPluginManager().createExecutableExtension(
                        "org.datanucleus.store.rdbms.connectionpool", "name", poolingType, "class-name", null, null);
                if (connPoolFactory == null)
                {
                    // User has specified a pool plugin that has not registered
                    throw new NucleusUserException(Localiser.msg("047003", poolingType)).setFatal();
                }

                // Create the ConnectionPool and get the DataSource
                pool = connPoolFactory.createConnectionPool(storeMgr);
                dataSources[0] = pool.getDataSource();
                if (NucleusLogger.CONNECTION.isDebugEnabled())
                {
                    NucleusLogger.CONNECTION.debug(Localiser.msg("047008", resourceType, poolingType));
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
                    throw new NucleusException(Localiser.msg("047004", poolingType,
                        ite.getTargetException().getMessage()), ite.getTargetException()).setFatal();
                }
                else
                {
                    throw new NucleusException(Localiser.msg("047004", poolingType,
                        e.getMessage()),e).setFatal();
                }
            }
        }
        return dataSources;
    }

    /**
     * Method to create a new ManagedConnection.
     * @param ec the object that is bound the connection during its lifecycle (if for a PM/EM operation)
     * @param options Options for creating the connection (optional)
     * @return The ManagedConnection
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map options)
    {
        if (dataSources == null)
        {
            // Lazy initialisation of DataSource(s)
            initialiseDataSources();
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

        ConnectionProvider connProvider = null;

        ManagedConnectionImpl(ExecutionContext ec, Map options)
        {
            if (options != null && options.get(Transaction.TRANSACTION_ISOLATION_OPTION) != null)
            {
                isolation = ((Number) options.get(Transaction.TRANSACTION_ISOLATION_OPTION)).intValue();
            }
            else
            {
                isolation = TransactionUtils.getTransactionIsolationLevelForName(
                    storeMgr.getStringProperty(PropertyNames.PROPERTY_TRANSACTION_ISOLATION));
            }

            // Use the required ConnectionProvider
            try
            {
                connProvider = (ConnectionProvider) storeMgr.getNucleusContext().getPluginManager().createExecutableExtension(
                        "org.datanucleus.store.rdbms.connectionprovider", "name",
                        storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CONNECTION_PROVIDER_NAME),
                        "class-name", null, null);
                if (connProvider == null)
                {
                    // No provider with this name (missing plugin ?)
                    throw new NucleusException(Localiser.msg("050000",
                        storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CONNECTION_PROVIDER_NAME))).setFatal();
                }
                connProvider.setFailOnError(storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CONNECTION_PROVIDER_FAIL_ON_ERROR));
            }
            catch (Exception e)
            {
                // Error creating provider
                throw new NucleusException(Localiser.msg("050001",
                    storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CONNECTION_PROVIDER_NAME), e.getMessage()), e).setFatal();
            }
        }

        /**
         * Release this connection.
         * Releasing this connection will allow this managed connection to be used one or more times
         * inside the same transaction. If this managed connection is managed by a transaction manager,
         * release is a no-op, otherwise the physical connection is closed
         */
        @Override
        public void release()
        {
            if (commitOnRelease)
            {
                // Nontransactional context, and need to commit the connection
                try
                {
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
                    boolean readOnly = ec != null ? ec.getBooleanProperty(PropertyNames.PROPERTY_DATASTORE_READONLY) : storeMgr.getBooleanProperty(PropertyNames.PROPERTY_DATASTORE_READONLY);
                    if (rdbmsMgr.getDatastoreAdapter() != null)
                    {
                        // Create Connection following DatastoreAdapter capabilities
                        DatastoreAdapter rdba = rdbmsMgr.getDatastoreAdapter();
                        int reqdIsolationLevel = isolation;
                        if (rdba.getRequiredTransactionIsolationLevel() >= 0)
                        {
                            // Override with the adapters required isolation level
                            reqdIsolationLevel = rdba.getRequiredTransactionIsolationLevel();
                        }

                        cnx = connProvider.getConnection(dataSources);
                        boolean succeeded = false;
                        try
                        {
                            if (cnx.isReadOnly() != readOnly)
                            {
                                NucleusLogger.CONNECTION.debug("Setting readonly=" + readOnly + " to connection: " + cnx.toString());
                                cnx.setReadOnly(readOnly);
                            }

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
                                if (rdba.supportsTransactionIsolation(reqdIsolationLevel))
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

                            if (NucleusLogger.CONNECTION.isDebugEnabled())
                            {
                                NucleusLogger.CONNECTION.debug(Localiser.msg("009012", this.toString(),
                                    TransactionUtils.getNameForTransactionIsolationLevel(reqdIsolationLevel),
                                    cnx.getAutoCommit()));
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

                            succeeded = true;
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
                        // Create basic Connection since no DatastoreAdapter created yet
                        cnx = dataSources[0].getConnection();
                        if (cnx == null)
                        {
                            String msg = Localiser.msg("009010", dataSources[0]);
                            NucleusLogger.CONNECTION.error(msg);
                            throw new NucleusDataStoreException(msg);
                        }
                        if (NucleusLogger.CONNECTION.isDebugEnabled())
                        {
                            NucleusLogger.CONNECTION.debug(Localiser.msg("009011", this.toString()));
                        }
                    }
                }
                catch (SQLException e)
                {
                    throw new NucleusDataStoreException(e.getMessage(),e);
                }

                this.conn = cnx;
            }
            needsCommitting = true;
            return this.conn;
        }

        /**
         * Close the connection
         */
        public void close()
        {
            for (int i=0; i<listeners.size(); i++)
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

                            conn.commit();
                            needsCommitting = false;
                            if (NucleusLogger.CONNECTION.isDebugEnabled())
                            {
                                NucleusLogger.CONNECTION.debug(Localiser.msg("009015", this.toString()));
                            }
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

            try
            {
                for (int i=0; i<listeners.size(); i++)
                {
                    listeners.get(i).managedConnectionPostClose();
                }
            }
            finally
            {
                listeners.clear();
            }
            if (savepoints != null)
            {
                savepoints.clear();
                savepoints = null;
            }
            this.conn = null;
            this.xaRes = null;
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
            else if (this.conn != null && this.conn instanceof XAConnection)
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
                    savepoints = new HashMap<String, Savepoint>();
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

    /**
     * Method to set the connection pooling type (if any).
     * Tries to use any user-provided value if possible, otherwise will fallback to something
     * available in the CLASSPATH (if any), else use the builtin DBCP.
     * @param storeMgr Store Manager
     * @param requiredPoolingType Pooling type requested by the user
     * @return Pooling type to use (name of a pool type)
     */
    protected static String calculatePoolingType(StoreManager storeMgr, String requiredPoolingType)
    {
        String poolingType = requiredPoolingType;
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);

        if (poolingType != null)
        {
            // User-specified, so check availability
            if (poolingType.equalsIgnoreCase("DBCP") && !dbcpPresent(clr))
            {
                NucleusLogger.CONNECTION.warn("DBCP specified but not present in CLASSPATH (or one of dependencies)");
                poolingType = null;
            }
            else if (poolingType.equalsIgnoreCase("C3P0") && !c3p0Present(clr))
            {
                NucleusLogger.CONNECTION.warn("C3P0 specified but not present in CLASSPATH (or one of dependencies)");
                poolingType = null;
            }
            else if (poolingType.equalsIgnoreCase("Proxool") && !proxoolPresent(clr))
            {
                NucleusLogger.CONNECTION.warn("Proxool specified but not present in CLASSPATH (or one of dependencies)");
                poolingType = null;
            }
            else if (poolingType.equalsIgnoreCase("BoneCP") && !bonecpPresent(clr))
            {
                NucleusLogger.CONNECTION.warn("BoneCP specified but not present in CLASSPATH (or one of dependencies)");
                poolingType = null;
            }
        }

        // Not specified, so try to find one in the CLASSPATH
        if (poolingType == null && dbcpPresent(clr))
        {
            poolingType = "DBCP";
        }
        if (poolingType == null && c3p0Present(clr))
        {
            poolingType = "C3P0";
        }
        if (poolingType == null && proxoolPresent(clr))
        {
            poolingType = "Proxool";
        }
        if (poolingType == null && bonecpPresent(clr))
        {
            poolingType = "BoneCP";
        }

        if (poolingType == null)
        {
            // Fallback to built-in DBCP (JDK1.6+)
            poolingType = "dbcp-builtin";
        }
        return poolingType;
    }

    protected static boolean dbcpPresent(ClassLoaderResolver clr)
    {
        try
        {
            // Need commons-dbcp, commons-pool, commons-collections
            clr.classForName("org.apache.commons.pool.ObjectPool");
            clr.classForName("org.apache.commons.dbcp.ConnectionFactory");
            return true;
        }
        catch (ClassNotResolvedException cnre)
        {
            // DBCP not available
            return false;
        }
    }

    protected static boolean c3p0Present(ClassLoaderResolver clr)
    {
        try
        {
            // Need c3p0
            clr.classForName("com.mchange.v2.c3p0.ComboPooledDataSource");
            return true;
        }
        catch (ClassNotResolvedException cnre)
        {
            // C3P0 not available
            return false;
        }
    }

    protected static boolean proxoolPresent(ClassLoaderResolver clr)
    {
        try
        {
            // Need proxool, commons-logging
            clr.classForName("org.logicalcobwebs.proxool.ProxoolDriver");
            clr.classForName("org.apache.commons.logging.Log");
            return true;
        }
        catch (ClassNotResolvedException cnre)
        {
            // Proxool not available
            return false;
        }
    }

    protected static boolean bonecpPresent(ClassLoaderResolver clr)
    {
        try
        {
            // Need bonecp, slf4j, google-collections
            clr.classForName("com.jolbox.bonecp.BoneCPDataSource");
            clr.classForName("org.slf4j.Logger");
            clr.classForName("com.google.common.collect.Multiset");
            return true;
        }
        catch (ClassNotResolvedException cnre)
        {
            // BoneCP not available
            return false;
        }
    }
}