/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.datanucleus.store.rdbms.datasource.dbcp.datasources;

import java.io.Serializable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.Referenceable;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;

import org.datanucleus.store.rdbms.datasource.dbcp.SQLNestedException;
import org.datanucleus.store.rdbms.datasource.dbcp.pool.impl.GenericObjectPool;

/**
 * <p>The base class for <code>SharedPoolDataSource</code> and 
 * <code>PerUserPoolDataSource</code>.  
 */
public abstract class InstanceKeyDataSource
        implements DataSource, Referenceable, Serializable {
    private static final long serialVersionUID = -4243533936955098795L;
    private static final String GET_CONNECTION_CALLED 
            = "A Connection was already requested from this source, " 
            + "further initialization is not allowed.";
    private static final String BAD_TRANSACTION_ISOLATION
        = "The requested TransactionIsolation level is invalid.";
    /**
    * Internal constant to indicate the level is not set. 
    */
    protected static final int UNKNOWN_TRANSACTIONISOLATION = -1;
    
    /** Guards property setters - once true, setters throw IllegalStateException */
    private volatile boolean getConnectionCalled = false;

    /** Underlying source of PooledConnections */
    private ConnectionPoolDataSource dataSource = null;
    
    /** DataSource Name used to find the ConnectionPoolDataSource */
    private String dataSourceName = null;
    
    // Default connection properties
    private boolean defaultAutoCommit = false;
    private int defaultTransactionIsolation = UNKNOWN_TRANSACTIONISOLATION;
    private boolean defaultReadOnly = false;
    
    /** Description */
    private String description = null;
    
    /** Environment that may be used to set up a jndi initial context. */
    Properties jndiEnvironment = null;
    
    /** Login TimeOut in seconds */
    private int loginTimeout = 0;
    
    /** Log stream */
    private PrintWriter logWriter = null;
    
    // Pool properties
    private boolean _testOnBorrow = GenericObjectPool.DEFAULT_TEST_ON_BORROW;
    private boolean _testOnReturn = GenericObjectPool.DEFAULT_TEST_ON_RETURN;
    private int _timeBetweenEvictionRunsMillis = (int)
        Math.min(Integer.MAX_VALUE,
                 GenericObjectPool.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
    private int _numTestsPerEvictionRun = 
        GenericObjectPool.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
    private int _minEvictableIdleTimeMillis = (int)
    Math.min(Integer.MAX_VALUE,
             GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
    private boolean _testWhileIdle = GenericObjectPool.DEFAULT_TEST_WHILE_IDLE;
    private String validationQuery = null;
    private boolean rollbackAfterValidation = false;
    
    /** true iff one of the setters for testOnBorrow, testOnReturn, testWhileIdle has been called. */
    private boolean testPositionSet = false;

    /** Instance key */
    protected String instanceKey = null;

    public InstanceKeyDataSource() {
        defaultAutoCommit = true;
    }

    protected void assertInitializationAllowed()
        throws IllegalStateException {
        if (getConnectionCalled) {
            throw new IllegalStateException(GET_CONNECTION_CALLED);
        }
    }

    public abstract void close() throws Exception;
    
    protected abstract PooledConnectionManager getConnectionManager(UserPassKey upkey);

    /* JDBC_4_ANT_KEY_BEGIN */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("InstanceKeyDataSource is not a wrapper.");
    }
    /* JDBC_4_ANT_KEY_END */

    public ConnectionPoolDataSource getConnectionPoolDataSource() {
        return dataSource;
    }
    
    public void setConnectionPoolDataSource(ConnectionPoolDataSource v) {
        assertInitializationAllowed();
        if (dataSourceName != null) {
            throw new IllegalStateException(
                "Cannot set the DataSource, if JNDI is used.");
        }
        if (dataSource != null) 
        {
            throw new IllegalStateException(
                "The CPDS has already been set. It cannot be altered.");
        }
        dataSource = v;
        instanceKey = InstanceKeyObjectFactory.registerNewInstance(this);
    }

    public String getDataSourceName() {
        return dataSourceName;
    }
    
    public void setDataSourceName(String v) {
        assertInitializationAllowed();
        if (dataSource != null) {
            throw new IllegalStateException(
                "Cannot set the JNDI name for the DataSource, if already " +
                "set using setConnectionPoolDataSource.");
        }
        if (dataSourceName != null) 
        {
            throw new IllegalStateException(
                "The DataSourceName has already been set. " + 
                "It cannot be altered.");
        }
        this.dataSourceName = v;
        instanceKey = InstanceKeyObjectFactory.registerNewInstance(this);
    }

    public boolean isDefaultAutoCommit() {
        return defaultAutoCommit;
    }
    
    public void setDefaultAutoCommit(boolean v) {
        assertInitializationAllowed();
        this.defaultAutoCommit = v;
    }

    public boolean isDefaultReadOnly() {
        return defaultReadOnly;
    }
    
    public void setDefaultReadOnly(boolean v) {
        assertInitializationAllowed();
        this.defaultReadOnly = v;
    }

    public int getDefaultTransactionIsolation() {
            return defaultTransactionIsolation;
    }

    public void setDefaultTransactionIsolation(int v) {
        assertInitializationAllowed();
        switch (v) {
        case Connection.TRANSACTION_NONE:
        case Connection.TRANSACTION_READ_COMMITTED:
        case Connection.TRANSACTION_READ_UNCOMMITTED:
        case Connection.TRANSACTION_REPEATABLE_READ:
        case Connection.TRANSACTION_SERIALIZABLE:
            break;
        default:
            throw new IllegalArgumentException(BAD_TRANSACTION_ISOLATION);
        }
        this.defaultTransactionIsolation = v;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String v) {
        this.description = v;
    }
        
    public String getJndiEnvironment(String key) {
        String value = null;
        if (jndiEnvironment != null) {
            value = jndiEnvironment.getProperty(key);
        }
        return value;
    }
    
    public void setJndiEnvironment(String key, String value) {
        if (jndiEnvironment == null) {
            jndiEnvironment = new Properties();
        }
        jndiEnvironment.setProperty(key, value);
    }
    
    public int getLoginTimeout() {
        return loginTimeout;
    }
    
    public void setLoginTimeout(int v) {
        this.loginTimeout = v;
    }
        
    public PrintWriter getLogWriter() {
        if (logWriter == null) {
            logWriter = new PrintWriter(System.out);
        }        
        return logWriter;
    }
    
    public void setLogWriter(PrintWriter v) {
        this.logWriter = v;
    }
    
    public final boolean isTestOnBorrow() {
        return getTestOnBorrow();
    }
    
    public boolean getTestOnBorrow() {
        return _testOnBorrow;
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        assertInitializationAllowed();
        _testOnBorrow = testOnBorrow;
        testPositionSet = true;
    }

    public final boolean isTestOnReturn() {
        return getTestOnReturn();
    }
    
    public boolean getTestOnReturn() {
        return _testOnReturn;
    }

    public void setTestOnReturn(boolean testOnReturn) {
        assertInitializationAllowed();
        _testOnReturn = testOnReturn;
        testPositionSet = true;
    }

    public int getTimeBetweenEvictionRunsMillis() {
        return _timeBetweenEvictionRunsMillis;
    }

    public void 
        setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
        assertInitializationAllowed();
            _timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    public int getNumTestsPerEvictionRun() {
        return _numTestsPerEvictionRun;
    }

    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        assertInitializationAllowed();
        _numTestsPerEvictionRun = numTestsPerEvictionRun;
    }

    public int getMinEvictableIdleTimeMillis() {
        return _minEvictableIdleTimeMillis;
    }

    public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
        assertInitializationAllowed();
        _minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
    }

    public final boolean isTestWhileIdle() {
        return getTestWhileIdle();
    }
    
    public boolean getTestWhileIdle() {
        return _testWhileIdle;
    }

    public void setTestWhileIdle(boolean testWhileIdle) {
        assertInitializationAllowed();
        _testWhileIdle = testWhileIdle;
        testPositionSet = true;
    }

    public String getValidationQuery() {
        return this.validationQuery;
    }

    public void setValidationQuery(String validationQuery) {
        assertInitializationAllowed();
        this.validationQuery = validationQuery;
        if (!testPositionSet) {
            setTestOnBorrow(true);
        }
    }

    public boolean isRollbackAfterValidation() {
        return this.rollbackAfterValidation;
    }

    public void setRollbackAfterValidation(boolean rollbackAfterValidation) {
        assertInitializationAllowed();
        this.rollbackAfterValidation = rollbackAfterValidation;
    }

    // ----------------------------------------------------------------------
    // Instrumentation Methods

    // ----------------------------------------------------------------------
    // DataSource implementation 

    public Connection getConnection() throws SQLException {
        return getConnection(null, null);
    }

    public Connection getConnection(String username, String password)
            throws SQLException {        
        if (instanceKey == null) {
            throw new SQLException("Must set the ConnectionPoolDataSource " 
                    + "through setDataSourceName or setConnectionPoolDataSource"
                    + " before calling getConnection.");
        }
        getConnectionCalled = true;
        PooledConnectionAndInfo info = null;
        try {
            info = getPooledConnectionAndInfo(username, password);
        } catch (NoSuchElementException e) {
            closeDueToException(info);
            throw new SQLNestedException("Cannot borrow connection from pool", e);
        } catch (RuntimeException e) {
            closeDueToException(info);
            throw e;
        } catch (SQLException e) {            
            closeDueToException(info);
            throw e;
        } catch (Exception e) {
            closeDueToException(info);
            throw new SQLNestedException("Cannot borrow connection from pool", e);
        }
        
        if (!(null == password ? null == info.getPassword() 
                : password.equals(info.getPassword()))) {  // Password on PooledConnectionAndInfo does not match
            try { // See if password has changed by attempting connection
                testCPDS(username, password);
            } catch (SQLException ex) {
                // Password has not changed, so refuse client, but return connection to the pool
                closeDueToException(info);
                throw new SQLException("Given password did not match password used"
                                       + " to create the PooledConnection.");
            } catch (javax.naming.NamingException ne) {
                throw (SQLException) new SQLException(
                        "NamingException encountered connecting to database").initCause(ne);
            }
            /*
             * Password must have changed -> destroy connection and keep retrying until we get a new, good one,
             * destroying any idle connections with the old passowrd as we pull them from the pool.
             */
            final UserPassKey upkey = info.getUserPassKey();
            final PooledConnectionManager manager = getConnectionManager(upkey);
            manager.invalidate(info.getPooledConnection()); // Destroy and remove from pool
            manager.setPassword(upkey.getPassword()); // Reset the password on the factory if using CPDSConnectionFactory
            info = null;
            for (int i = 0; i < 10; i++) { // Bound the number of retries - only needed if bad instances return 
                try {
                    info = getPooledConnectionAndInfo(username, password);
                } catch (NoSuchElementException e) {
                    closeDueToException(info);
                    throw new SQLNestedException("Cannot borrow connection from pool", e);
                } catch (RuntimeException e) {
                    closeDueToException(info);
                    throw e;
                } catch (SQLException e) {            
                    closeDueToException(info);
                    throw e;
                } catch (Exception e) {
                    closeDueToException(info);
                    throw new SQLNestedException("Cannot borrow connection from pool", e);
                }
                if (info != null && password.equals(info.getPassword())) {
                    break;
                } 

                if (info != null) {
                    manager.invalidate(info.getPooledConnection());
                }
                info = null;
            }  
            if (info == null) {
                throw new SQLException("Cannot borrow connection from pool - password change failure.");
            }
        }

        Connection con = info.getPooledConnection().getConnection();
        try { 
            setupDefaults(con, username);
            con.clearWarnings();
            return con;
        } catch (SQLException ex) {  
            try {
                con.close();
            } catch (Exception exc) { 
                getLogWriter().println(
                     "ignoring exception during close: " + exc);
            }
            throw ex;
        }
    }

    protected abstract PooledConnectionAndInfo 
        getPooledConnectionAndInfo(String username, String password)
        throws SQLException;

    protected abstract void setupDefaults(Connection con, String username) 
        throws SQLException;

        
    private void closeDueToException(PooledConnectionAndInfo info) {
        if (info != null) {
            try {
                info.getPooledConnection().getConnection().close();
            } catch (Exception e) {
                // do not throw this exception because we are in the middle
                // of handling another exception.  But record it because
                // it potentially leaks connections from the pool.
                getLogWriter().println("[ERROR] Could not return connection to "
                    + "pool during exception handling. " + e.getMessage());   
            }
        }
    }

    protected ConnectionPoolDataSource 
        testCPDS(String username, String password)
        throws javax.naming.NamingException, SQLException {
        // The source of physical db connections
        ConnectionPoolDataSource cpds = this.dataSource;
        if (cpds == null) {            
            Context ctx = null;
            if (jndiEnvironment == null) {
                ctx = new InitialContext();                
            } else {
                ctx = new InitialContext(jndiEnvironment);
            }
            Object ds = ctx.lookup(dataSourceName);
            if (ds instanceof ConnectionPoolDataSource) {
                cpds = (ConnectionPoolDataSource) ds;
            } else {
                throw new SQLException("Illegal configuration: "
                    + "DataSource " + dataSourceName
                    + " (" + ds.getClass().getName() + ")"
                    + " doesn't implement javax.sql.ConnectionPoolDataSource");
            }
        }
        
        // try to get a connection with the supplied username/password
        PooledConnection conn = null;
        try {
            if (username != null) {
                conn = cpds.getPooledConnection(username, password);
            }
            else {
                conn = cpds.getPooledConnection();
            }
            if (conn == null) {
                throw new SQLException(
                    "Cannot connect using the supplied username/password");
            }
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                }
                catch (SQLException e) {
                    // at least we could connect
                }
            }
        }
        return cpds;
    }

    protected byte whenExhaustedAction(int maxActive, int maxWait) {
        byte whenExhausted = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
        if (maxActive <= 0) {
            whenExhausted = GenericObjectPool.WHEN_EXHAUSTED_GROW;
        } else if (maxWait == 0) {
            whenExhausted = GenericObjectPool.WHEN_EXHAUSTED_FAIL;
        }
        return whenExhausted;
    }    

    // ----------------------------------------------------------------------
    // Referenceable implementation 

    public Reference getReference() throws NamingException {
        final String className = getClass().getName();
        final String factoryName = className + "Factory"; // XXX: not robust 
        Reference ref = new Reference(className, factoryName, null);
        ref.add(new StringRefAddr("instanceKey", instanceKey));
        return ref;
    }
}
