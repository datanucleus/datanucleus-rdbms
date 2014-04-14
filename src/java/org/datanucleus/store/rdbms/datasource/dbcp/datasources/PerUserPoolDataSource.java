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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.sql.ConnectionPoolDataSource;

import org.datanucleus.store.rdbms.datasource.dbcp.SQLNestedException;

import org.datanucleus.store.rdbms.datasource.dbcp.pool.ObjectPool;
import org.datanucleus.store.rdbms.datasource.dbcp.pool.impl.GenericObjectPool;

/**
 * <p>A pooling <code>DataSource</code> appropriate for deployment within
 * J2EE environment. 
 */
public class PerUserPoolDataSource
    extends InstanceKeyDataSource {

    private static final long serialVersionUID = -3104731034410444060L;

    private int defaultMaxActive = GenericObjectPool.DEFAULT_MAX_ACTIVE;
    private int defaultMaxIdle = GenericObjectPool.DEFAULT_MAX_IDLE;
    private int defaultMaxWait = (int)Math.min(Integer.MAX_VALUE,
        GenericObjectPool.DEFAULT_MAX_WAIT);
    Map perUserDefaultAutoCommit = null;    
    Map perUserDefaultTransactionIsolation = null;
    Map perUserMaxActive = null;    
    Map perUserMaxIdle = null;    
    Map perUserMaxWait = null;
    Map perUserDefaultReadOnly = null;    

    private transient Map /* <PoolKey, PooledConnectionManager> */ managers = new HashMap();

    public PerUserPoolDataSource() {
    }

    public void close() {
        for (Iterator poolIter = managers.values().iterator();
             poolIter.hasNext();) {    
            try {
              ((CPDSConnectionFactory) poolIter.next()).getPool().close();
            } catch (Exception closePoolException) {
                    //ignore and try to close others.
            }
        }
        InstanceKeyObjectFactory.removeInstance(instanceKey);
    }

    public int getDefaultMaxActive() {
        return (this.defaultMaxActive);
    }

    public void setDefaultMaxActive(int maxActive) {
        assertInitializationAllowed();
        this.defaultMaxActive = maxActive;
    }

    public int getDefaultMaxIdle() {
        return (this.defaultMaxIdle);
    }

    public void setDefaultMaxIdle(int defaultMaxIdle) {
        assertInitializationAllowed();
        this.defaultMaxIdle = defaultMaxIdle;
    }

    public int getDefaultMaxWait() {
        return (this.defaultMaxWait);
    }

    public void setDefaultMaxWait(int defaultMaxWait) {
        assertInitializationAllowed();
        this.defaultMaxWait = defaultMaxWait;
    }

    public Boolean getPerUserDefaultAutoCommit(String key) {
        Boolean value = null;
        if (perUserDefaultAutoCommit != null) {
            value = (Boolean) perUserDefaultAutoCommit.get(key);
        }
        return value;
    }
    
    public void setPerUserDefaultAutoCommit(String username, Boolean value) {
        assertInitializationAllowed();
        if (perUserDefaultAutoCommit == null) {
            perUserDefaultAutoCommit = new HashMap();
        }
        perUserDefaultAutoCommit.put(username, value);
    }

    public Integer getPerUserDefaultTransactionIsolation(String username) {
        Integer value = null;
        if (perUserDefaultTransactionIsolation != null) {
            value = (Integer) perUserDefaultTransactionIsolation.get(username);
        }
        return value;
    }

    public void setPerUserDefaultTransactionIsolation(String username, 
                                                      Integer value) {
        assertInitializationAllowed();
        if (perUserDefaultTransactionIsolation == null) {
            perUserDefaultTransactionIsolation = new HashMap();
        }
        perUserDefaultTransactionIsolation.put(username, value);
    }

    public Integer getPerUserMaxActive(String username) {
        Integer value = null;
        if (perUserMaxActive != null) {
            value = (Integer) perUserMaxActive.get(username);
        }
        return value;
    }
    
    public void setPerUserMaxActive(String username, Integer value) {
        assertInitializationAllowed();
        if (perUserMaxActive == null) {
            perUserMaxActive = new HashMap();
        }
        perUserMaxActive.put(username, value);
    }

    public Integer getPerUserMaxIdle(String username) {
        Integer value = null;
        if (perUserMaxIdle != null) {
            value = (Integer) perUserMaxIdle.get(username);
        }
        return value;
    }
    
    public void setPerUserMaxIdle(String username, Integer value) {
        assertInitializationAllowed();
        if (perUserMaxIdle == null) {
            perUserMaxIdle = new HashMap();
        }
        perUserMaxIdle.put(username, value);
    }
    
    public Integer getPerUserMaxWait(String username) {
        Integer value = null;
        if (perUserMaxWait != null) {
            value = (Integer) perUserMaxWait.get(username);
        }
        return value;
    }
    
    public void setPerUserMaxWait(String username, Integer value) {
        assertInitializationAllowed();
        if (perUserMaxWait == null) {
            perUserMaxWait = new HashMap();
        }
        perUserMaxWait.put(username, value);
    }

    public Boolean getPerUserDefaultReadOnly(String username) {
        Boolean value = null;
        if (perUserDefaultReadOnly != null) {
            value = (Boolean) perUserDefaultReadOnly.get(username);
        }
        return value;
    }
    
    public void setPerUserDefaultReadOnly(String username, Boolean value) {
        assertInitializationAllowed();
        if (perUserDefaultReadOnly == null) {
            perUserDefaultReadOnly = new HashMap();
        }
        perUserDefaultReadOnly.put(username, value);
    }

    public int getNumActive() {
        return getNumActive(null, null);
    }

    public int getNumActive(String username, String password) {
        ObjectPool pool = getPool(getPoolKey(username,password));
        return (pool == null) ? 0 : pool.getNumActive();
    }
    public int getNumIdle() {
        return getNumIdle(null, null);
    }

    public int getNumIdle(String username, String password) {
        ObjectPool pool = getPool(getPoolKey(username,password));
        return (pool == null) ? 0 : pool.getNumIdle();
    }


    protected PooledConnectionAndInfo 
        getPooledConnectionAndInfo(String username, String password)
        throws SQLException {

        final PoolKey key = getPoolKey(username,password);
        ObjectPool pool;
        PooledConnectionManager manager;
        synchronized(this) {
            manager = (PooledConnectionManager) managers.get(key);
            if (manager == null) {
                try {
                    registerPool(username, password);
                    manager = (PooledConnectionManager) managers.get(key);
                } catch (NamingException e) {
                    throw new SQLNestedException("RegisterPool failed", e);
                }
            }
            pool = ((CPDSConnectionFactory) manager).getPool();
        }

        PooledConnectionAndInfo info = null;
        try {
            info = (PooledConnectionAndInfo) pool.borrowObject();
        }
        catch (NoSuchElementException ex) {
            throw new SQLNestedException(
                    "Could not retrieve connection info from pool", ex);
        }
        catch (Exception e) {
            // See if failure is due to CPDSConnectionFactory authentication failure
            try {
                testCPDS(username, password);
            } catch (Exception ex) {
                throw (SQLException) new SQLException(
                        "Could not retrieve connection info from pool").initCause(ex);
            }
            // New password works, so kill the old pool, create a new one, and borrow
            manager.closePool(username);
            synchronized (this) {
                managers.remove(key);
            }
            try {
                registerPool(username, password);
                pool = getPool(key);
            } catch (NamingException ne) {
                throw new SQLNestedException("RegisterPool failed", ne);
            }
            try {
                info = (PooledConnectionAndInfo)pool.borrowObject();
            } catch (Exception ex) {
                throw (SQLException) new SQLException(
                "Could not retrieve connection info from pool").initCause(ex);
            }
        }
        return info;
    }

    protected void setupDefaults(Connection con, String username) 
        throws SQLException {
        boolean defaultAutoCommit = isDefaultAutoCommit();
        if (username != null) {
            Boolean userMax = getPerUserDefaultAutoCommit(username);
            if (userMax != null) {
                defaultAutoCommit = userMax.booleanValue();
            }
        }    

        boolean defaultReadOnly = isDefaultReadOnly();
        if (username != null) {
            Boolean userMax = getPerUserDefaultReadOnly(username);
            if (userMax != null) {
                defaultReadOnly = userMax.booleanValue();
            }
        }    

        int defaultTransactionIsolation = getDefaultTransactionIsolation();
        if (username != null) {
            Integer userMax = getPerUserDefaultTransactionIsolation(username);
            if (userMax != null) {
                defaultTransactionIsolation = userMax.intValue();
            }
        }

        if (con.getAutoCommit() != defaultAutoCommit) {
            con.setAutoCommit(defaultAutoCommit);
        }

        if (defaultTransactionIsolation != UNKNOWN_TRANSACTIONISOLATION) {
            con.setTransactionIsolation(defaultTransactionIsolation);
        }

        if (con.isReadOnly() != defaultReadOnly) {
            con.setReadOnly(defaultReadOnly);
        }
    }
    
    protected PooledConnectionManager getConnectionManager(UserPassKey upkey) {
        return (PooledConnectionManager) managers.get(getPoolKey(
                upkey.getUsername(), upkey.getPassword()));
    }

    /**
     * Returns a <code>PerUserPoolDataSource</code> {@link Reference}.
     * 
     * @since 1.2.2
     */
    public Reference getReference() throws NamingException {
        Reference ref = new Reference(getClass().getName(),
                PerUserPoolDataSourceFactory.class.getName(), null);
        ref.add(new StringRefAddr("instanceKey", instanceKey));
        return ref;
    }
    
    private PoolKey getPoolKey(String username, String password) { 
        return new PoolKey(getDataSourceName(), username); 
    }

    private synchronized void registerPool(
        String username, String password) 
        throws javax.naming.NamingException, SQLException {

        ConnectionPoolDataSource cpds = testCPDS(username, password);

        Integer userMax = getPerUserMaxActive(username);
        int maxActive = (userMax == null) ? 
            getDefaultMaxActive() : userMax.intValue();
        userMax = getPerUserMaxIdle(username);
        int maxIdle =  (userMax == null) ?
            getDefaultMaxIdle() : userMax.intValue();
        userMax = getPerUserMaxWait(username);
        int maxWait = (userMax == null) ?
            getDefaultMaxWait() : userMax.intValue();

        // Create an object pool to contain our PooledConnections
        GenericObjectPool pool = new GenericObjectPool(null);
        pool.setMaxActive(maxActive);
        pool.setMaxIdle(maxIdle);
        pool.setMaxWait(maxWait);
        pool.setWhenExhaustedAction(whenExhaustedAction(maxActive, maxWait));
        pool.setTestOnBorrow(getTestOnBorrow());
        pool.setTestOnReturn(getTestOnReturn());
        pool.setTimeBetweenEvictionRunsMillis(
            getTimeBetweenEvictionRunsMillis());
        pool.setNumTestsPerEvictionRun(getNumTestsPerEvictionRun());
        pool.setMinEvictableIdleTimeMillis(getMinEvictableIdleTimeMillis());
        pool.setTestWhileIdle(getTestWhileIdle());
                
        // Set up the factory we will use (passing the pool associates
        // the factory with the pool, so we do not have to do so
        // explicitly)
        CPDSConnectionFactory factory = new CPDSConnectionFactory(cpds, pool, getValidationQuery(),
                isRollbackAfterValidation(), username, password);
           
        Object old = managers.put(getPoolKey(username,password), factory);
        if (old != null) {
            throw new IllegalStateException("Pool already contains an entry for this user/password: "+username);
        }
    }

    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        try 
        {
            in.defaultReadObject();
            PerUserPoolDataSource oldDS = (PerUserPoolDataSource)
                new PerUserPoolDataSourceFactory()
                    .getObjectInstance(getReference(), null, null, null);
            this.managers = oldDS.managers;
        }
        catch (NamingException e)
        {
            throw new IOException("NamingException: " + e);
        }
    }
    
    private GenericObjectPool getPool(PoolKey key) {
        CPDSConnectionFactory mgr = (CPDSConnectionFactory) managers.get(key);
        return mgr == null ? null : (GenericObjectPool) mgr.getPool();
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }
}
