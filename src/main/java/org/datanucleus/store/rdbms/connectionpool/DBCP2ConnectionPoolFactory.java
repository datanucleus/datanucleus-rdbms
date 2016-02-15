/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.connectionpool;

import java.util.Properties;

import javax.sql.DataSource;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.util.ClassUtils;

/**
 * Plugin for the creation of a DBCP2 connection pool.
 * Note that all Apache DBCP classes are named explicitly in the code to avoid loading them at class initialisation.
 * (see http://jakarta.apache.org/commons/dbcp/). Also see 
 * http://jakarta.apache.org/commons/dbcp/apidocs/org/apache/commons/dbcp/package-summary.html#package_description
 * for javadocs that give pretty much the only useful description of DBCP2.
 */
public class DBCP2ConnectionPoolFactory extends AbstractConnectionPoolFactory
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.datasource.ConnectionPoolFactory#createConnectionPool(org.datanucleus.store.StoreManager)
     */
    public ConnectionPool createConnectionPool(StoreManager storeMgr)
    {
        String dbDriver = storeMgr.getConnectionDriverName();
        String dbURL = storeMgr.getConnectionURL();

        // Load the database driver
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        loadDriver(dbDriver, clr);

        // Check the existence of the necessary pooling classes
        ClassUtils.assertClassForJarExistsInClasspath(clr, "org.apache.commons.pool2.ObjectPool", "commons-pool-2.x.jar");
        ClassUtils.assertClassForJarExistsInClasspath(clr, "org.apache.commons.dbcp2.ConnectionFactory", "commons-dbcp-2.x.jar");

        org.apache.commons.dbcp2.PoolingDataSource ds = null;
        org.apache.commons.pool2.impl.GenericObjectPool<org.apache.commons.dbcp2.PoolableConnection> connectionPool;
        try
        {
            // Create a factory to be used by the pool to create the connections
            Properties dbProps = getPropertiesForDriver(storeMgr);
            org.apache.commons.dbcp2.ConnectionFactory connectionFactory = new org.apache.commons.dbcp2.DriverManagerConnectionFactory(dbURL, dbProps);

            // Wrap the connections and statements with pooled variants
            org.apache.commons.dbcp2.PoolableConnectionFactory poolableCF = null;
            poolableCF = new org.apache.commons.dbcp2.PoolableConnectionFactory(connectionFactory, null);

            String testSQL = null;
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_TEST_SQL))
            {
                testSQL = storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_TEST_SQL);
                poolableCF.setValidationQuery(testSQL);
            }

            // Create the actual pool of connections, and apply any properties
            connectionPool = new org.apache.commons.pool2.impl.GenericObjectPool(poolableCF);
            poolableCF.setPool(connectionPool);
            if (testSQL != null)
            {
                connectionPool.setTestOnBorrow(true);
            }
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_IDLE))
            {
                int value = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_IDLE);
                if (value > 0)
                {
                    connectionPool.setMaxIdle(value);
                }
            }
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_IDLE))
            {
                int value = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_IDLE);
                if (value > 0)
                {
                    connectionPool.setMinIdle(value);
                }
            }
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_ACTIVE))
            {
                int value = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_ACTIVE);
                if (value > 0)
                {
                    connectionPool.setMaxTotal(value);
                }
            }
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_WAIT))
            {
                int value = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_WAIT);
                if (value > 0)
                {
                    connectionPool.setMaxWaitMillis(value);
                }
            }
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_TIME_BETWEEN_EVICTOR_RUNS_MILLIS))
            {
                // how often should the evictor run (if ever, default is -1 = off)
                int value = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_TIME_BETWEEN_EVICTOR_RUNS_MILLIS);
                if (value > 0)
                {
                    connectionPool.setTimeBetweenEvictionRunsMillis(value);

                    // in each eviction run, evict at least a quarter of "maxIdle" connections
                    int maxIdle = connectionPool.getMaxIdle();
                    int numTestsPerEvictionRun = (int) Math.ceil(((double) maxIdle / 4));
                    connectionPool.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
                }
            }
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS))
            {
                // how long may a connection sit idle in the pool before it may be evicted
                int value = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS);
                if (value > 0)
                {
                    connectionPool.setMinEvictableIdleTimeMillis(value);
                }
            }

            // Create the datasource
            ds = new org.apache.commons.dbcp2.PoolingDataSource(connectionPool);
        }
        catch (Exception e)
        {
            throw new DatastorePoolException("DBCP2", dbDriver, dbURL, e);
        }

        return new DBCPConnectionPool(ds, connectionPool);
    }

    public class DBCPConnectionPool implements ConnectionPool
    {
        final org.apache.commons.dbcp2.PoolingDataSource dataSource;
        final org.apache.commons.pool2.ObjectPool pool;
        public DBCPConnectionPool(org.apache.commons.dbcp2.PoolingDataSource ds, org.apache.commons.pool2.ObjectPool pool)
        {
            this.dataSource = ds;
            this.pool = pool;
        }
        public void close()
        {
            try
            {
                pool.close();
            }
            catch (Exception e)
            {
            }
        }
        public DataSource getDataSource()
        {
            return dataSource;
        }
    }
}