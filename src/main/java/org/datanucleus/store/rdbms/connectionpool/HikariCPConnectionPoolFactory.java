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

import javax.sql.DataSource;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.util.ClassUtils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * ConnectionFactory for HikariCP pools.
 * See https://github.com/brettwooldridge/HikariCP
 */
public class HikariCPConnectionPoolFactory extends AbstractConnectionPoolFactory
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.datasource.ConnectionPoolFactory#createConnectionPool(org.datanucleus.store.StoreManager)
     */
    public ConnectionPool createConnectionPool(StoreManager storeMgr)
    {
        String dbDriver = storeMgr.getConnectionDriverName();
        String dbURL = storeMgr.getConnectionURL();
        String dbUser = storeMgr.getConnectionUserName();
        if (dbUser == null)
        {
            dbUser = ""; // Some RDBMS (e.g Postgresql) don't like null usernames
        }
        String dbPassword = storeMgr.getConnectionPassword();
        if (dbPassword == null)
        {
            dbPassword = ""; // Some RDBMS (e.g Postgresql) don't like null passwords
        }

        // Load the database driver
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        loadDriver(dbDriver, clr);

        // Check the existence of the necessary pooling classes
        ClassUtils.assertClassForJarExistsInClasspath(clr, "com.zaxxer.hikari.HikariConfig", "hikaricp.jar");

        HikariConfig config = new HikariConfig();
        config.setDriverClassName(dbDriver);
        config.setJdbcUrl(dbURL);
        config.setUsername(dbUser);
        config.setPassword(dbPassword);
        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_POOL_SIZE))
        {
            int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_POOL_SIZE);
            if (size >= 0)
            {
                config.setMaximumPoolSize(size);
            }
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_IDLE))
        {
            long idle = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_IDLE);
            if (idle >= 0 )
            {
                config.setIdleTimeout(idle);
            }
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_LEAK_DETECTION_THRESHOLD))
        {
            long threshold = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_LEAK_DETECTION_THRESHOLD);
            if (threshold >= 0)
            {
                config.setLeakDetectionThreshold(threshold);
            }
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_LIFETIME))
        {
            long maxLifeTime = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_LIFETIME);
            if (maxLifeTime >= 0)
            {
                config.setMaxLifetime(maxLifeTime);
            }
        }

        // Create the actual pool of connections
        HikariDataSource ds = new HikariDataSource(config);

        return new HikariCPConnectionPool(ds);
    }

    public class HikariCPConnectionPool implements ConnectionPool
    {
        final HikariDataSource dataSource;
        public HikariCPConnectionPool(HikariDataSource ds)
        {
            dataSource = ds;
        }
        public void close()
        {
            dataSource.close();
        }
        public DataSource getDataSource()
        {
            return dataSource;
        }
    }
}