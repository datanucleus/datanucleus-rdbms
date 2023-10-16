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
import org.datanucleus.util.StringUtils;

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
        // Check the existence of the necessary pooling classes
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        ClassUtils.assertClassForJarExistsInClasspath(clr, "com.zaxxer.hikari.HikariConfig", "hikaricp.jar");

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(storeMgr.getConnectionURL());

        String dbUser = storeMgr.getConnectionUserName();
        if (dbUser == null)
        {
            dbUser = ""; // Some RDBMS (e.g Postgresql) don't like null usernames
        }
        config.setUsername(dbUser);

        String dbPassword = storeMgr.getConnectionPassword();
        if (dbPassword == null)
        {
            dbPassword = ""; // Some RDBMS (e.g Postgresql) don't like null passwords
        }
        config.setPassword(dbPassword);

        String dbDriver = storeMgr.getConnectionDriverName();
        if (!StringUtils.isWhitespace(dbDriver))
        {
            loadDriver(dbDriver, clr);
            config.setDriverClassName(dbDriver);
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_POOL_SIZE))
        {
            int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_POOL_SIZE);
            if (size >= 0)
            {
                config.setMaximumPoolSize(size);
            }
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_IDLE))
        {
            int idle = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_IDLE);
            if (idle >= 0)
            {
                config.setMinimumIdle(idle);
            }
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_IDLE_TIMEOUT))
        {
            long timeout = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_IDLE_TIMEOUT);
            if (timeout >= 0 )
            {
                config.setIdleTimeout(timeout);
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

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_AUTO_COMMIT))
        {
            boolean autoCommit = storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_AUTO_COMMIT);
            config.setAutoCommit(autoCommit);
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_CONNECTION_WAIT_TIMEOUT))
        {
            long connectionTimeout = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_CONNECTION_WAIT_TIMEOUT);
            if (connectionTimeout >= 0)
            {
                config.setConnectionTimeout(connectionTimeout);
            }
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_NAME))
        {
            String poolName = storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_NAME);
            config.setPoolName(poolName);
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_TRANSACTION_ISOLATION))
        {
            String transactionIsolation = storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_TRANSACTION_ISOLATION);
            config.setTransactionIsolation(transactionIsolation);
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_CATALOG))
        {
            String catalog = storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_CATALOG);
            config.setCatalog(catalog);
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_READ_ONLY))
        {
            boolean readOnly = storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_READ_ONLY);
            config.setReadOnly(readOnly);
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_ALLOW_POOL_SUPSENSION))
        {
            boolean allowPoolSuspension = storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_ALLOW_POOL_SUPSENSION);
            config.setAllowPoolSuspension(allowPoolSuspension);
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_VALIDATION_TIMEOUT))
        {
            long validationTimeout = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_VALIDATION_TIMEOUT);
            if (validationTimeout >= 0)
            {
                config.setValidationTimeout(validationTimeout);
            }
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_INIT_SQL))
        {
            String connectionInitSQL = storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_INIT_SQL);
            config.setConnectionInitSql(connectionInitSQL);
        }

        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_REGISTER_MBEANS))
        {
            boolean registerMbeans = storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_REGISTER_MBEANS);
            config.setRegisterMbeans(registerMbeans);
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