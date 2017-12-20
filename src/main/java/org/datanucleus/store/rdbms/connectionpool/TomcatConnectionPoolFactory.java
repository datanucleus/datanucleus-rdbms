/**********************************************************************
Copyright (c) 2013 Marshall Reeske and others. All rights reserved.
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
2013 Andy Jefferson - integrate into DataNucleus
    ...
 **********************************************************************/
package org.datanucleus.store.rdbms.connectionpool;

import java.util.Properties;

import javax.sql.DataSource;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.StringUtils;

/**
 * Plugin for the creation of a Tomcat JDBC connection pool. Note that all Tomcat JDBC classes are named
 * explicitly in the code to avoid loading them at class initialisation.
 * http://tomcat.apache.org/tomcat-7.0-doc/jdbc-pool.html
 */
public class TomcatConnectionPoolFactory extends AbstractConnectionPoolFactory
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.datasource.ConnectionPoolFactory#createConnectionPool(org.datanucleus.store.StoreManager)
     */
    public ConnectionPool createConnectionPool(StoreManager storeMgr)
    {
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);

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
        String dbDriver = storeMgr.getConnectionDriverName();
        if (!StringUtils.isWhitespace(dbDriver))
        {
            loadDriver(dbDriver, clr);
        }

        // Check the existence of the necessary pooling classes
        ClassUtils.assertClassForJarExistsInClasspath(clr, "org.apache.tomcat.jdbc.pool.DataSource", "tomcat-jdbc.jar");

        org.apache.tomcat.jdbc.pool.PoolProperties config = new org.apache.tomcat.jdbc.pool.PoolProperties();
        String dbURL = storeMgr.getConnectionURL();
        config.setUrl(dbURL);
        config.setDriverClassName(dbDriver != null ? dbDriver : ""); // Tomcat JDBC Connection Pool doesn't like null driver class name
        config.setUsername(dbUser);
        config.setPassword(dbPassword);
        Properties dbProps = getPropertiesForDriver(storeMgr);
        config.setDbProperties(dbProps);

        // Apply any Tomcat JDBC Connection Pool properties
        if (storeMgr.hasProperty("datanucleus.connectionPool.abandonWhenPercentageFull"))
        {
            int value = storeMgr.getIntProperty("datanucleus.connectionPool.abandonWhenPercentageFull");
            if (value >= 0)
            {
                config.setAbandonWhenPercentageFull(value);
            }
        }
        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_INIT_POOL_SIZE))
        {
            int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_INIT_POOL_SIZE);
            if (size > 0)
            {
                config.setInitialSize(size);
            }
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.initSQL"))
        {
            String value = storeMgr.getStringProperty("datanucleus.connectionPool.initSQL");
            config.setInitSQL(value);
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.jdbcInterceptors"))
        {
            String value = storeMgr.getStringProperty("datanucleus.connectionPool.jdbcInterceptors");
            config.setJdbcInterceptors(value);
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.logAbandonded"))
        {
            String value = storeMgr.getStringProperty("datanucleus.connectionPool.logAbandonded");
            config.setLogAbandoned(Boolean.parseBoolean(value));
        }
        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_ACTIVE))
        {
            int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_ACTIVE);
            if (size > 0)
            {
                config.setMaxActive(size);
            }
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.maxAge"))
        {
            int value = storeMgr.getIntProperty("datanucleus.connectionPool.maxAge");
            if (value >= 0)
            {
                config.setMaxAge(value);
            }
        }
        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_IDLE))
        {
            int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_IDLE);
            if (size >= 0)
            {
                config.setMaxIdle(size);
            }
        }
        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_WAIT))
        {
            int value = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_WAIT);
            if (value > 0)
            {
                config.setMaxWait(value);
            }
        }
        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS))
        {
            int value = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS);
            if (value > 0)
            {
                config.setMinEvictableIdleTimeMillis(value);
            }
        }
        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_IDLE))
        {
            int value = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_IDLE);
            if (value >= 0)
            {
                config.setMinIdle(value);
            }
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.removeAbandonded"))
        {
            String value = storeMgr.getStringProperty("datanucleus.connectionPool.removeAbandonded");
            config.setRemoveAbandoned(Boolean.parseBoolean(value));
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.removeAbandondedTimeout"))
        {
            int value = storeMgr.getIntProperty("datanucleus.connectionPool.removeAbandondedTimeout");
            if (value > 0)
            {
                config.setRemoveAbandonedTimeout(value);
            }
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.suspectTimeout"))
        {
            int value = storeMgr.getIntProperty("datanucleus.connectionPool.suspectTimeout");
            if (value > 0)
            {
                config.setSuspectTimeout(value);
            }
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.testOnBorrow"))
        {
            String value = storeMgr.getStringProperty("datanucleus.connectionPool.testOnBorrow");
            config.setTestOnBorrow(Boolean.parseBoolean(value));
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.testOnConnect"))
        {
            String value = storeMgr.getStringProperty("datanucleus.connectionPool.testOnConnect");
            config.setTestOnConnect(Boolean.parseBoolean(value));
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.testOnReturn"))
        {
            String value = storeMgr.getStringProperty("datanucleus.connectionPool.testOnReturn");
            config.setTestOnReturn(Boolean.parseBoolean(value));
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.testWhileIdle"))
        {
            String value = storeMgr.getStringProperty("datanucleus.connectionPool.testWhileIdle");
            config.setTestWhileIdle(Boolean.parseBoolean(value));
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.timeBetweenEvictionRunsMillis"))
        {
            int value = storeMgr.getIntProperty("datanucleus.connectionPool.timeBetweenEvictionRunsMillis");
            if (value > 0)
            {
                config.setTimeBetweenEvictionRunsMillis(value);
            }
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.validationInterval"))
        {
            int value = storeMgr.getIntProperty("datanucleus.connectionPool.validationInterval");
            if (value >= 0)
            {
                config.setValidationInterval(value);
            }
        }
        if (storeMgr.hasProperty("datanucleus.connectionPool.validationQuery"))
        {
            String value = storeMgr.getStringProperty("datanucleus.connectionPool.validationQuery");
            config.setValidationQuery(value);
        }

        // Create the actual pool of connections.
        return new TomcatConnectionPool(new org.apache.tomcat.jdbc.pool.DataSource(config));
    }

    public class TomcatConnectionPool implements ConnectionPool
    {
        final org.apache.tomcat.jdbc.pool.DataSource dataSource;
        public TomcatConnectionPool(org.apache.tomcat.jdbc.pool.DataSource ds)
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