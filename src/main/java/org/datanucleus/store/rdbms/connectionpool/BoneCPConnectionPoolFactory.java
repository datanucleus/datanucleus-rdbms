/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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
 * ConnectionFactory for BoneCP pools.
 */
public class BoneCPConnectionPoolFactory extends AbstractConnectionPoolFactory
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
        ClassUtils.assertClassForJarExistsInClasspath(clr, "com.jolbox.bonecp.BoneCPDataSource", "bonecp.jar");

        com.jolbox.bonecp.BoneCPConfig config = new com.jolbox.bonecp.BoneCPConfig();
        config.setUsername(dbUser);
        config.setPassword(dbPassword);
        Properties dbProps = getPropertiesForDriver(storeMgr);
        config.setDriverProperties(dbProps);

        // Create the actual pool of connections
        com.jolbox.bonecp.BoneCPDataSource ds = new com.jolbox.bonecp.BoneCPDataSource(config);

        // Apply any BoneCP properties
        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_STATEMENTS))
        {
            int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_STATEMENTS);
            if (size >= 0)
            {
                ds.setStatementsCacheSize(size);
            }
        }
        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_POOL_SIZE))
        {
            int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_POOL_SIZE);
            if (size >= 0)
            {
                ds.setMaxConnectionsPerPartition(size);
            }
        }
        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_POOL_SIZE))
        {
            int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_POOL_SIZE);
            if (size >= 0)
            {
                ds.setMinConnectionsPerPartition(size);
            }
        }
        if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_IDLE))
        {
            int value = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_IDLE);
            if (value > 0)
            {
                ds.setIdleMaxAgeInMinutes(value);
            }
        }

        ds.setJdbcUrl(dbURL);
        ds.setUsername(dbUser);
        ds.setPassword(dbPassword);

        return new BoneCPConnectionPool(ds);
    }

    public class BoneCPConnectionPool implements ConnectionPool
    {
        final com.jolbox.bonecp.BoneCPDataSource dataSource;
        public BoneCPConnectionPool(com.jolbox.bonecp.BoneCPDataSource ds)
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