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

import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.util.ClassUtils;

/**
 * Plugin for the creation of a C3P0 connection pool.
 * Note that all C3P0 classes are named explicitly in the code to avoid loading
 * them at class initialisation.
 * See http://www.mchange.com/projects/c3p0/index.html
 * See http://www.sf.net/projects/c3p0
 */
public class C3P0ConnectionPoolFactory extends AbstractConnectionPoolFactory
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
        ClassUtils.assertClassForJarExistsInClasspath(clr, "com.mchange.v2.c3p0.DataSources", "c3p0.jar");

        try
        {
            Properties dbProps = getPropertiesForDriver(storeMgr);
            DataSource unpooled = com.mchange.v2.c3p0.DataSources.unpooledDataSource(dbURL, dbProps);

            // Apply any properties and make it a pooled DataSource
            // Note that C3P0 will always look for "c3p0.properties" at the root of the CLASSPATH
            Properties c3p0Props = new Properties();
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_STATEMENTS))
            {
                int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_STATEMENTS);
                if (size >= 0)
                {
                    c3p0Props.setProperty("maxStatementsPerConnection", "" + size);
                    c3p0Props.setProperty("maxStatements", "" + size);
                }
            }
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_POOL_SIZE))
            {
                int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_POOL_SIZE);
                if (size >= 0)
                {
                    c3p0Props.setProperty("maxPoolSize", "" + size);
                }
            }
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_POOL_SIZE))
            {
                int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MIN_POOL_SIZE);
                if (size >= 0)
                {
                    c3p0Props.setProperty("minPoolSize", "" + size);
                }
            }
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_INIT_POOL_SIZE))
            {
                int size = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_INIT_POOL_SIZE);
                if (size >= 0)
                {
                    c3p0Props.setProperty("initialPoolSize", "" + size);
                }
            }

            com.mchange.v2.c3p0.PooledDataSource ds = (com.mchange.v2.c3p0.PooledDataSource) com.mchange.v2.c3p0.DataSources.pooledDataSource(unpooled, c3p0Props);
            return new C3P0ConnectionPool(ds);
        }
        catch (SQLException sqle)
        {
            throw new DatastorePoolException("c3p0", dbDriver, dbURL, sqle);
        }
    }

    public class C3P0ConnectionPool implements ConnectionPool
    {
        final com.mchange.v2.c3p0.PooledDataSource dataSource;
        public C3P0ConnectionPool(com.mchange.v2.c3p0.PooledDataSource ds)
        {
            dataSource = ds;
        }
        public void close()
        {
            try
            {
                dataSource.close();
            }
            catch (SQLException sqle)
            {
            }
        }
        public DataSource getDataSource()
        {
            return dataSource;
        }
    }
}