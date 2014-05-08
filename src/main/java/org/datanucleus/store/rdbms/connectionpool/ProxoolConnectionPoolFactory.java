/**********************************************************************
Copyright (c) 2006 Andy Jefferson and others. All rights reserved.
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
 * Plugin for the creation of a Proxool connection pool.
 * Note that all Proxool classes are named explicitly in the code to avoid loading
 * them at class initialisation. (see http://proxool.sourceforge.net/)
 */
public class ProxoolConnectionPoolFactory extends AbstractConnectionPoolFactory
{
    /** Number of the pool being created (using in the Proxool alias). */
    private static int poolNumber = 0;

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

        // Check the presence of commons-logging
        ClassUtils.assertClassForJarExistsInClasspath(clr, 
            "org.apache.commons.logging.Log", "commons-logging.jar");
        ClassUtils.assertClassForJarExistsInClasspath(clr, 
            "org.logicalcobwebs.proxool.ProxoolDriver", "proxool.jar");

        // Create a Proxool pool with alias "datanucleus{poolNumber}"
        String alias = "datanucleus" + poolNumber;
        String poolURL = null;
        try
        {
            // Apply any properties
            Properties dbProps = getPropertiesForDriver(storeMgr);

            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_CONNECTIONS))
            {
                int value = storeMgr.getIntProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_MAX_CONNECTIONS);
                if (value > 0)
                {
                    dbProps.put("proxool.maximum-connection-count", "" + value);
                }
                else
                {
                    dbProps.put("proxool.maximum-connection-count", "10");
                }
            }
            else
            {
                dbProps.put("proxool.maximum-connection-count", "10");
            }
            if (storeMgr.hasProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_TEST_SQL))
            {
                String value = storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_TEST_SQL);
                dbProps.put("proxool.house-keeping-test-sql", value);
            }
            else
            {
                dbProps.put("proxool.house-keeping-test-sql", "SELECT 1");
            }

            poolURL = "proxool." + alias + ":" + dbDriver + ":" + dbURL;
            poolNumber++;
            org.logicalcobwebs.proxool.ProxoolFacade.registerConnectionPool(poolURL, dbProps);
        }
        catch (org.logicalcobwebs.proxool.ProxoolException pe)
        {
            pe.printStackTrace();
            throw new DatastorePoolException("Proxool", dbDriver, dbURL, pe);
        }

        org.logicalcobwebs.proxool.ProxoolDataSource ds = new org.logicalcobwebs.proxool.ProxoolDataSource(alias);

        return new ProxoolConnectionPool(ds, poolURL);
    }

    public class ProxoolConnectionPool implements ConnectionPool
    {
        final String poolURL;
        final org.logicalcobwebs.proxool.ProxoolDataSource dataSource;
        public ProxoolConnectionPool(org.logicalcobwebs.proxool.ProxoolDataSource ds, String poolURL)
        {
            this.dataSource = ds;
            this.poolURL = poolURL;
        }
        public void close()
        {
            try
            {
                org.logicalcobwebs.proxool.ProxoolFacade.removeConnectionPool(poolURL);
            }
            catch (org.logicalcobwebs.proxool.ProxoolException e)
            {
            }
        }
        public DataSource getDataSource()
        {
            return dataSource;
        }
    }
}