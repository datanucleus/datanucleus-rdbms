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
package org.datanucleus.store.rdbms.connectionpool;

import java.util.Properties;

import javax.sql.DataSource;

import org.datanucleus.store.StoreManager;
import org.datanucleus.store.rdbms.datasource.DriverManagerDataSource;

/**
 * Default ConnectionPool factory implementation (no pooling).
 */
public class DefaultConnectionPoolFactory implements ConnectionPoolFactory
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.datasource.ConnectionPoolFactory#createConnectionPool(org.datanucleus.store.StoreManager)
     */
    public ConnectionPool createConnectionPool(StoreManager storeMgr)
    {
        Properties props = AbstractConnectionPoolFactory.getPropertiesForDriver(storeMgr);
        if (props.size() == 2)
        {
            props = null;
        }
        return new DefaultConnectionPool(new DriverManagerDataSource(
            storeMgr.getConnectionDriverName(), storeMgr.getConnectionURL(),
            storeMgr.getConnectionUserName(), storeMgr.getConnectionPassword(),
            storeMgr.getNucleusContext().getClassLoaderResolver(null), props));
    }

    public class DefaultConnectionPool implements ConnectionPool
    {
        final DataSource dataSource;
        public DefaultConnectionPool(DataSource ds)
        {
            dataSource = ds;
        }
        public void close()
        {
            // Nothing to do since no pool
        }
        public DataSource getDataSource()
        {
            return dataSource;
        }
    }
}