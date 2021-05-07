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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.StringTokenizer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.Configuration;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;

/**
 * Abstract superclass for ConnectionPool factory.
 */
public abstract class AbstractConnectionPoolFactory implements ConnectionPoolFactory
{
    /**
     * Convenience method to load the (JDBC) driver.
     * @param dbDriver Datastore driver
     * @param clr Class loader resolver
     */
    protected void loadDriver(String dbDriver, ClassLoaderResolver clr)
    {
        // Load the database driver
        try
        {
            clr.classForName(dbDriver).getDeclaredConstructor().newInstance();
        }
        catch (Exception e)
        {
            try
            {
                Class.forName(dbDriver).getDeclaredConstructor().newInstance();
            }
            catch (Exception e2)
            {
                // JDBC driver not found
                throw new DatastoreDriverNotFoundException(dbDriver);
            }
        }
    }

    /**
     * Convenience method to return the properties to pass to the driver.
     * Includes as a minimum "user" and "password", but a user may define a persistence property with name
     * "datanucleus.connectionPool.driverProps" then is a comma separated name-value pair that are
     * treated as properties
     * @param storeMgr StoreManager
     * @return The properties for the driver
     */
    public static Properties getPropertiesForDriver(StoreManager storeMgr)
    {
        Properties dbProps = new Properties();

        String dbUser = storeMgr.getConnectionUserName();
        if (dbUser == null)
        {
            dbUser = ""; // Some RDBMS (e.g Postgresql) don't like null usernames
        }
        dbProps.setProperty("user", dbUser);

        String dbPassword = storeMgr.getConnectionPassword();
        if (dbPassword == null)
        {
            dbPassword = ""; // Some RDBMS (e.g Postgresql) don't like null passwords
        }
        dbProps.setProperty("password", dbPassword);

        // Optional driver properties
        Configuration conf = storeMgr.getNucleusContext().getConfiguration();
        String drvPropsString = (String) conf.getProperty(RDBMSPropertyNames.PROPERTY_CONNECTION_POOL_DRIVER_PROPS);
        if (drvPropsString != null) 
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintWriter pw = new PrintWriter(baos);
            StringTokenizer st = new StringTokenizer(drvPropsString, ",");
            while (st.hasMoreTokens())
            {
                String prop = st.nextToken();
                pw.println(prop);
            }
            pw.flush();
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            Properties drvProps = new Properties();
            try
            {
                drvProps.load(bais);
            }
            catch (IOException e) {}
			dbProps.putAll(drvProps);
        }
        return dbProps;
    }
}