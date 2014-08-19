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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

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

    /**
     * Wrapper to the JDBC DataSource class. 
     * Provides checking for driver class existence, and utility methods for obtaining a connection.
     * <P>
     * It should be noted that setting the log writer and login timeout will apply
     * to DriverManager and NOT to the Data Source on its own. If you have 2 or
     * more DataSource's they will have THE SAME log writer and login timeout. 
     * </P> 
     */
    public static class DriverManagerDataSource implements DataSource
    {
        /** Name of the database driver. */
        private final String driverName;

        /** URL for the database. */
        private final String url;

        /** ClassLoader resolver to use for class loading */
        private final ClassLoaderResolver clr;

        /** the user name **/
        private final String userName;

        /** the password **/
        private final String password;

        private final Properties props;

        /**
         * Constructor.
         * @param driverName Class name of the JDBC driver.
         * @param url URL of the data source.
         * @param userName User name
         * @param password User password
         * @param clr ClassLoaderResolver to use for loading issues
         * @param props Any custom properties for the driver
         */
        public DriverManagerDataSource(String driverName, String url, String userName, String password, ClassLoaderResolver clr, Properties props)
        {
            this.driverName = driverName;
            this.url = url;
            this.clr = clr;
            this.userName = userName;
            this.password = password;
            this.props = props;

            if (driverName != null)
            {
                try
                {
                    clr.classForName(driverName).newInstance();
                }
                catch (Exception e)
                {
                    try
                    {
                        Class.forName(driverName).newInstance();
                    }
                    catch (Exception e2)
                    {
                        throw new NucleusUserException(Localiser.msg("047006", driverName), e).setFatal();
                    }
                }
            }
        }

        /**
         * Accessor for a JDBC connection for this data source.
         * @return The connection
         * @throws SQLException Thrown when an error occurs obtaining the connection.
         */
        public Connection getConnection() throws SQLException
        {
            if (StringUtils.isWhitespace(driverName))
            {
                // User didnt bother specifying the JDBC driver
                throw new NucleusUserException(Localiser.msg("047007"));
            }

            return getConnection(this.userName, this.password);
        }

        /**
         * Accessor for a JDBC connection for this data source, specifying username and password.
         * @param userName User name for the data source (this user name is ignored)
         * @param password Password for the data source (this password is ignored)
         * @return The connection
         * @throws SQLException Thrown when an error occurs obtaining the connection.
         */
        public Connection getConnection(String userName, String password) throws SQLException
        {
            try
            {
                Properties info = new Properties();
                if (userName != null)
                {
                    info.put("user", this.userName);
                }
                if (password != null)
                {
                    info.put("password", this.password);
                }
                if (props != null)
                {
                    info.putAll(props);
                }
                return ((Driver) clr.classForName(driverName).newInstance()).connect(url, info);
            }
            catch (SQLException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                try
                {
                    //try with DriverManager for known drivers
                    return DriverManager.getConnection(url, this.userName, this.password);
                }
                catch (Exception e2)
                {
                    throw new NucleusUserException(Localiser.msg("047006", driverName), e).setFatal();
                }
            }
        }

        /**
         * Accessor for the LogWriter of the driver manager.
         * @return The Log Writer
         */
        public PrintWriter getLogWriter()
        {
            return DriverManager.getLogWriter();
        }

        /**
         * Mutator for the LogWriter of the driver manager.
         * @param out The Log Writer
         */
        public void setLogWriter(PrintWriter out)
        {
            DriverManager.setLogWriter(out);
        }

        /**
         * Accessor for the Login timeout for the driver manager.
         * @return The login timeout (seconds)
         */
        public int getLoginTimeout()
        {
            return DriverManager.getLoginTimeout();
        }

        /**
         * Mutator for the Login timeout for the driver manager.
         * @param seconds The login timeout (seconds)
         */
        public void setLoginTimeout(int seconds)
        {
            DriverManager.setLoginTimeout(seconds);
        }

        /**
         * Equality operator.
         * @param obj The object to compare against.
         * @return Whether the objects are equal.
         */
        public boolean equals(Object obj)
        {
            if (obj == this)
            {
                return true;
            }

            if (!(obj instanceof DriverManagerDataSource))
            {
                return false;
            }

            DriverManagerDataSource dmds = (DriverManagerDataSource) obj;
            if (driverName == null)
            {
                if (dmds.driverName != null)
                {
                    return false;
                }
            }
            else if (!driverName.equals(dmds.driverName))
            {
                return false;
            }

            if (url == null)
            {
                if (dmds.url != null)
                {
                    return false;
                }
            }
            else if (!url.equals(dmds.url))
            {
                return false;
            }

            return true;
        }

        /**
         * Hashcode operator.
         * @return The Hashcode for this object.
         */
        public int hashCode()
        {
            return (driverName == null ? 0 : driverName.hashCode()) ^ (url == null ? 0 : url.hashCode());
        }

        // Implementation of JDBC 4.0's Wrapper interface

        public Object unwrap(Class iface) throws SQLException
        {
            if (!DataSource.class.equals(iface))
            {
                throw new SQLException("DataSource of type [" + getClass().getName() +
                    "] can only be unwrapped as [javax.sql.DataSource], not as [" + iface.getName() + "]");
            }
            return this;
        }

        public boolean isWrapperFor(Class iface) throws SQLException
        {
            return DataSource.class.equals(iface);
        }

        public Logger getParentLogger() throws SQLFeatureNotSupportedException
        {
            throw new SQLFeatureNotSupportedException("Not supported");
        }
    }
}