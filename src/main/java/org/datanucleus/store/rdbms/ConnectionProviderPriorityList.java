/**********************************************************************
Copyright (c) 2005 Erik Bengtson and others. All rights reserved.
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
package org.datanucleus.store.rdbms;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Utility class for Failover.
 */
public class ConnectionProviderPriorityList implements ConnectionProvider
{
    private boolean failOnError;
    
    public void setFailOnError(boolean flag)
    {
        this.failOnError = flag;        
    }
    
    /**
     * Obtain a connection from the datasources, starting on the first
     * datasource, and if unable to obtain a connection skips to the next one on the list, and try again until the list is exhausted.
     * @param ds the array of datasources. An ordered list of datasources
     * @return the Connection, null if <code>ds</code> is null, or null if the DataSources has returned a null as connection
     * @throws SQLException in case of error and failOnError is true or the error occurs while obtaining a connection with the last
     * DataSource on the list
     */
    public Connection getConnection(DataSource[] ds)
    throws SQLException
    {
        if (ds == null)
        {
            return null;
        }
        for (int i = 0; i < ds.length; i++)
        {
            try
            {
                return ds[i].getConnection();
            }
            catch (SQLException e)
            {
                if (failOnError || i == ds.length - 1)
                {
                    throw e;
                }
            }
        }
        return null;
    }
}