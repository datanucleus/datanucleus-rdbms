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
package org.datanucleus.store.rdbms;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.StringTokenizer;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Convenience helper for JDBC.
 */
public class JDBCUtils
{
    private static final Localiser LOCALISER = Localiser.getInstance("org.datanucleus.store.rdbms.Localisation",
        RDBMSStoreManager.class.getClassLoader());

    /**
     * Method to return the "subprotocol" for a JDBC URL.
     * A JDBC URL is made up of
     * "jdbc:{subprotocol}:...". For example, "jdbc:mysql:..." or "jdbc:hsqldb:...".
     * @param url The JDBC URL
     * @return The subprotocol
     */
    public static String getSubprotocolForURL(String url)
    {
        StringTokenizer tokeniser = new StringTokenizer(url, ":");
        tokeniser.nextToken();
        return tokeniser.nextToken();
    }

    /**
     * Logs SQL warnings to the common log. 
     * Should be called after any operation on a JDBC <tt>Statement</tt> or <tt>ResultSet</tt> object.
     * @param warning the value returned from getWarnings().
     */
    public static void logWarnings(SQLWarning warning)
    {
        while (warning != null)
        {
            NucleusLogger.DATASTORE.warn(LOCALISER.msg("052700", warning.getMessage()));
            warning = warning.getNextWarning();
        }
    }

    /**
     * Utility to log all warning for the specified Connection.
     * @param conn The connection to the datastore
     **/
    public static void logWarnings(Connection conn)
    {
        try
        {
            logWarnings(conn.getWarnings());
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER.msg("052701",conn),e);
        }
    }

    /**
     * Utility to log all warning for the specified Statement.
     * @param stmt The statement
     **/
    public static void logWarnings(Statement stmt)
    {
        try
        {
            logWarnings(stmt.getWarnings());
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER.msg("052702",stmt), e);
        }
    }

    /**
     * Utility to log all warning for the specified ResultSet.
     * @param rs The ResultSet
     **/
    public static void logWarnings(ResultSet rs)
    {
        try
        {
            logWarnings(rs.getWarnings());
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER.msg("052703",rs), e);
        }
    }
}