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
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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

    private static Map<Integer, String> supportedJdbcTypesById = new HashMap();
    private static Map<Integer, String> unsupportedJdbcTypesById = new HashMap();

    static
    {
        // Add the supported types TODO Remove this in DN 4.0
        supportedJdbcTypesById.put(Integer.valueOf(Types.BIGINT), "BIGINT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.BIT), "BIT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.BLOB), "BLOB");
        supportedJdbcTypesById.put(Integer.valueOf(Types.BOOLEAN), "BOOLEAN");
        supportedJdbcTypesById.put(Integer.valueOf(Types.CHAR), "CHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.CLOB), "CLOB");
        supportedJdbcTypesById.put(Integer.valueOf(Types.DATALINK), "DATALINK");
        supportedJdbcTypesById.put(Integer.valueOf(Types.DATE), "DATE");
        supportedJdbcTypesById.put(Integer.valueOf(Types.DECIMAL), "DECIMAL");
        supportedJdbcTypesById.put(Integer.valueOf(Types.DOUBLE), "DOUBLE");
        supportedJdbcTypesById.put(Integer.valueOf(Types.FLOAT), "FLOAT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.INTEGER), "INTEGER");
        supportedJdbcTypesById.put(Integer.valueOf(Types.LONGVARBINARY), "LONGVARBINARY");
        supportedJdbcTypesById.put(Integer.valueOf(Types.LONGVARCHAR), "LONGVARCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.NUMERIC), "NUMERIC");
        supportedJdbcTypesById.put(Integer.valueOf(Types.REAL), "REAL");
        supportedJdbcTypesById.put(Integer.valueOf(Types.SMALLINT), "SMALLINT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.TIME), "TIME");
        supportedJdbcTypesById.put(Integer.valueOf(Types.TIMESTAMP), "TIMESTAMP");
        supportedJdbcTypesById.put(Integer.valueOf(Types.TINYINT), "TINYINT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.VARBINARY), "VARBINARY");
        supportedJdbcTypesById.put(Integer.valueOf(Types.VARCHAR), "VARCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(-9), "NVARCHAR"); // JDK 1.6 addition
        supportedJdbcTypesById.put(Integer.valueOf(-15), "NCHAR"); // JDK 1.6 addition
        supportedJdbcTypesById.put(Integer.valueOf(2011), "NCLOB"); // JDK 1.6 addition

        // Add the unsupported types
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.ARRAY), "ARRAY");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.BINARY), "BINARY");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.DISTINCT), "DISTINCT");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.JAVA_OBJECT), "JAVA_OBJECT");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.NULL), "NULL");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.OTHER), "OTHER");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.REF), "REF");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.STRUCT), "STRUCT");
    }

    /**
     * Accessor for a string name of a JDBC Type
     * @deprecated Use DatastoreAdapter.getNameForJDBCType(). Dropped in ver 4.0
     * @param jdbcType The JDBC Type
     * @return The name
     */
    public static String getNameForJDBCType(int jdbcType)
    {
        String typeName = supportedJdbcTypesById.get(Integer.valueOf(jdbcType));
        if (typeName == null)
        {
            typeName = unsupportedJdbcTypesById.get(Integer.valueOf(jdbcType));
        }
        return typeName;
    }
    
    /**
     * Method to return the type given the "jdbc-type" name.
     * @deprecated Use DatastoreAdapter.getNameForJDBCType(). Dropped in ver 4.0
     * @param typeName "jdbc-type" name
     * @return Whether it is valid
     */
    public static int getJDBCTypeForName(String typeName)
    {
        if (typeName == null)
        {
            return 0;
        }

        Set<Map.Entry<Integer, String>> entries = supportedJdbcTypesById.entrySet();
        Iterator<Map.Entry<Integer, String>> entryIter = entries.iterator();
        while (entryIter.hasNext())
        {
            Map.Entry<Integer, String> entry = entryIter.next();
            if (typeName.equalsIgnoreCase(entry.getValue()))
            {
                return entry.getKey().intValue();
            }
        }
        return 0;
    }

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