/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.schema.StoreSchemaData;

/**
 * Represents the metadata of a specific primary key column. 
 * Supports the following properties.
 * <ul>
 * <li>table_cat</li>
 * <li>table_schem</li>
 * <li>table_name</li>
 * <li>column_name</li>
 * <li>key_seq</li>
 * <li>pk_name</li>
 * </ul>
 */
public class PrimaryKeyInfo implements StoreSchemaData
{
    /** Properties of the primary-key. */
    Map properties = new HashMap();

    /** Hashcode. Set on first use. */
    private int hash = 0;

    /**
     * Constructs a primary key information object from the current row of the given result set.
     * The {@link ResultSet} object passed must have been obtained from a call
     * to java.sql.DatabaseMetaData.getPrimaryKeys().
     * @param rs The result set returned from java.sql.DatabaseMetaData.getPrimaryKeys().
     * @exception NucleusDataStoreException if an exception occurs during retrieval
     */
    public PrimaryKeyInfo(ResultSet rs)
    {
        try
        {
            addProperty("table_cat", rs.getString(1));
            addProperty("table_schem", rs.getString(2));
            addProperty("table_name", rs.getString(3));
            addProperty("column_name", rs.getString(4));
            addProperty("key_seq", Short.valueOf(rs.getShort(5)));
            addProperty("pk_name", rs.getString(6));
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException("Can't read JDBC metadata from result set", e).setFatal();
        }
    }

    /**
     * Method to add a property for the PK.
     * @param name Name of property
     * @param value Its value
     */
    public void addProperty(String name, Object value)
    {
        if (name != null && value != null)
        {
            properties.put(name, value);
        }
    }

    /**
     * Accessor for a property.
     * @param name Name of the property
     * @return Its value, or null if not defined
     */
    public Object getProperty(String name)
    {
        return properties.get(name);
    }

    /**
     * Indicates whether some object is "equal to" this one.
     * Two <tt>PrimaryKeyInfo</tt> objects are considered equal if their catalog, schema, table, and 
     * column names are all equal.
     * @param obj the reference object with which to compare
     * @return Whether they are equal
     */
    public final boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof PrimaryKeyInfo))
        {
            return false;
        }

        PrimaryKeyInfo other = (PrimaryKeyInfo)obj;
        String tableCat1 = (String)getProperty("table_cat");
        String tableSch1 = (String)getProperty("table_schema");
        String tableName1 = (String)getProperty("table_name");
        String columnName1 = (String)getProperty("column_name");
        String pkName1 = (String)getProperty("pk_name");
        String tableCat2 = (String)other.getProperty("table_cat");
        String tableSch2 = (String)other.getProperty("table_schema");
        String tableName2 = (String)other.getProperty("table_name");
        String columnName2 = (String)other.getProperty("column_name");
        String pkName2 = (String)other.getProperty("pk_name");

        return (tableCat1 == null ? tableCat2 == null : tableCat1.equals(tableCat2)) &&
            (tableSch1 == null ? tableSch2 == null : tableSch1.equals(tableSch2)) &&
            tableName1.equals(tableName2) &&
            columnName1.equals(columnName2) &&
            (tableCat1 == null ? tableCat2 == null : tableCat1.equals(tableCat2)) &&
            (tableSch1 == null ? tableSch2 == null : tableSch1.equals(tableSch2)) &&
            tableName1.equals(tableName2) &&
            columnName1.equals(columnName2) &&
            (pkName1 == null ? pkName2 == null : pkName1.equals(pkName2));
    }

    /**
     * Returns a hash code value for this object.
     * @return  a hash code value for this object.
     */
    public final int hashCode()
    {
        if (hash == 0)
        {
            String tableCat = (String)getProperty("table_cat");
            String tableSch = (String)getProperty("table_schema");
            String tableName = (String)getProperty("table_name");
            String columnName = (String)getProperty("column_name");
            hash = (tableCat == null ? 0 : tableCat.hashCode()) ^
                    (tableSch == null ? 0 : tableSch.hashCode()) ^
                    tableName.hashCode() ^
                    columnName.hashCode();
        }

        return hash;
    }

    /**
     * Returns the string representation of this object.
     * @return  string representation of this object.
     */
    public String toString()
    {
        StringBuffer str= new StringBuffer();
        str.append(this.getClass().getName() + "\n");
        str.append("  tableCat    = " + getProperty("table_cat") + "\n");
        str.append("  tableSchem  = " + getProperty("table_schema") + "\n");
        str.append("  tableName   = " + getProperty("table_name") + "\n");
        str.append("  columnName  = " + getProperty("column_name") + "\n");
        str.append("  keySeq      = " + getProperty("key_seq") + "\n");
        str.append("  pkName      = " + getProperty("pk_name") + "\n");
        return str.toString();
    }
}