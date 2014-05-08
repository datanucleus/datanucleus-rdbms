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
 * Represents the metadata of a specific index column. 
 * Supports the following properties.
 * <ul>
 * <li>table_cat</li>
 * <li>table_schem</li>
 * <li>table_name</li>
 * <li>column_name</li>
 * <li>non_unique</li>
 * <li>index_name</li>
 * <li>type</li>
 * <li>ordinal_position</li>
 * </ul>
 */
public class IndexInfo implements StoreSchemaData
{
    /** Properties of the index. */
    Map properties = new HashMap();

    /** Hashcode. Set on first use. */
    private int hash = 0;

    /**
     * Constructs an index information object from the current row of the given result set.
     * The {@link ResultSet} object passed must have been obtained from a call
     * to java.sql.DatabaseMetaData.getIndexInfo().
     * @param rs The result set returned from java.sql.DatabaseMetaData.getIndexInfo().
     * @exception NucleusDataStoreException if an exception occurs during retrieval
     */
    public IndexInfo(ResultSet rs)
    {
        try
        {
            addProperty("table_cat", rs.getString(1));
            addProperty("table_schem", rs.getString(2));
            addProperty("table_name", rs.getString(3));
            addProperty("non_unique", Boolean.valueOf(rs.getBoolean(4)));
            addProperty("index_name", rs.getString(6));
            addProperty("type", Short.valueOf(rs.getShort(7)));
            addProperty("ordinal_position", Short.valueOf(rs.getShort(8)));
            addProperty("column_name", rs.getString(9));
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException("Can't read JDBC metadata from result set", e).setFatal();
        }
    }

    /**
     * Method to add a property for the index.
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
     * Two <tt>IndexInfo</tt> objects are considered equal if their catalog, schema, table, and 
     * column names AND index name are all equal.
     * @param obj the reference object with which to compare
     * @return Whether they are equal
     */
    public final boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof IndexInfo))
        {
            return false;
        }

        IndexInfo other = (IndexInfo)obj;
        String tableCat1 = (String)getProperty("table_cat");
        String tableSch1 = (String)getProperty("table_schema");
        String tableName1 = (String)getProperty("table_name");
        String columnName1 = (String)getProperty("column_name");
        String indexName1 = (String)getProperty("index_name");
        String tableCat2 = (String)other.getProperty("table_cat");
        String tableSch2 = (String)other.getProperty("table_schema");
        String tableName2 = (String)other.getProperty("table_name");
        String columnName2 = (String)other.getProperty("column_name");
        String indexName2 = (String)other.getProperty("index_name");

        boolean equals = (tableCat1 == null ? tableCat2 == null : tableCat1.equals(tableCat2)) &&
            (tableSch1 == null ? tableSch2 == null : tableSch1.equals(tableSch2)) &&
            (tableName1 == null ? tableName2 == null : tableName1.equals(tableName2)) &&
            (columnName1 == null ? columnName2 == null : columnName1.equals(columnName2)) &&
            (indexName1 == null ? indexName2 == null : indexName1.equals(indexName2));
        return equals;
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
        StringBuilder str= new StringBuilder();
        str.append(this.getClass().getName() + "\n");
        str.append("  tableCat        = " + getProperty("table_cat") + "\n");
        str.append("  tableSchem      = " + getProperty("table_schema") + "\n");
        str.append("  tableName       = " + getProperty("table_name") + "\n");
        str.append("  columnName      = " + getProperty("column_name") + "\n");
        str.append("  nonUnique       = " + getProperty("non_unique") + "\n");
        str.append("  ordinalPosition = " + getProperty("ordinal_position") + "\n");
        str.append("  indexName       = " + getProperty("index_name") + "\n");
        return str.toString();
    }
}