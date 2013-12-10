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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.schema.ListStoreSchemaData;
import org.datanucleus.store.schema.StoreSchemaData;

/**
 * Representation of table column information in the datastore.
 * Columns are stored as List-based, but also in a lookup Map keyed by the column name.
 * Supports the properties :-
 * <ul>
 * <li><b>table_key</b> : unique key under which this table is known (fully-qualified name)</li>
 * <li><b>table_cat</b> : catalog for this table (or null if not defined/supported)</li>
 * <li><b>table_sch</b> : schema for this table (or null if not defined/supported)</li>
 * <li><b>table_name</b> : name of the table</li>
 * <li><b>time</b> : time at which the information was provided</li>
 * </ul>
 */
public class RDBMSTableInfo implements ListStoreSchemaData
{
    /** Hashcode. Set on first use. */
    private int hash = 0;

    /** Properties of the table. */
    Map<String, Object> properties = new HashMap();

    /** Column information for this table. */
    List columns = new ArrayList();

    /** Map of column information keyed by the column name. */
    Map<String, StoreSchemaData> columnMapByColumnName = new HashMap();

    public RDBMSTableInfo()
    {
    }

    /**
     * Constructor taking just the catalog, schema and table name directly.
     * @param catalog Catalog containing the table
     * @param schema Schema containing the table
     * @param table The table name
     */
    public RDBMSTableInfo(String catalog, String schema, String table)
    {
        addProperty("table_cat", catalog);
        addProperty("table_schem", schema);
        addProperty("table_name", table);
    }

    /**
     * Constructor to create a table info definition for the current row of the passed ResultSet.
     * @param rs ResultSet (from DatabaseMetaData.getTables() for example).
     * @throws NucleusDataStoreException Thrown if an error occurs getting the information
     */
    public RDBMSTableInfo(ResultSet rs)
    {
        try
        {
            addProperty("table_cat", rs.getString(1));
            addProperty("table_schem", rs.getString(2));
            addProperty("table_name", rs.getString(3));
            addProperty("table_type", rs.getString(4));
            addProperty("remarks", rs.getString(5));
            if (rs.getMetaData().getColumnCount() > 5)
            {
                addProperty("type_cat", rs.getString(6));
                addProperty("type_schem", rs.getString(7));
                addProperty("type_name", rs.getString(8));
                addProperty("self_referencing_col_name", rs.getString(9));
                addProperty("ref_generation", rs.getString(10));
            }
        }
        catch (SQLException sqle)
        {
            throw new NucleusDataStoreException("Exception thrown obtaining schema table information from datastore", sqle);
        }
    }

    /**
     * Method to add another column to the table schema.
     * @param child Column
     */
    public void addChild(StoreSchemaData child)
    {
        RDBMSColumnInfo col = (RDBMSColumnInfo)child;
        columns.add(col);
        columnMapByColumnName.put(col.getColumnName(), child);
    }

    /**
     * Method to remove all children.
     */
    public void clearChildren()
    {
        columns.clear();
        columnMapByColumnName.clear();
    }

    /**
     * Accessor for the column at the position.
     * @param position Index of the column
     * @return Column at the position
     */
    public StoreSchemaData getChild(int position)
    {
        return (StoreSchemaData)columns.get(position);
    }

    /**
     * Accessor for the column with a particular name.
     * @param key Name of the column
     * @return Column information
     */
    public StoreSchemaData getChild(String key)
    {
        return columnMapByColumnName.get(key);
    }

    /**
     * Accessor for the columns
     * @return Column schema information
     */
    public List getChildren()
    {
        return columns;
    }

    /**
     * Accessor for the number of columns in the schema for this table.
     * @return Number of cols
     */
    public int getNumberOfChildren()
    {
        return columns.size();
    }

    /**
     * Method to add a property for the table.
     * @param name Name of property
     * @param value Its value
     */
    public void addProperty(String name, Object value)
    {
        properties.put(name, value);
    }

    /**
     * Accessor for a property of the table.
     * @param name Name of the property
     * @return Its value, or null if not defined
     */
    public Object getProperty(String name)
    {
        return properties.get(name);
    }

    public StoreSchemaData getParent()
    {
        // Table has no parent
        return null;
    }

    public void setParent(StoreSchemaData parent)
    {
        // Table has no parent
    }

    /**
     * Indicates whether some object is "equal to" this one. Two <tt>RDBMSTableInfo</tt> are considered
     * equal if their catalog, schema, table are all equal.
     * @param obj the reference object with which to compare
     * @return  <tt>true</tt> if this object is equal to the obj argument; <tt>false</tt> otherwise.
     */
    public final boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (!(obj instanceof RDBMSTableInfo))
        {
            return false;
        }

        RDBMSTableInfo other = (RDBMSTableInfo)obj;
        String cat1 = (String)getProperty("table_cat");
        String sch1 = (String)getProperty("table_schem");
        String name1 = (String)getProperty("table_name");
        String cat2 = (String)other.getProperty("table_cat");
        String sch2 = (String)other.getProperty("table_schem");
        String name2 = (String)other.getProperty("table_name");

        return (cat1   == null ? cat2   == null : cat1.equals(cat2)) &&
            (sch1 == null ? sch2 == null : sch1.equals(sch2)) &&
            name1.equals(name2);
    }

    /**
     * Returns a hash code value for this object.
     * @return hash code
     */
    public final int hashCode()
    {
        if (hash == 0)
        {
            String cat = (String)getProperty("table_cat");
            String sch = (String)getProperty("table_schem");
            String name = (String)getProperty("table_name");
            hash = (cat == null ? 0 : cat.hashCode()) ^ (sch == null ? 0 : sch.hashCode()) ^ name.hashCode();
        }
        return hash;
    }

    /**
     * Returns the string representation of this object.
     * @return string representation of this object.
     */
    public String toString()
    {
        StringBuffer str = new StringBuffer("RDBMSTableInfo : ");
        Iterator iter = properties.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry entry = (Map.Entry)iter.next();
            str.append(entry.getKey() + " = " + entry.getValue());
            if (iter.hasNext())
            {
                str.append(", ");
            }
        }
        str.append(", numColumns=" + columns.size());
        return str.toString();
    }
}