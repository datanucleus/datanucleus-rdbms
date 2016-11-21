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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.schema.MapStoreSchemaData;
import org.datanucleus.store.schema.StoreSchemaData;

/**
 * Representation of schema table information in the datastore.
 * Stores a map of tables, keyed by the table name.
 * Supports the following properties :-
 * <ul>
 * <li><b>catalog</b> Catalog containing the tables</li>
 * <li><b>schema</b> Schema containing the tables</li>
 * </ul>
 */
public class RDBMSSchemaInfo implements MapStoreSchemaData
{
    /** Hashcode. Set on first use. */
    private int hash = 0;

    /** Properties of the schema. */
    Map<String, Object> properties = new HashMap<>();

    /** Table information for this schema, keyed by table name. */
    Map<String, StoreSchemaData> tables = new HashMap<>();

    public RDBMSSchemaInfo(String catalog, String schema)
    {
        addProperty("catalog", catalog);
        addProperty("schema", schema);
    }

    /**
     * Method to add another table to the schema.
     * Will key the table into its Map using the table property "table_key".
     * @param data Child information (RDBMSTableInfo)
     * @throws NucleusException Thrown if the table has no property "table_key"
     */
    public void addChild(StoreSchemaData data)
    {
        RDBMSTableInfo table = (RDBMSTableInfo)data;
        String tableKey = (String)data.getProperty("table_key");
        if (tableKey == null)
        {
            // TODO Localise
            throw new NucleusException(
                "Attempt to add RDBMSTableInfo to RDBMSSchemaInfo with null table key! tableName=" + 
                data.getProperty("table_name"));
        }
        tables.put(tableKey, table);
    }

    /**
     * Method to remove all children.
     */
    public void clearChildren()
    {
        tables.clear();
    }

    /**
     * Accessor for the table with this key.
     * @param key Key of the table
     * @return Table with this key
     */
    public StoreSchemaData getChild(String key)
    {
        return tables.get(key);
    }

    /**
     * Accessor for the tables.
     * @return Tables
     */
    public Map<String, StoreSchemaData> getChildren()
    {
        return tables;
    }

    /**
     * Accessor for the number of tables in the schema for this schema.
     * @return Number of tables
     */
    public int getNumberOfChildren()
    {
        return tables.size();
    }

    /**
     * Method to add a property for the schema.
     * @param name Name of property
     * @param value Its value
     */
    public void addProperty(String name, Object value)
    {
        properties.put(name, value);
    }

    /**
     * Accessor for a property of the schema.
     * @param name Name of the property
     * @return Its value, or null if not defined
     */
    public Object getProperty(String name)
    {
        return properties.get(name);
    }

    public StoreSchemaData getParent()
    {
        // Schema has no parent
        return null;
    }

    public void setParent(StoreSchemaData parent)
    {
        // Schema has no parent
    }

    /**
     * Indicates whether some object is "equal to" this one. Two <tt>RDBMSSchemaInfo</tt> are considered
     * equal if their catalog, schema are all equal.
     * @param obj the reference object with which to compare
     * @return <tt>true</tt> if this object is equal to the obj argument; <tt>false</tt> otherwise.
     */
    public final boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (!(obj instanceof RDBMSSchemaInfo))
        {
            return false;
        }

        RDBMSSchemaInfo other = (RDBMSSchemaInfo)obj;
        String cat1 = (String)getProperty("table_cat");
        String sch1 = (String)getProperty("table_schem");
        String cat2 = (String)other.getProperty("table_cat");
        String sch2 = (String)other.getProperty("table_schem");

        return (cat1   == null ? cat2   == null : cat1.equals(cat2)) &&
            (sch1 == null ? sch2 == null : sch1.equals(sch2));
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
            hash = (cat == null ? 0 : cat.hashCode()) ^ (sch == null ? 0 : sch.hashCode());
        }
        return hash;
    }

    /**
     * Returns the string representation of this object.
     * @return string representation of this object.
     */
    public String toString()
    {
        StringBuilder str = new StringBuilder("RDBMSSchemaInfo : ");
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
        str.append(", numTables=" + tables.size());
        return str.toString();
    }
}