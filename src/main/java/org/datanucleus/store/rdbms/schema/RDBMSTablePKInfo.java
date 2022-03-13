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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.store.schema.ListStoreSchemaData;
import org.datanucleus.store.schema.StoreSchemaData;

/**
 * Representation of PK information for a table in the datastore.
 * Supports the properties :-
 * <ul>
 * <li><b>table_cat</b> : catalog for this table (or null if not defined/supported)</li>
 * <li><b>table_sch</b> : schema for this table (or null if not defined/supported)</li>
 * <li><b>table_name</b> : name of the table</li>
 * </ul>
 */
public class RDBMSTablePKInfo implements ListStoreSchemaData
{
    /** Hashcode. Set on first use. */
    private int hash = 0;

    /** Properties of the table. */
    Map<String, Object> properties = new HashMap<>();

    /** FK information for this table. */
    List<StoreSchemaData> pks = new ArrayList<>();

    public RDBMSTablePKInfo()
    {
    }

    /**
     * Constructor taking just the catalog, schema and table name directly.
     * @param catalog Catalog containing the table
     * @param schema Schema containing the table
     * @param table The table name
     */
    public RDBMSTablePKInfo(String catalog, String schema, String table)
    {
        addProperty("table_cat", catalog);
        addProperty("table_schem", schema);
        addProperty("table_name", table);
    }

    /**
     * Method to add another PK col to the table schema.
     * @param child Column
     */
    public void addChild(StoreSchemaData child)
    {
        pks.add(child);
    }

    /**
     * Method to remove all children.
     */
    public void clearChildren()
    {
        pks.clear();
    }

    /**
     * Accessor for the PK column at the position.
     * @param position Index
     * @return PK column at the position
     */
    public StoreSchemaData getChild(int position)
    {
        return pks.get(position);
    }

    /**
     * Accessor for the primary keys
     * @return PK information
     */
    public List<StoreSchemaData> getChildren()
    {
        return pks;
    }

    /**
     * Accessor for the number of pk cols in the schema for this table.
     * @return Number of pk cols
     */
    public int getNumberOfChildren()
    {
        return pks.size();
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
     * Indicates whether some object is "equal to" this one. Two <i>RDBMSTableInfo</i> are considered
     * equal if their catalog, schema, table are all equal.
     * @param obj the reference object with which to compare
     * @return  <i>true</i> if this object is equal to the obj argument; <i>false</i> otherwise.
     */
    public final boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (!(obj instanceof RDBMSTablePKInfo))
        {
            return false;
        }

        RDBMSTablePKInfo other = (RDBMSTablePKInfo)obj;
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
        StringBuilder str = new StringBuilder("RDBMSTablePKInfo : ");
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
        str.append(", numPKs=" + pks.size());
        return str.toString();
    }
}