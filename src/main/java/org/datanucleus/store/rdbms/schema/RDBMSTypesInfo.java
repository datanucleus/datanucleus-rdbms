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

import org.datanucleus.store.schema.MapStoreSchemaData;
import org.datanucleus.store.schema.StoreSchemaData;

/**
 * Representation of types information in the datastore.
 * Contains a map of child JDBCTypeInfo objects, which turn contain child SQLTypeInfo objects.
 */
public class RDBMSTypesInfo implements MapStoreSchemaData
{
    /** Properties of the types. */
    Map<String, Object> properties = new HashMap<>();

    /** JDBC Types information, keyed by type number (java.sql.Types). */
    Map<String, StoreSchemaData> jdbcTypes = new HashMap<>();

    public RDBMSTypesInfo()
    {
    }

    /**
     * Method to add another type to the schema.
     * @param type Type
     */
    public void addChild(StoreSchemaData type)
    {
        // Keyed by jdbc-type number as String
        jdbcTypes.put("" + type.getProperty("jdbc_type"), type);
    }

    /**
     * Method to remove all children.
     */
    public void clearChildren()
    {
        jdbcTypes.clear();
    }

    /**
     * Accessor for the JDBC type for this type.
     * @param key JDBC type to retrieve
     * @return Type with this key
     */
    public StoreSchemaData getChild(String key)
    {
        return jdbcTypes.get(key);
    }

    /**
     * Accessor for the JDBC types.
     * @return Types
     */
    public Map<String, StoreSchemaData> getChildren()
    {
        return jdbcTypes;
    }

    /**
     * Accessor for the number of JDBC types in the schema for this schema.
     * @return Number of JDBC types
     */
    public int getNumberOfChildren()
    {
        return jdbcTypes.size();
    }

    /**
     * Method to add a property for the types.
     * @param name Name of property
     * @param value Its value
     */
    public void addProperty(String name, Object value)
    {
        properties.put(name, value);
    }

    /**
     * Accessor for a property of the types.
     * @param name Name of the property
     * @return Its value, or null if not defined
     */
    public Object getProperty(String name)
    {
        return properties.get(name);
    }

    public StoreSchemaData getParent()
    {
        // Types has no parent
        return null;
    }

    public void setParent(StoreSchemaData parent)
    {
        // Types has no parent
    }

    /**
     * Indicates whether some object is "equal to" this one.
     * @param obj the reference object with which to compare
     * @return <tt>true</tt> if this object is equal to the obj argument; <tt>false</tt> otherwise.
     */
    public final boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (!(obj instanceof RDBMSTypesInfo))
        {
            return false;
        }

        return (obj == this);
    }

    /**
     * Returns a hash code value for this object.
     * @return hash code
     */
    public final int hashCode()
    {
        return super.hashCode();
    }

    /**
     * Returns the string representation of this object.
     * @return string representation of this object.
     */
    public String toString()
    {
        StringBuilder str = new StringBuilder("RDBMSTypesInfo : ");
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
        str.append(", numJDBCTypes=" + jdbcTypes.size());
        return str.toString();
    }
}