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
 * Representation of JDBC type information in the datastore.
 * Each JDBC type info has a map of SQL type info for this JDBC type.
 * Has the property "jdbc_type" as a Short of the java.sql.Types value.
 */
public class JDBCTypeInfo implements MapStoreSchemaData
{
    /** Hashcode. Set on first use. */
    private int hash = 0;

    /** Properties of the JDBC type. */
    Map properties = new HashMap();

    /** SQL types for this JDBC type, keyed by (SQL) type name. */
    Map sqlTypes = new HashMap();

    public JDBCTypeInfo(short type)
    {
        addProperty("jdbc_type", Short.valueOf(type));
    }

    /**
     * Mutator for the parent component.
     * @param parent Parent component
     */
    public void setParent(StoreSchemaData parent)
    {
        // Nothing to do as we have no parent
    }

    /**
     * Accessor for the parent component.
     * @return null
     */
    public StoreSchemaData getParent()
    {
        return null; // No parent
    }

    /**
     * Method to add a property for the type.
     * @param name Name of property
     * @param value Its value
     */
    public void addProperty(String name, Object value)
    {
        properties.put(name, value);
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
     * Add a SQL type for this JDBC type.
     * @param child The SQL type
     */
    public void addChild(StoreSchemaData child)
    {
        SQLTypeInfo sqlType = (SQLTypeInfo)child;
        sqlTypes.put(sqlType.getTypeName(), sqlType);
        if (sqlTypes.size() == 1)
        {
            sqlTypes.put("DEFAULT", sqlType); // Set reference to the default
        }
    }

    /**
     * Method to remove all children.
     */
    public void clearChildren()
    {
        sqlTypes.clear();
    }

    /**
     * Accessor for the SQL type with this type name (if supported for this JDBC type).
     * @param key type name
     * @return the SQL type
     */
    public StoreSchemaData getChild(String key)
    {
        return (StoreSchemaData)sqlTypes.get(key);
    }

    /**
     * Accessor for the SQL types map for this JDBC type, keyed by the type name.
     * @return Map of SQL types
     */
    public Map getChildren()
    {
        return sqlTypes;
    }

    public int getNumberOfChildren()
    {
        return sqlTypes.size();
    }

    /**
     * Indicates whether some object is "equal to" this one. Two <tt>JDBCTypeInfo</tt> are considered
     * equal if their jdbc type is the same.
     * @param obj the reference object with which to compare
     * @return  <tt>true</tt> if this object is equal to the obj argument; <tt>false</tt> otherwise.
     */
    public final boolean equals(Object obj)
    {
        if (!(obj instanceof JDBCTypeInfo))
        {
            return false;
        }

        JDBCTypeInfo other = (JDBCTypeInfo)obj;
        short jdbcType1 = ((Short)getProperty("jdbc_type")).shortValue();
        short jdbcType2 = ((Short)other.getProperty("jdbc_type")).shortValue();

        return (jdbcType1 == jdbcType2);
    }

    /**
     * Returns a hash code value for this object.
     * @return  a hash code value for this object.
     */
    public final int hashCode()
    {
        if (hash == 0)
        {
            short jdbcType1 = ((Short)getProperty("jdbc_type")).shortValue();
            hash = jdbcType1;
        }
        return hash;
    }

    /**
     * Returns the string representation of this object.
     * @return  string representation of this object.
     */
    public String toString()
    {
        StringBuffer str = new StringBuffer("JDBCTypeInfo : ");
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
        str.append(", numSQLTypes=" + sqlTypes.size());
        return str.toString();
    }
}