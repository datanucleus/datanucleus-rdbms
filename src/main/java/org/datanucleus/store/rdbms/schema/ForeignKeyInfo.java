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
 * Represents the metadata of a specific foreign key column. 
 * Supports the following properties.
 * <ul>
 * <li>pk_table_cat</li>
 * <li>pk_table_schem</li>
 * <li>pk_table_name</li>
 * <li>pk_column_name</li>
 * <li>fk_table_cat</li>
 * <li>fk_table_schem</li>
 * <li>fk_table_name</li>
 * <li>fk_column_name</li>
 * <li>key_seq</li>
 * <li>update_rule</li>
 * <li>delete_rule</li>
 * <li>pk_name</li>
 * <li>fk_name</li>
 * <li>deferrability</li>
 * </ul>
 */
public class ForeignKeyInfo implements StoreSchemaData
{
    /** Properties of the foreign-key. */
    Map<String, Object> properties = new HashMap<>();

    /** Hashcode. Set on first use. */
    private int hash = 0;

    /**
     * Constructs a foreign key information object from the current row of the given result set.
     * The {@link ResultSet} object passed must have been obtained from a call
     * to java.sql.DatabaseMetaData.getImportedKeys() or java.sql.DatabaseMetaData.getImportedKeys().
     * @param rs The result set returned from java.sql.DatabaseMetaData.getImportedKeys() or
     *           java.sql.DatabaseMetaData.getExportedKeys().
     * @exception NucleusDataStoreException if an exception is thrown upon retrieval
     */
    public ForeignKeyInfo(ResultSet rs)
    {
        try
        {
            addProperty("pk_table_cat", rs.getString(1));
            addProperty("pk_table_schem", rs.getString(2));
            addProperty("pk_table_name", rs.getString(3));
            addProperty("pk_column_name", rs.getString(4));
            addProperty("fk_table_cat", rs.getString(5));
            addProperty("fk_table_schem", rs.getString(6));
            addProperty("fk_table_name", rs.getString(7));
            addProperty("fk_column_name", rs.getString(8));
            addProperty("key_seq", Short.valueOf(rs.getShort(9)));
            addProperty("update_rule", Short.valueOf(rs.getShort(10)));
            addProperty("delete_rule", Short.valueOf(rs.getShort(11)));
            addProperty("fk_name", rs.getString(12));
            addProperty("pk_name", rs.getString(13));
            addProperty("deferrability", Short.valueOf(rs.getShort(14)));
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException("Can't read JDBC metadata from result set", e).setFatal();
        }
    }

    /**
     * Method to add a property for the type.
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
     * Two <i>ForeignKeyInfo</i> objects are considered equal if their
     * catalog, schema, table, and column names, both primary and foreign, are
     * all equal.
     *
     * @param   obj     the reference object with which to compare
     *
     * @return  <i>true</i> if this object is equal to the obj argument;
     *          <i>false</i> otherwise.
     */
    public final boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (!(obj instanceof ForeignKeyInfo))
        {
            return false;
        }

        ForeignKeyInfo other = (ForeignKeyInfo)obj;
        String pkTableCat1 = (String)getProperty("pk_table_cat");
        String pkTableSch1 = (String)getProperty("pk_table_schema");
        String pkTableName1 = (String)getProperty("pk_table_name");
        String pkColumnName1 = (String)getProperty("pk_column_name");
        String fkTableCat1 = (String)getProperty("fk_table_cat");
        String fkTableSch1 = (String)getProperty("fk_table_schema");
        String fkTableName1 = (String)getProperty("fk_table_name");
        String fkColumnName1 = (String)getProperty("fk_column_name");
        String pkName1 = (String)getProperty("pk_name");
        String fkName1 = (String)getProperty("fk_name");
        String pkTableCat2 = (String)other.getProperty("pk_table_cat");
        String pkTableSch2 = (String)other.getProperty("pk_table_schema");
        String pkTableName2 = (String)other.getProperty("pk_table_name");
        String pkColumnName2 = (String)other.getProperty("pk_column_name");
        String fkTableCat2 = (String)other.getProperty("fk_table_cat");
        String fkTableSch2 = (String)other.getProperty("fk_table_schema");
        String fkTableName2 = (String)other.getProperty("fk_table_name");
        String fkColumnName2 = (String)other.getProperty("fk_column_name");
        String pkName2 = (String)other.getProperty("pk_name");
        String fkName2 = (String)other.getProperty("fk_name");

        return (pkTableCat1 == null ? pkTableCat2 == null : pkTableCat1.equals(pkTableCat2)) &&
            (pkTableSch1 == null ? pkTableSch2 == null : pkTableSch1.equals(pkTableSch2)) &&
            pkTableName1.equals(pkTableName2) &&
            pkColumnName1.equals(pkColumnName2) &&
            (fkTableCat1 == null ? fkTableCat2 == null : fkTableCat1.equals(fkTableCat2)) &&
            (fkTableSch1 == null ? fkTableSch2 == null : fkTableSch1.equals(fkTableSch2)) &&
            fkTableName1.equals(fkTableName2) &&
            fkColumnName1.equals(fkColumnName2) &&
            (fkName1 == null ? fkName2 == null : fkName1.equals(fkName2)) &&
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
            String pkTableCat = (String)getProperty("pk_table_cat");
            String pkTableSch = (String)getProperty("pk_table_schema");
            String pkTableName = (String)getProperty("pk_table_name");
            String pkColumnName = (String)getProperty("pk_column_name");
            String fkTableCat = (String)getProperty("fk_table_cat");
            String fkTableSch = (String)getProperty("fk_table_schema");
            String fkTableName = (String)getProperty("fk_table_name");
            String fkColumnName = (String)getProperty("fk_column_name");
            hash = (pkTableCat == null ? 0 : pkTableCat.hashCode()) ^
                    (pkTableSch == null ? 0 : pkTableSch.hashCode()) ^
                    pkTableName.hashCode() ^
                    pkColumnName.hashCode() ^
                    (fkTableCat   == null ? 0 : fkTableCat.hashCode()) ^
                    (fkTableSch == null ? 0 : fkTableSch.hashCode()) ^
                    fkTableName.hashCode() ^
                    fkColumnName.hashCode();
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
        str.append("  pkTableCat    = " + getProperty("pk_table_cat") + "\n");
        str.append("  pkTableSchem  = " + getProperty("pk_table_schema") + "\n");
        str.append("  pkTableName   = " + getProperty("pk_table_name") + "\n");
        str.append("  pkColumnName  = " + getProperty("pk_column_name") + "\n");
        str.append("  fkTableCat    = " + getProperty("fk_table_cat") + "\n");
        str.append("  fkTableSchem  = " + getProperty("fk_table_schema") + "\n");
        str.append("  fkTableName   = " + getProperty("fk_table_name") + "\n");
        str.append("  fkColumnName  = " + getProperty("fk_column_name") + "\n");
        str.append("  keySeq        = " + getProperty("key_seq") + "\n");
        str.append("  updateRule    = " + getProperty("update_rule") + "\n");
        str.append("  deleteRule    = " + getProperty("delete_rule") + "\n");
        str.append("  fkName        = " + getProperty("fk_name") + "\n");
        str.append("  pkName        = " + getProperty("pk_name") + "\n");
        str.append("  deferrability = " + getProperty("deferrability") + "\n");
        return str.toString();
    }
}