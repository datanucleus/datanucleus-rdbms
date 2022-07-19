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
import java.util.List;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.schema.ListStoreSchemaData;
import org.datanucleus.store.schema.StoreSchemaData;

/**
 * Representation of column schema information in the datastore.
 */
public class RDBMSColumnInfo implements ListStoreSchemaData
{
    /** The table catalog, which may be <i>null</i>. */
    protected String tableCat;

    /** The table schema, which may be <i>null</i>. */
    protected String tableSchem;

    /** The table name. */
    protected String tableName;

    /** The column name. */
    protected String columnName;

    /** Indicates the JDBC (SQL) data type from {@link java.sql.Types}. */
    protected short dataType;

    /** The local type name used by the data source. */
    protected String typeName;

    /** The column size. For char/date types, this is the maximum num of chars, otherwise precision. */
    protected int columnSize;

    /** Indicates the number of fractional digits. */
    protected int decimalDigits;

    /** Indicates the radix, which is typically either 10 or 2. */
    protected int numPrecRadix;

    /** Indicates whether the column can be NULL. */
    protected int nullable;

    /** An explanatory comment on the column; may be <i>null</i>. */
    protected String remarks;

    /** The default value for the column; may be <i>null</i>. */
    protected String columnDef;

    /** Indicates the maximum number of bytes in the column (for char types only). */
    protected int charOctetLength;

    /** The index of the column in its table; the first column is 1, the second column is 2. */
    protected int ordinalPosition;

    /** Whether the column definitely doesnt allow NULL values ("NO") or possibly does ("YES"). */
    protected String isNullable;

    /** Hashcode. Set on first use. */
    private int hash = 0;

    /** Parent table schema info. */
    RDBMSTableInfo tableInfo;

    /**
     * Constructor to create a column info definition for the current row of the passed ResultSet.
     * @param rs ResultSet (from DatabaseMetaData.getColumns() for example).
     * @throws NucleusDataStoreException Thrown if an error occurs getting the information
     */
    public RDBMSColumnInfo(ResultSet rs)
    {
        try
        {
            tableCat        = rs.getString(1);
            tableSchem      = rs.getString(2);
            tableName       = rs.getString(3);
            columnName      = rs.getString(4);
            dataType        = rs.getShort(5);
            typeName        = rs.getString(6);
            columnSize      = rs.getInt(7);
            decimalDigits   = rs.getInt(9);
            numPrecRadix    = rs.getInt(10);
            nullable        = rs.getInt(11);
            remarks         = rs.getString(12);
            columnDef       = rs.getString(13);
            charOctetLength = rs.getInt(16);
            ordinalPosition = rs.getInt(17);
            isNullable      = rs.getString(18);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException("Can't read JDBC metadata from result set", e).setFatal();
        }
    }

    /**
     * Mutator for the parent component.
     * @param parent Parent component
     */
    public void setParent(StoreSchemaData parent)
    {
        this.tableInfo = (RDBMSTableInfo)parent;
    }

    /**
     * Accessor for the parent Table schema component.
     * @return Table schema
     */
    public StoreSchemaData getParent()
    {
        return tableInfo;
    }

    public void setDecimalDigits(int digits)
    {
        this.decimalDigits = digits;
    }

    public void setDataType(short type)
    {
        this.dataType = type;
    }

    public void setColumnSize(int size)
    {
        this.columnSize = size;
    }

    public void setColumnDef(String def)
    {
        this.columnDef = def;
    }

    public int getDecimalDigits()
    {
        return decimalDigits;
    }

    public String getIsNullable()
    {
        return isNullable;
    }

    public int getNullable()
    {
        return nullable;
    }

    public int getColumnSize()
    {
        return columnSize;
    }

    public short getDataType()
    {
        return dataType;
    }

    public int getNumPrecRadix()
    {
        return numPrecRadix;
    }

    public int getCharOctetLength()
    {
        return charOctetLength;
    }

    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public String getColumnDef()
    {
        return columnDef;
    }

    public String getRemarks()
    {
        return remarks;
    }

    public String getTypeName()
    {
        return typeName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String getTableCat()
    {
        return tableCat;
    }

    public String getTableSchem()
    {
        return tableSchem;
    }

    /**
     * Method to add a property for the column.
     * @param name Name of property
     * @param value Its value
     */
    public void addProperty(String name, Object value)
    {
        throw new UnsupportedOperationException("SQLTypeInfo doesnt support properties");
    }

    /**
     * Accessor for a property.
     * @param name Name of the property
     * @return Its value, or null if not defined
     */
    public Object getProperty(String name)
    {
        throw new UnsupportedOperationException("SQLTypeInfo doesnt support properties");
    }

    public void addChild(StoreSchemaData child)
    {
        // Do nothing. A column has no child components
    }

    /**
     * Method to remove all children.
     */
    public void clearChildren()
    {
        // Do nothing. A column has no child components.
    }

    public StoreSchemaData getChild(int position)
    {
        // A column has no child components
        return null;
    }

    public List<StoreSchemaData> getChildren()
    {
        // A column has no child components
        return null;
    }

    public int getNumberOfChildren()
    {
        // A column has no child components
        return 0;
    }

    /**
     * Indicates whether some object is "equal to" this one. Two <i>RDBMSColumnInfo</i> are considered
     * equal if their catalog, schema, table, and column name are all equal.
     * @param obj the reference object with which to compare
     * @return  <i>true</i> if this object is equal to the obj argument; <i>false</i> otherwise.
     */
    public final boolean equals(Object obj)
    {
        if (!(obj instanceof RDBMSColumnInfo))
        {
            return false;
        }

        RDBMSColumnInfo other = (RDBMSColumnInfo)obj;
        return (tableCat   == null ? other.tableCat   == null : tableCat.equals(other.tableCat)) &&
            (tableSchem == null ? other.tableSchem == null : tableSchem.equals(other.tableSchem)) &&
            tableName.equals(other.tableName) && columnName.equals(other.columnName);
    }

    /**
     * Returns a hash code value for this object.
     *
     * @return  a hash code value for this object.
     */
    public final int hashCode()
    {
        if (hash == 0)
        {
            hash = 
                (tableCat == null ? 0 : tableCat.hashCode()) ^
                (tableSchem == null ? 0 : tableSchem.hashCode()) ^
                tableName.hashCode() ^ columnName.hashCode();
        }
        return hash;
    }

    /**
     * Returns the string representation of this object.
     * @return  string representation of this object.
     */
    public String toString()
    {
        StringBuilder str = new StringBuilder("RDBMSColumnInfo : ");
        str.append("  tableCat        = " + tableCat + "\n");
        str.append("  tableSchem      = " + tableSchem + "\n");
        str.append("  tableName       = " + tableName + "\n");
        str.append("  columnName      = " + columnName + "\n");
        str.append("  dataType        = " + dataType + "\n");
        str.append("  typeName        = " + typeName + "\n");
        str.append("  columnSize      = " + columnSize + "\n");
        str.append("  decimalDigits   = " + decimalDigits + "\n");
        str.append("  numPrecRadix    = " + numPrecRadix + "\n");
        str.append("  nullable        = " + nullable + "\n");
        str.append("  remarks         = " + remarks + "\n");
        str.append("  columnDef       = " + columnDef + "\n");
        str.append("  charOctetLength = " + charOctetLength + "\n");
        str.append("  ordinalPosition = " + ordinalPosition + "\n");
        str.append("  isNullable      = " + isNullable + "\n");
        return str.toString();
    }
}