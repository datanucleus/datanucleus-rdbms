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
import java.sql.Types;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.schema.StoreSchemaData;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Representation of SQL type information in the datastore.
 */
public class SQLTypeInfo implements StoreSchemaData
{
    /** Whether this originates from the JDBC Driver */
    protected boolean fromJdbcDriver = true;

    /** The RDBMS-specific name for this data type. */
    protected String typeName;

    /** The JDBC data type number of this data type (see java.sql.Types). */
    protected short dataType;

    /** The maximum precision/length allowed for this data type. */
    protected int precision;

    /** The prefix used to quote a literal of this data type; may be <tt>null</tt>. */
    protected String literalPrefix;

    /** The suffix used to quote a literal of this data type; may be <tt>null</tt>. */
    protected String literalSuffix;

    /** Indicates the parameters used in defining columns of this type. */
    protected String createParams;

    /** Indicates whether null values are allowed for this data type. */
    protected int nullable;

    /** Whether the data type is case-sensitive in comparisons. */
    protected boolean caseSensitive;

    /** The searchability of this data type in terms of the kinds of SQL WHERE clauses that are allowed. */
    protected short searchable;

    /** <tt>true</tt> indicates the type is unsigned, <tt>false</tt> otherwise. */
    protected boolean unsignedAttribute;

    /** Whether the type can be assigned a fixed scale value, such as for decimal or currency types. */
    protected boolean fixedPrecScale;

    /** Whether the type automatically increments for each new row inserted. */
    protected boolean autoIncrement;

    /** Localized version of the DBMS-specific type name of this data type. */
    protected String localTypeName;

    /** The minimum supported scale value for this data type. */
    protected short minimumScale;

    /** The maximum supported scale value for this data type. */
    protected short maximumScale;

    /** Indicates the numeric radix of this data type, which is usually 2 or 10. */
    protected int numPrecRadix;

    /** Whether the type allows specification of the precision in parentheses after the type name. */
    protected boolean allowsPrecisionSpec = true;

    /** Hashcode. Set on first use. */
    private int hash = 0;

    public SQLTypeInfo(String typeName,
            short dataType,
            int precision,
            String literalPrefix,
            String literalSuffix,
            String createParams,
            int nullable,
            boolean caseSensitive,
            short searchable,
            boolean unsignedAttribute,
            boolean fixedPrecScale,
            boolean autoIncrement,
            String localTypeName,
            short minimumScale,
            short maximumScale,
            int numPrecRadix)
    {
        this.fromJdbcDriver = false;
        this.typeName = typeName;
        this.dataType = dataType;
        this.precision = precision;
        this.literalPrefix = literalPrefix;
        this.literalSuffix = literalSuffix;
        this.createParams = createParams;
        this.nullable = nullable;
        this.caseSensitive = caseSensitive;
        this.searchable = searchable;
        this.unsignedAttribute = unsignedAttribute;
        this.fixedPrecScale = fixedPrecScale;
        this.autoIncrement = autoIncrement;
        this.localTypeName = localTypeName;
        this.minimumScale = minimumScale;
        this.maximumScale = maximumScale;
        this.numPrecRadix = numPrecRadix;
    }

    /**
     * Constructor to create a type info definition for the current row of the passed ResultSet.
     * @param rs ResultSet (from DatabaseMetaData.getTypeInfo() for example).
     * @throws NucleusDataStoreException Thrown if an error occurs getting the information
     */
    public SQLTypeInfo(ResultSet rs)
    {
        try
        {
            typeName = rs.getString(1);
            dataType = rs.getShort(2);
            precision = (int)rs.getLong(3);
            literalPrefix = rs.getString(4);
            literalSuffix = rs.getString(5);
            createParams = rs.getString(6);
            nullable = rs.getInt(7);
            caseSensitive = rs.getBoolean(8);
            searchable = rs.getShort(9);
            unsignedAttribute = rs.getBoolean(10);
            fixedPrecScale = rs.getBoolean(11);
            autoIncrement = rs.getBoolean(12);
            localTypeName = rs.getString(13);
            minimumScale = rs.getShort(14);
            maximumScale = rs.getShort(15);
            numPrecRadix = rs.getInt(18);
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

    /**
     * Indicates whether some object is "equal to" this one. Two <tt>SQLTypeInfo</tt> are considered
     * equal if their type name and data type properties are equal.
     * @param obj the reference object with which to compare
     * @return  <tt>true</tt> if this object is equal to the obj argument; <tt>false</tt> otherwise.
     */
    public final boolean equals(Object obj)
    {
        if (!(obj instanceof SQLTypeInfo))
        {
            return false;
        }

        SQLTypeInfo other = (SQLTypeInfo)obj;
        return getTypeName().equals(other.getTypeName()) && getDataType() == other.getDataType();
    }

    /**
     * Returns a hash code value for this object.
     * @return  a hash code value for this object.
     */
    public final int hashCode()
    {
        if (hash == 0)
        {
            hash = getTypeName().hashCode() ^ getDataType();
        }
        return hash;
    }

    /**
     * Returns the string representation of this object.
     * @return string representation of this object.
     */
    public String toString()
    {
        return toString("");
    }

    /**
     * Returns the string representation of this object.
     * @param indent The indent for each line of the output
     * @return string representation of this object.
     */
    public String toString(String indent)
    {
        StringBuilder str = new StringBuilder(indent).append("SQLTypeInfo : ").append(fromJdbcDriver ? "[JDBC-DRIVER]" : "[DATANUCLEUS]").append("\n");
        str.append(indent + "  ").append("type : name = " + getTypeName())
            .append(", ").append("jdbcId = " + getDataType())
            .append(", ").append("localName = " + getLocalTypeName())
            .append(", ").append("createParams = " + getCreateParams())
            .append("\n");
        str.append(indent + "  ").append("precision = " + getPrecision())
            .append(", ").append("allowsSpec = " + isAllowsPrecisionSpec())
            .append(", ").append("numPrecRadix = " + getNumPrecRadix())
            .append("\n");
        str.append(indent + "  ").append("scale : min = " + getMinimumScale())
            .append(", ").append("max = " + getMaximumScale())
            .append(", ").append("fixedPrec = " + isFixedPrecScale())
            .append("\n");
        str.append(indent + "  ").append("literals : prefix = " + getLiteralPrefix())
            .append(", ").append("suffix = " + getLiteralSuffix())
            .append("\n");
        str.append(indent + "  ").append("nullable = " + getNullable())
            .append(", ").append("caseSensitive = " + isCaseSensitive())
            .append(", ").append("searchable = " + getSearchable())
            .append(", ").append("unsigned = " + isUnsignedAttribute())
            .append(", ").append("autoIncrement = " + isAutoIncrement())
            .append("\n");
        return str.toString();
    }

    /**
     * Convenience method for returning if this type is compatible with the provided column.
     * Compares the data type of each record, and returns true if the types are equivalent. 
     * For example one could be VARCHAR, and the other LONGVARCHAR so they both store string data, and hence they are compatible.
     * @param colInfo The column
     * @return Whether they are considered compatible
     */
    public boolean isCompatibleWith(RDBMSColumnInfo colInfo)
    {
        int expected = getDataType();
        int actual = colInfo.getDataType();

        if (actual == Types.OTHER)
        {
            NucleusLogger.DATASTORE.warn(Localiser.msg("020191", actual));
            return true;
        }
        
        switch (expected)
        {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return isIntegerType(actual);

            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return isFloatingType(actual);

            case Types.NUMERIC:
            case Types.DECIMAL:
                return isNumericType(actual);

            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return isCharacterType(actual);

            case Types.TIMESTAMP:
            case Types.DATE:
            case Types.TIME:
                return isDateType(actual);
    
            case Types.OTHER:
            case Types.BIT:
            case Types.CHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.NULL:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.REF:
            default:
                return expected == actual;
        }
    }

    /**
     * Verify if it's TINYINT, SMALLINT, INTEGER, or BIGINT type.
     * @param type The type
     * @return Whether the type is of an integer type
     */
    private static boolean isIntegerType(int type)
    {
        switch (type)
        {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return true;
            default:
                return isNumericType(type);
        }
    }

    /**
     * Verify if it's DATE, TIME, TIMESTAMP type.
     * @param type The type
     * @return Whether the type is of a date type
     */
    private static boolean isDateType(int type)
    {
        switch (type)
        {
            case Types.TIMESTAMP:
            case Types.TIME:
            case Types.DATE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Verify if it's FLOAT, REAL or DOUBLE.
     * @param type The type
     * @return Whether the type is of a floating point type
     */
    private static boolean isFloatingType(int type)
    {
        switch (type)
        {
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return true;
            default:
                return isNumericType(type);
        }
    }

    /**
     * Verify if it's NUMERIC or DECIMAL.
     * @param type The type
     * @return Whether the type is of a numeric type
     */
    private static boolean isNumericType(int type)
    {
        switch (type)
        {
            case Types.NUMERIC:
            case Types.DECIMAL:
                return true;
            default:
                return false;
        }
    }

    /**
     * Verify if it's LONGVARCHAR or VARCHAR.
     * @param type The type
     * @return Whether the type is of a character type
     */
    private static boolean isCharacterType(int type)
    {
        switch (type)
        {
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return true;
            default:
                return false;
        }
    }

    public void setTypeName(String typeName)
    {
        this.typeName = typeName;
    }

    public String getTypeName()
    {
        return typeName;
    }

    public short getDataType()
    {
        return dataType;
    }

    public int getPrecision()
    {
        return precision;
    }

    public String getLiteralPrefix()
    {
        return literalPrefix;
    }

    public String getLiteralSuffix()
    {
        return literalSuffix;
    }

    public String getCreateParams()
    {
        return createParams;
    }

    public int getNullable()
    {
        return nullable;
    }

    public boolean isCaseSensitive()
    {
        return caseSensitive;
    }

    public short getSearchable()
    {
        return searchable;
    }

    public boolean isUnsignedAttribute()
    {
        return unsignedAttribute;
    }

    public boolean isFixedPrecScale()
    {
        return fixedPrecScale;
    }

    public boolean isAutoIncrement()
    {
        return autoIncrement;
    }

    public void setLocalTypeName(String localTypeName)
    {
        this.localTypeName = localTypeName;
    }

    public String getLocalTypeName()
    {
        return localTypeName;
    }

    public short getMinimumScale()
    {
        return minimumScale;
    }

    public short getMaximumScale()
    {
        return maximumScale;
    }

    public int getNumPrecRadix()
    {
        return numPrecRadix;
    }

    public void setAllowsPrecisionSpec(boolean allowsPrecisionSpec)
    {
        this.allowsPrecisionSpec = allowsPrecisionSpec;
    }

    public boolean isAllowsPrecisionSpec()
    {
        return allowsPrecisionSpec;
    }
}