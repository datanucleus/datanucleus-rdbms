/**********************************************************************
Copyright (c) 2006 Andy Jefferson and others. All rights reserved. 
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
package org.datanucleus.store.rdbms.mapping.column;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.imageio.ImageIO;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.TypeConverterMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.types.converters.ArrayConversionHelper;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.Localiser;

/**
 * Mapping of a large binary (e.g BLOB, LONGVARBINARY) column.
 * A large binary column can be treated in two ways in terms of storage and retrieval.
 * <ul>
 * <li>Serialise the field into the large binary using ObjectOutputStream, and deserialise it back using ObjectInputStream - i.e Java serialisation</li>
 * <li>Store the field using a byte[] stream, and retrieve it in the same way.</li>
 * </ul>
 */
public abstract class AbstractLargeBinaryColumnMapping extends AbstractColumnMapping
{
    /**
     * Constructor.
     * @param mapping Java type mapping
     * @param storeMgr Store Manager
     * @param col Column
     */
    public AbstractLargeBinaryColumnMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
        super(storeMgr, mapping);
        column = col;
        initialize();
    }

    /**
     * Initialisation method, initialising the type info.
     */
    protected void initialize()
    {
        initTypeInfo();
    }

    /**
     * Method to store a field into a "large varbinary" column.
     * @param ps The PreparedStatement
     * @param param Parameter position in the statement
     * @param value The value to store
     */
    public void setObject(PreparedStatement ps, int param, Object value)
    {
        if (value == null)
        {
            try
            {
                if (useDefaultWhenNull())
                {
                    ps.setBytes(param, column.getDefaultValue().toString().trim().getBytes());
                }
                else
                {
                    ps.setNull(param, getJDBCType());
                }
            }
            catch (SQLException sqle)
            {
                throw new NucleusDataStoreException(Localiser.msg("055001", "Object", "" + param, column, sqle.getMessage()), sqle);
            }
        }
        else
        {
            try
            {
                // Use Java serialisation, else byte-streaming, and if not determined then Java serialisation
                if (getJavaTypeMapping().isSerialised())
                {
                    // Serialised field so just perform basic Java serialisation for retrieval
                    if (!(value instanceof Serializable))
                    {
                        throw new NucleusDataStoreException(Localiser.msg("055005", value.getClass().getName()));
                    }
                    BlobImpl b = new BlobImpl(value);
                    ps.setBytes(param, b.getBytes(0, (int) b.length()));
                }
                else if (value instanceof boolean[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromBooleanArray((boolean[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof char[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromCharArray((char[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof double[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromDoubleArray((double[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof float[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromFloatArray((float[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof int[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromIntArray((int[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof long[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromLongArray((long[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof short[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromShortArray((short[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof Boolean[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromBooleanObjectArray((Boolean[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof Byte[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromByteObjectArray((Byte[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof Character[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromCharObjectArray((Character[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof Double[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromDoubleObjectArray((Double[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof Float[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromFloatObjectArray((Float[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof Integer[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromIntObjectArray((Integer[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof Long[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromLongObjectArray((Long[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof Short[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromShortObjectArray((Short[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof BigDecimal[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromBigDecimalArray((BigDecimal[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof BigInteger[])
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromBigIntegerArray((BigInteger[]) value);
                    ps.setBytes(param, data);
                }
                else if (value instanceof byte[])
                {
                    ps.setBytes(param, (byte[]) value);
                }
                else if (value instanceof java.util.BitSet)
                {
                    byte[] data = ArrayConversionHelper.getByteArrayFromBooleanArray(ArrayConversionHelper.getBooleanArrayFromBitSet((java.util.BitSet) value));
                    ps.setBytes(param, data);
                }
                else if (value instanceof java.awt.image.BufferedImage)
                {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
                    ImageIO.write((java.awt.image.BufferedImage) value, "jpg", baos);
                    byte[] buffer = baos.toByteArray();
                    baos.close();
                    ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
                    ps.setBytes(param, buffer);
                    bais.close();
                }
                else
                {
                    // Fall back to just perform Java serialisation for storage
                    if (!(value instanceof Serializable))
                    {
                        throw new NucleusDataStoreException(Localiser.msg("055005", value.getClass().getName()));
                    }
                    BlobImpl b = new BlobImpl(value);
                    ps.setBytes(param, b.getBytes(0, (int) b.length()));
                }
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(Localiser.msg("055001", "Object", "" + value, column, e.getMessage()), e);
            }
        }
    }

    protected Object getObjectForBytes(byte[] bytes, int param)
    {
        String typeName = getJavaTypeMapping().getType();
        if (getJavaTypeMapping() instanceof TypeConverterMapping)
        {
            // Using TypeConverterMapping so use the datastore type for the converter
            TypeConverter conv = ((TypeConverterMapping)getJavaTypeMapping()).getTypeConverter();
            Class datastoreType = storeMgr.getNucleusContext().getTypeManager().getDatastoreTypeForTypeConverter(conv, getJavaTypeMapping().getJavaType());
            typeName = datastoreType.getName();
        }

        // Use Java serialisation, else byte-streaming, and if not determined then Java serialisation
        if (getJavaTypeMapping().isSerialised())
        {
            // Serialised field so just perform basic Java deserialisation for retrieval
            try
            {
                BlobImpl blob = new BlobImpl(bytes);
                return blob.getObject();
            }
            catch (SQLException sqle)
            {
                // Impossible (JDK 1.6 use of free())
                return null;
            }
        }
        else if (typeName.equals(ClassNameConstants.BOOLEAN_ARRAY))
        {
            return ArrayConversionHelper.getBooleanArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.BYTE_ARRAY))
        {
            return bytes;
        }
        else if (typeName.equals(ClassNameConstants.CHAR_ARRAY))
        {
            return ArrayConversionHelper.getCharArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.DOUBLE_ARRAY))
        {
            return ArrayConversionHelper.getDoubleArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.FLOAT_ARRAY))
        {
            return ArrayConversionHelper.getFloatArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.INT_ARRAY))
        {
            return ArrayConversionHelper.getIntArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.LONG_ARRAY))
        {
            return ArrayConversionHelper.getLongArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.SHORT_ARRAY))
        {
            return ArrayConversionHelper.getShortArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.JAVA_LANG_BOOLEAN_ARRAY))
        {
            return ArrayConversionHelper.getBooleanObjectArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.JAVA_LANG_BYTE_ARRAY))
        {
            return ArrayConversionHelper.getByteObjectArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.JAVA_LANG_CHARACTER_ARRAY))
        {
            return ArrayConversionHelper.getCharObjectArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.JAVA_LANG_DOUBLE_ARRAY))
        {
            return ArrayConversionHelper.getDoubleObjectArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.JAVA_LANG_FLOAT_ARRAY))
        {
            return ArrayConversionHelper.getFloatObjectArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.JAVA_LANG_INTEGER_ARRAY))
        {
            return ArrayConversionHelper.getIntObjectArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.JAVA_LANG_LONG_ARRAY))
        {
            return ArrayConversionHelper.getLongObjectArrayFromByteArray(bytes);
        }
        else if (typeName.equals(ClassNameConstants.JAVA_LANG_SHORT_ARRAY))
        {
            return ArrayConversionHelper.getShortObjectArrayFromByteArray(bytes);
        }
        else if (typeName.equals(BigDecimal[].class.getName()))
        {
            return ArrayConversionHelper.getBigDecimalArrayFromByteArray(bytes);
        }
        else if (typeName.equals(BigInteger[].class.getName()))
        {
            return ArrayConversionHelper.getBigIntegerArrayFromByteArray(bytes);
        }
        else if (getJavaTypeMapping().getJavaType() != null && getJavaTypeMapping().getJavaType().getName().equals("java.util.BitSet"))
        {
            return ArrayConversionHelper.getBitSetFromBooleanArray(ArrayConversionHelper.getBooleanArrayFromByteArray(bytes));
        }
        else if (getJavaTypeMapping().getJavaType() != null && getJavaTypeMapping().getJavaType().getName().equals("java.awt.image.BufferedImage"))
        {
            try
            {
                return ImageIO.read(new ByteArrayInputStream(bytes));
            }
            catch (IOException e)
            {
                throw new NucleusDataStoreException(Localiser.msg("055002", "Object", "" + param, column, e.getMessage()), e);
            }
        }
        else
        {
            // Fallback to just perform basic Java deserialisation for retrieval
            try
            {
                BlobImpl blob = new BlobImpl(bytes);
                return blob.getObject();
            }
            catch (SQLException sqle)
            {
                // Impossible (JDK 1.6 use of free())
                return null;
            }
        }
    }

    /**
     * Method to retrieve the object from the large binary column.
     * @param rs The ResultSet
     * @param param The parameter position
     * @return The object
     */
    public Object getObject(ResultSet rs, int param)
    {
        byte[] bytes = null;
        try
        {
            // Retrieve the bytes of the object
            bytes = rs.getBytes(param);
        }
        catch (SQLException sqle)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "Object", "" + param, column, sqle.getMessage()), sqle);
        }
        if (bytes == null)
        {
            return null;
        }

        return getObjectForBytes(bytes, param);
    }

    /**
     * Cater for serialisation of Strings.
     * @param ps PreparedStatement
     * @param exprIndex param indexes
     * @param value The value of the String
     */
    public void setString(PreparedStatement ps, int exprIndex, String value)
    {
        // Delegate to the setObject method
        setObject(ps, exprIndex, value);
    }

    /**
     * Accessor for String value when serialised.
     * @param resultSet ResultSet
     * @param exprIndex param indexes
     * @return The String value
     */
    public String getString(ResultSet resultSet, int exprIndex)
    {
        return (String)getObject(resultSet, exprIndex);
    }
}