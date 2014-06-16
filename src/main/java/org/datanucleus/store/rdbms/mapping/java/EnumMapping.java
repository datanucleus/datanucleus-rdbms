/**********************************************************************
Copyright (c) 2005 Erik Bengtson and others. All rights reserved. 
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
2006 Andy Jefferson - moved to Core. Removed RDBMS mappings.
2007 Andy Jefferson - support roleForMember so it handles collection elements etc
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.lang.reflect.Method;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ClassNameConstants;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Mapping for JDK1.5 Enum type.
 */
public class EnumMapping extends SingleFieldMapping
{
    protected static final String ENUM_VALUE_GETTER = "enum-value-getter";
    protected static final String ENUM_GETTER_BY_VALUE = "enum-getter-by-value";

    protected String datastoreJavaType = ClassNameConstants.JAVA_LANG_STRING;

    /**
     * Initialize this JavaTypeMapping with the given DatastoreAdapter for the given FieldMetaData.
     * @param fmd FieldMetaData for the field to be mapped (if any)
     * @param table The datastore container storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     */
    public void initialize(AbstractMemberMetaData fmd, Table table, ClassLoaderResolver clr)
    {
        if (fmd != null && fmd.isSerialized())
        {
            datastoreJavaType = ClassNameConstants.JAVA_IO_SERIALIZABLE;
        }
        else if (fmd != null)
        {
            ColumnMetaData[] colmds = getColumnMetaDataForMember(fmd, roleForMember);
            if (colmds != null && colmds.length > 0)
            {
                // Check if the user requested an INTEGER based datastore type
                if (MetaDataUtils.isJdbcTypeNumeric(colmds[0].getJdbcType()))
                {
                    datastoreJavaType = ClassNameConstants.JAVA_LANG_INTEGER;
                }
            }
        }

        super.initialize(fmd, table, clr);
    }

    /**
     * Accessor for the valid values for this mapping (if any restriction is imposed).
     * @param index The index of the datastore column
     * @return The valid value(s)
     */
    public Object[] getValidValues(int index)
    {
        // this block defines check constraints based on the values of enums
        if (mmd != null)
        {
            if (mmd.getColumnMetaData() != null && 
                mmd.getColumnMetaData().length > 0 && 
                mmd.getColumnMetaData()[0].hasExtension("enum-check-constraint") &&
                mmd.getColumnMetaData()[0].getValueForExtension("enum-check-constraint").equalsIgnoreCase("true"))
            {
                try
                {
                    Enum[] values = (Enum[])mmd.getType().getMethod("values", (Class[])null).invoke((Object)null, (Object[])null);
                    if (datastoreJavaType.equals(ClassNameConstants.JAVA_LANG_STRING))
                    {
                        String[] valueStrings = new String[values.length];
                        for (int i=0;i<values.length;i++)
                        {
                            valueStrings[i] = values[i].toString();
                        }
                        return valueStrings;
                    }
                    else
                    {
                        Integer[] valueInts = new Integer[values.length];
                        for (int i=0;i<values.length;i++)
                        {
                            valueInts[i] = values[i].ordinal();
                        }
                        return valueInts;
                    }
                }
                catch (Exception e)
                {
                    NucleusLogger.PERSISTENCE.warn(StringUtils.getStringFromStackTrace(e));
                }
            }
        }
        return super.getValidValues(index);
    }

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore
     * field. This java-type must have an entry in the datastore mappings.
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForDatastoreMapping(int index)
    {
        return datastoreJavaType;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.JavaTypeMapping#getJavaType()
     */
    public Class getJavaType()
    {
        return Enum.class;
    }

    /**
     * Method to set the Enum in the datastore statement.
     * @param ec ExecutionContext
     * @param ps Statement for the datastore
     * @param exprIndex Index position(s) to set the Enum at in the statement
     * @param value The Enum value to set
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
        if (value == null)
        {
            getDatastoreMapping(0).setObject(ps, exprIndex[0], null);
        }
        else if (datastoreJavaType.equals(ClassNameConstants.JAVA_LANG_INTEGER))
        {
            if (value instanceof Enum)
            {
                int intVal = ((Enum)value).ordinal();
                String methodName = null;
                if (roleForMember == FieldRole.ROLE_FIELD)
                {
                    if (mmd != null && mmd.hasExtension(ENUM_VALUE_GETTER))
                    {
                        // Case where the user has defined their own "value" for each enum
                        methodName = mmd.getValueForExtension(ENUM_VALUE_GETTER);
                    }
                }
                else if (roleForMember == FieldRole.ROLE_COLLECTION_ELEMENT ||
                    roleForMember == FieldRole.ROLE_ARRAY_ELEMENT)
                {
                    if (mmd != null && mmd.getElementMetaData() != null && mmd.getElementMetaData().hasExtension(ENUM_VALUE_GETTER))
                    {
                        // Case where the user has defined their own "value" for each enum
                        methodName = mmd.getElementMetaData().getValueForExtension(ENUM_VALUE_GETTER);
                    }
                }
                else if (roleForMember == FieldRole.ROLE_MAP_KEY)
                {
                    if (mmd != null && mmd.getKeyMetaData() != null && mmd.getKeyMetaData().hasExtension(ENUM_VALUE_GETTER))
                    {
                        // Case where the user has defined their own "value" for each enum
                        methodName = mmd.getKeyMetaData().getValueForExtension(ENUM_VALUE_GETTER);
                    }
                }
                else if (roleForMember == FieldRole.ROLE_MAP_VALUE)
                {
                    if (mmd != null && mmd.getValueMetaData() != null && mmd.getValueMetaData().hasExtension(ENUM_VALUE_GETTER))
                    {
                        // Case where the user has defined their own "value" for each enum
                        methodName = mmd.getValueMetaData().getValueForExtension(ENUM_VALUE_GETTER);
                    }
                }

                if (methodName != null)
                {
                    String getterMethodName = mmd.getValueForExtension(ENUM_VALUE_GETTER);
                    Long longVal = getValueForEnumUsingMethod((Enum)value, getterMethodName);
                    if (longVal != null)
                    {
                        intVal = longVal.intValue();
                    }
                }
                getDatastoreMapping(0).setInt(ps, exprIndex[0], intVal);
            }
            else if (value instanceof BigInteger)
            {
                // ordinal value passed in directly (e.g ENUM_FIELD == ? in query)
                getDatastoreMapping(0).setInt(ps, exprIndex[0], ((BigInteger)value).intValue());
            }
        }
        else if (datastoreJavaType.equals(ClassNameConstants.JAVA_LANG_STRING))
        {
            String stringVal;
            if (value instanceof String)
            {
                //it may be String when there is a compare between Enums values and Strings in the JDOQL query (for example)
                stringVal = (String)value;
            }
            else
            {
                stringVal = ((Enum)value).name();
            }
            getDatastoreMapping(0).setString(ps, exprIndex[0], stringVal);
        }
        else
        {
            super.setObject(ec, ps, exprIndex, value);
        }
    }

    /**
     * Convenience method to get the value to be persisted for an Enum via a method call.
     * @param value The Enum value
     * @param methodName The name of the method
     * @return The value to use (or null if not valid)
     */
    protected Long getValueForEnumUsingMethod(Enum value, String methodName)
    {
        // Case where the user has defined their own "value" for each enum
        // Assumes the method returns a Number (something that we can get an int from)
        try
        {
            Method getterMethod = ClassUtils.getMethodForClass(value.getClass(), methodName, null);
            Number num = (Number)getterMethod.invoke(value);
            return Long.valueOf(num.longValue());
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.warn("Specified enum value-getter for method " + methodName +
                " on field " + mmd.getFullFieldName() +
                " gave an error on extracting the value : " + e.getMessage());
        }
        return null;
    }

    /**
     * Method to extract the Enum object from the passed result set.
     * @param ec ExecutionContext
     * @param resultSet The result set
     * @param exprIndex The index position(s) in the result set to use.
     * @return The Enum
     */
    public Object getObject(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return null;
        }

        if (getDatastoreMapping(0).getObject(resultSet, exprIndex[0]) == null)
        {
            return null;
        }
        else if (datastoreJavaType.equals(ClassNameConstants.JAVA_LANG_INTEGER))
        {
            long longVal = getDatastoreMapping(0).getLong(resultSet, exprIndex[0]);
            Class enumType = null;
            if (mmd == null)
            {
                enumType = ec.getClassLoaderResolver().classForName(type);
            }
            else
            {
                enumType = mmd.getType();
                if (roleForMember == FieldRole.ROLE_FIELD)
                {
                    if (mmd.hasExtension(ENUM_GETTER_BY_VALUE))
                    {
                        // Case where the user has defined their own "value" for each enum
                        String getterMethodName = mmd.getValueForExtension(ENUM_GETTER_BY_VALUE);
                        return getEnumValueForMethod(enumType, longVal, getterMethodName);
                    }
                }

                if (roleForMember == FieldRole.ROLE_COLLECTION_ELEMENT)
                {
                    enumType = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
                    if (mmd.getElementMetaData() != null && mmd.getElementMetaData().hasExtension(ENUM_GETTER_BY_VALUE))
                    {
                        // Case where the user has defined their own "value" for each enum
                        String getterMethodName = mmd.getElementMetaData().getValueForExtension(ENUM_GETTER_BY_VALUE);
                        return getEnumValueForMethod(enumType, longVal, getterMethodName);
                    }
                }
                else if (roleForMember == FieldRole.ROLE_ARRAY_ELEMENT)
                {
                    enumType = ec.getClassLoaderResolver().classForName(mmd.getArray().getElementType());
                    if (mmd.getElementMetaData() != null && mmd.getElementMetaData().hasExtension(ENUM_GETTER_BY_VALUE))
                    {
                        // Case where the user has defined their own "value" for each enum
                        String getterMethodName = mmd.getElementMetaData().getValueForExtension(ENUM_GETTER_BY_VALUE);
                        return getEnumValueForMethod(enumType, longVal, getterMethodName);
                    }
                }
                else if (roleForMember == FieldRole.ROLE_MAP_KEY)
                {
                    enumType = ec.getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
                    if (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().hasExtension(ENUM_GETTER_BY_VALUE))
                    {
                        // Case where the user has defined their own "value" for each enum
                        String getterMethodName = mmd.getKeyMetaData().getValueForExtension(ENUM_GETTER_BY_VALUE);
                        return getEnumValueForMethod(enumType, longVal, getterMethodName);
                    }
                }
                else if (roleForMember == FieldRole.ROLE_MAP_VALUE)
                {
                    enumType = ec.getClassLoaderResolver().classForName(mmd.getMap().getValueType());
                    if (mmd.getValueMetaData() != null && mmd.getValueMetaData().hasExtension(ENUM_GETTER_BY_VALUE))
                    {
                        // Case where the user has defined their own "value" for each enum
                        String getterMethodName = mmd.getValueMetaData().getValueForExtension(ENUM_GETTER_BY_VALUE);
                        return getEnumValueForMethod(enumType, longVal, getterMethodName);
                    }
                }
            }
            return enumType.getEnumConstants()[(int)longVal];
        }
        else if (datastoreJavaType.equals(ClassNameConstants.JAVA_LANG_STRING))
        {
            String stringVal = getDatastoreMapping(0).getString(resultSet, exprIndex[0]);
            Class enumType = null;
            if (mmd == null)
            {
                enumType = ec.getClassLoaderResolver().classForName(type);
            }
            else
            {
                enumType = mmd.getType();
                if (roleForMember == FieldRole.ROLE_COLLECTION_ELEMENT)
                {
                    enumType = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
                }
                else if (roleForMember == FieldRole.ROLE_ARRAY_ELEMENT)
                {
                    enumType = ec.getClassLoaderResolver().classForName(mmd.getArray().getElementType());
                }
                else if (roleForMember == FieldRole.ROLE_MAP_KEY)
                {
                    enumType = ec.getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
                }
                else if (roleForMember == FieldRole.ROLE_MAP_VALUE)
                {
                    enumType = ec.getClassLoaderResolver().classForName(mmd.getMap().getValueType());
                }
            }
            return Enum.valueOf(enumType, stringVal);
        }
        else
        {
            return super.getObject(ec, resultSet, exprIndex);
        }
    }

    /**
     * Convenience method to return the value of an Enum using a static getter method, passing in a value.
     * @param enumType The type of the enum
     * @param val The value to pass in
     * @param methodName Name of the static method
     * @return The Enum value
     */
    protected Object getEnumValueForMethod(Class enumType, long val, String methodName)
    {
        // Case where the user has defined their own "value" for each enum
        // Assumes the method takes in a short/int, and returns the Enum);
        try
        {
            Method getterMethod = ClassUtils.getMethodForClass(enumType, methodName, new Class[] {short.class});
            return getterMethod.invoke(null, new Object[] {(short)val});
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.warn("Specified enum getter-by-value for field " + mmd.getFullFieldName() +
                " gave an error on extracting the enum so just using the ordinal : " + e.getMessage());
        }
        try
        {
            Method getterMethod = ClassUtils.getMethodForClass(enumType, methodName, new Class[] {int.class});
            return getterMethod.invoke(null, new Object[] {(int)val});
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.warn("Specified enum getter-by-value for field " + mmd.getFullFieldName() +
                " gave an error on extracting the enum so just using the ordinal : " + e.getMessage());
        }
        return null;
    }
}