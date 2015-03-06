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
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;
import org.datanucleus.util.TypeConversionHelper;

/**
 * Mapping for Enum type.
 */
public class EnumMapping extends SingleFieldMapping
{
    protected String datastoreJavaType = ClassNameConstants.JAVA_LANG_STRING;

    /**
     * Initialize this JavaTypeMapping with the given DatastoreAdapter for the given member MetaData.
     * @param mmd MetaData for the member to be mapped (if any)
     * @param table The table storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     */
    public void initialize(AbstractMemberMetaData mmd, Table table, ClassLoaderResolver clr)
    {
        if (mmd != null && mmd.isSerialized())
        {
            datastoreJavaType = ClassNameConstants.JAVA_IO_SERIALIZABLE;
        }
        else if (mmd != null)
        {
            ColumnMetaData[] colmds = getColumnMetaDataForMember(mmd, roleForMember);
            if (colmds != null && colmds.length > 0)
            {
                // Check if the user requested an INTEGER based datastore type
                if (MetaDataUtils.isJdbcTypeNumeric(colmds[0].getJdbcType()))
                {
                    datastoreJavaType = ClassNameConstants.JAVA_LANG_INTEGER;
                }
            }
        }

        super.initialize(mmd, table, clr);
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
            if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0 && 
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

                    Integer[] valueInts = new Integer[values.length];
                    for (int i=0;i<values.length;i++)
                    {
                        valueInts[i] = (int)TypeConversionHelper.getValueFromEnum(mmd, roleForMember, values[i]);
                    }
                    return valueInts;
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
                int intVal = (int)TypeConversionHelper.getValueFromEnum(mmd, roleForMember, (Enum)value);
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
                return enumType.getEnumConstants()[(int)longVal];
            }

            return TypeConversionHelper.getEnumFromValue(mmd, roleForMember, ec, longVal);
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
}