/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.mapping.java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.Localiser;

/**
 * Mapping for fields of type java.util.UUID.
 * Makes use of a TypeConverter (converting to String) as default, but if the user provides sqlType then attempts to find a native sqlType
 */
public class UUIDMapping extends SingleFieldMapping
{
    TypeConverter converter;

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping#initialize(org.datanucleus.store.rdbms.RDBMSStoreManager, java.lang.String)
     */
    @Override
    public void initialize(RDBMSStoreManager storeMgr, String type)
    {
        boolean useConverter = true;
        if (mmd != null)
        {
            ColumnMetaData[] colmds = mmd.getColumnMetaData();
            if (colmds != null && colmds.length == 1)
            {
                ColumnMetaData colmd = colmds[0];
                if (colmd.getSqlType() != null)
                {
                    useConverter = false;
                }
            }
        }

        if (useConverter)
        {
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            Class fieldType = clr.classForName(type);
            converter = storeMgr.getNucleusContext().getTypeManager().getDefaultTypeConverterForType(fieldType);
            if (converter == null)
            {
                throw new NucleusUserException("Unable to find TypeConverter for converting " + fieldType + " to String");
            }
        }

        super.initialize(storeMgr, type);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping#initialize(org.datanucleus.metadata.AbstractMemberMetaData, org.datanucleus.store.rdbms.table.Table, org.datanucleus.ClassLoaderResolver)
     */
    @Override
    public void initialize(AbstractMemberMetaData mmd, Table table, ClassLoaderResolver clr)
    {
        boolean useConverter = true;
        if (mmd != null)
        {
            ColumnMetaData[] colmds = mmd.getColumnMetaData();
            if (colmds != null && colmds.length == 1)
            {
                ColumnMetaData colmd = colmds[0];
                if (colmd.getSqlType() != null)
                {
                    useConverter = false;
                }
            }

            if (useConverter)
            {
                if (mmd.getTypeConverterName() != null)
                {
                    // Use specified converter (if found)
                    converter = table.getStoreManager().getNucleusContext().getTypeManager().getTypeConverterForName(mmd.getTypeConverterName());
                    if (converter == null)
                    {
                        throw new NucleusUserException(Localiser.msg("044062", mmd.getFullFieldName(), mmd.getTypeConverterName()));
                    }
                }
                else
                {
                    converter = table.getStoreManager().getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                }
            }
        }

        super.initialize(mmd, table, clr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping#getJavaType()
     */
    @Override
    public Class getJavaType()
    {
        return UUID.class;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping#getJavaTypeForDatastoreMapping(int)
     */
    @Override
    public String getJavaTypeForDatastoreMapping(int index)
    {
        if (converter == null)
        {
            return UUID.class.getName();
        }
        return storeMgr.getNucleusContext().getTypeManager().getDatastoreTypeForTypeConverter(converter, getJavaType()).getName();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping#setObject(org.datanucleus.ExecutionContext, java.sql.PreparedStatement, int[], java.lang.Object)
     */
    @Override
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
        if (exprIndex == null)
        {
            return;
        }

        if (converter == null)
        {
            super.setObject(ec, ps, exprIndex, value);
        }
        else
        {
            getDatastoreMapping(0).setObject(ps, exprIndex[0], converter.toDatastoreType(value));
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping#getObject(org.datanucleus.ExecutionContext, java.sql.ResultSet, int[])
     */
    @Override
    public Object getObject(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return null;
        }

        if (converter == null)
        {
            return super.getObject(ec, resultSet, exprIndex);
        }

        Object datastoreValue = getDatastoreMapping(0).getObject(resultSet, exprIndex[0]);
        return (datastoreValue != null ? converter.toMemberType(datastoreValue) : null);
    }
}
