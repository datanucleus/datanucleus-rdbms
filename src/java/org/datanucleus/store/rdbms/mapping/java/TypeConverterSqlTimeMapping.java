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

import java.sql.Time;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;

/**
 * Mapping where the member has its value converted to/from some storable datastore type using a TypeConverter using java.sql.Time in the datastore.
 */
public class TypeConverterSqlTimeMapping extends TypeConverterMapping
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping#initialize(org.datanucleus.store.rdbms.RDBMSStoreManager, java.lang.String)
     */
    @Override
    public void initialize(RDBMSStoreManager storeMgr, String type)
    {
        super.initialize(storeMgr, type);

        // Sanity check on converter
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        Class fieldType = clr.classForName(type);
        Class datastoreType = TypeConverterHelper.getDatastoreTypeForTypeConverter(converter, fieldType);
        if (!Time.class.isAssignableFrom(datastoreType))
        {
            throw new NucleusException("Attempt to create mapping for type " + type + " yet this is not using Time in the datastore");
        }
    }

    public void initialize(AbstractMemberMetaData mmd, Table table, ClassLoaderResolver clr)
    {
        this.initialize(mmd, table, clr, null);
    }

    public void initialize(AbstractMemberMetaData mmd, Table table, ClassLoaderResolver clr, TypeConverter conv)
    {
        super.initialize(mmd, table, clr, conv);

        // Sanity check on converter
        Class datastoreType = TypeConverterHelper.getDatastoreTypeForTypeConverter(converter, mmd.getType());
        if (!Time.class.isAssignableFrom(datastoreType))
        {
            throw new NucleusException("Attempt to create mapping for member " + mmd.getFullFieldName() + " yet this is not using Time in the datastore");
        }
    }

    public String getJavaTypeForDatastoreMapping(int index)
    {
        return Time.class.getName();
    }
}