/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;

/**
 * Mapping where the member has its value converted to/from some storable datastore type using a TypeConverter that 
 * uses a String in the datastore.
 */
public class TypeConverterStringMapping extends TypeConverterMapping
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
        if (!String.class.isAssignableFrom(datastoreType))
        {
            throw new NucleusException("Attempt to create TypeConverterStringMapping for type " + type + " yet this is not using String in the datastore");
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
        if (!String.class.isAssignableFrom(datastoreType))
        {
            throw new NucleusException("Attempt to create TypeConverterStringMapping for member " + mmd.getFullFieldName() + " yet this is not using String in the datastore");
        }
    }

    // TODO Cater for the TypeConverter providing information about defaultLength
//    public int getDefaultLength(int index)
//    {
//        return ???
//    }

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore
     * field. This java-type must have an entry in the datastore mappings.
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForDatastoreMapping(int index)
    {
        return String.class.getName();
    }
}