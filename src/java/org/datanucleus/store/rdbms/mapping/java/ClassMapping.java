/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ClassNameConstants;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.types.converters.ClassStringConverter;

/**
 * Mapping for a Class type. Converts it to a String for persisting in the datastore.
 */
public class ClassMapping extends SingleFieldMapping
{
    private static ClassStringConverter converter = new ClassStringConverter();

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.SingleFieldMapping#initialize(org.datanucleus.metadata.AbstractMemberMetaData, org.datanucleus.store.rdbms.DatastoreContainerObject, org.datanucleus.ClassLoaderResolver)
     */
    @Override
    public void initialize(AbstractMemberMetaData fmd, Table table, ClassLoaderResolver clr)
    {
        super.initialize(fmd, table, clr);

        converter.setClassLoaderResolver(storeMgr.getNucleusContext().getClassLoaderResolver(null));
    }

    public Class getJavaType()
    {
        return Class.class;
    }

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore
     * field. This java-type must have an entry in the datastore mappings.
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForDatastoreMapping(int index)
    {
        // All of the types extending this class will be using java-type of String for the datastore
        return ClassNameConstants.JAVA_LANG_STRING;
    }

    /**
     * Method to set the object when updating the the datastore.
     * @see org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping#setObject(org.datanucleus.ExecutionContext, PreparedStatement, int[], java.lang.Object)
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
        getDatastoreMapping(0).setObject(ps, exprIndex[0], converter.toDatastoreType((Class)value));
    }

    /**
     * Method to get the object from the datastore and convert to an object.
     * @see org.datanucleus.store.rdbms.mapping.java.SingleFieldMapping#getObject(org.datanucleus.ExecutionContext, ResultSet, int[])
     */
    public Object getObject(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return null;
        }

        Object datastoreValue = getDatastoreMapping(0).getObject(resultSet, exprIndex[0]);
        Object value = null;
        if (datastoreValue != null)
        {
            value = converter.toMemberType((String)datastoreValue);
        }
        return value;
    }
}