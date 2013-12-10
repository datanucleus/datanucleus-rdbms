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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.types.converters.ClassStringConverter;

/**
 * Mapping for a Class type. Converts it to a String for persisting in the datastore.
 */
public class ClassMapping extends ObjectAsStringMapping
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
     * Method to set the datastore string value based on the object value.
     * @param object The object
     * @return The string value to pass to the datastore
     */
    protected String objectToString(Object object)
    {
        return converter.toDatastoreType((Class)object);
    }

    /**
     * Method to extract the objects value from the datastore string value.
     * @param datastoreValue Value obtained from the datastore
     * @return The value of this object (derived from the datastore string value)
     */
    protected Object stringToObject(String datastoreValue)
    {
        return converter.toMemberType(datastoreValue);
    }
}