/**********************************************************************
Copyright (c) 2006 Michael Brown and others. All rights reserved. 
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
2006 Michael Brown (AssetHouse Technology Ltd) - Added UUID support
2006 Andy Jefferson - changed to extend ObjectAsStringMapping
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.util.UUID;

import org.datanucleus.store.types.converters.UUIDStringConverter;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Mapping for java.util.UUID type.
 */
public class UUIDMapping extends ObjectAsStringMapping
{
    private static TypeConverter<UUID, String> converter = new UUIDStringConverter();

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#getJavaType()
     */
    public Class getJavaType()
    {
        return UUID.class;
    }

    /**
     * Method to set the datastore string value based on the object value.
     * @param object The object
     * @return The string value to pass to the datastore
     */
    protected String objectToString(Object object)
    {
        return converter.toDatastoreType((UUID)object);
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