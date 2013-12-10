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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.net.URL;

import org.datanucleus.store.types.converters.URLStringConverter;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Mapping for java.net.URL type.
 */
public class URLMapping extends ObjectAsStringMapping
{
    private static TypeConverter<URL, String> converter = new URLStringConverter();

    /**
     * Method to return the Java type. In our case a java.net.URL.
     * @see org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping#getJavaType()
     */
    public Class getJavaType()
    {
        return URL.class;
    }

    /**
     * Method to set the datastore string value based on the object value.
     * @param object The object
     * @return The string value to pass to the datastore
     */
    protected String objectToString(Object object)
    {
        return converter.toDatastoreType((URL)object);
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