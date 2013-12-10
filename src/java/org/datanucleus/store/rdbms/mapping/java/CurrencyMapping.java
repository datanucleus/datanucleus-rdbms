/**********************************************************************
Copyright (c) 2004 Andy Jefferson and others. All rights reserved.
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

import java.util.Currency;

import org.datanucleus.store.types.converters.CurrencyStringConverter;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * SCO Mapping for Currency type.
 **/
public class CurrencyMapping extends ObjectAsStringMapping
{
    private static TypeConverter<Currency, String> converter = new CurrencyStringConverter();

    public Class getJavaType()
    {
        return Currency.class;
    }

    /**
     * Method to return the default length of this type in the datastore.
     * ISO4217 currencies use 3 character codes, so return 3.
     * @param index The index position
     * @return The default length
     */
    public int getDefaultLength(int index)
    {
        return 3;
    }

    /**
     * Method to set the datastore string value based on the object value.
     * @param object The object
     * @return The string value to pass to the datastore
     */
    protected String objectToString(Object object)
    {
        return converter.toDatastoreType((Currency)object);
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