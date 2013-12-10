/**********************************************************************
Copyright (c) 2003 Erik Bengtson and others. All rights reserved.
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
2003 Andy Jefferson - coding standards
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.util.Locale;

import org.datanucleus.store.types.converters.LocaleStringConverter;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Mapping for Locale type.
 * 
 * Locale should be stored in colums from 2 to 20 characters. Normaly, we will have a string no longer than
 * 5 characters, but variants, in general, are vendor specific and can be longer than expected. 
 * The Variant codes are vendor and browser-specific. For example, use WIN for Windows, MAC for Macintosh,
 * and POSIX for POSIX. Where there are two variants, separate them with an underscore, and put the most
 * important one first. For example, a Traditional Spanish collation might construct a locale with
 * parameters for language, country and variant as: "es", "ES", "Traditional_WIN".  
 * language_country_variant
 * Examples: "en", "de_DE", "_GB", "en_US_WIN", "de__POSIX", "fr_MAC"
 * @see java.util.Locale
 */
public class LocaleMapping extends ObjectAsStringMapping
{
    private static TypeConverter<Locale, String> converter = new LocaleStringConverter();

    public Class getJavaType()
    {
        return Locale.class;
    }

    /**
     * Method to return the default length of this type in the datastore.
     * Locales require 20 characters.
     * @param index The index position
     * @return The default length
     */
    public int getDefaultLength(int index)
    {
        return 20;
    }

    /**
     * Method to set the datastore string value based on the object value.
     * @param object The object
     * @return The string value to pass to the datastore
     */
    protected String objectToString(Object object)
    {
        return converter.toDatastoreType((Locale)object);
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