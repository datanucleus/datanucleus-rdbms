/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved. 
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

import org.datanucleus.ClassNameConstants;

/**
 * Abstract base mapping for all temporal types.
 */
public abstract class TemporalMapping extends SingleFieldMapping
{
    /**
     * Method to return the default length of this type in the datastore.
     * @param index The index position
     * @return The default length
     */
    public int getDefaultLength(int index)
    {
        if (columnMappings != null && columnMappings.length > 0 && columnMappings[0].isStringBased())
        {
            return getDefaultLengthAsString();
        }
        return super.getDefaultLength(index);
    }

    protected abstract int getDefaultLengthAsString();

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore
     * field. This java-type must have an entry in the datastore mappings.
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForColumnMapping(int index)
    {
        if (columnMappings != null && columnMappings.length > 0 && columnMappings[0].isStringBased())
        {
            // Use String as our java type
            return ClassNameConstants.JAVA_LANG_STRING;
        }
        return super.getJavaTypeForColumnMapping(index);
    }
}