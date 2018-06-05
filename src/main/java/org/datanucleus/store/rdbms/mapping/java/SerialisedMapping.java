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

import org.datanucleus.ClassNameConstants;

/**
 * Maps a field as serialised.
 */
public class SerialisedMapping extends SingleFieldMapping
{
    /**
     * Accessor for the (Java) type of data represented here
     * @return java.lang.Object
     */
    public Class getJavaType()
    {
        return Object.class;
    }

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore
     * field. Returns Serializable since the object needs to be Serialisable
     * @param index requested column index.
     * @return the name of java-type for the requested column.
     */
    public String getJavaTypeForColumnMapping(int index)
    {
        return ClassNameConstants.JAVA_IO_SERIALIZABLE;
    }
}