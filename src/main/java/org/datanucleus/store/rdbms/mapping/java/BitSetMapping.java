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

import java.util.BitSet;

import org.datanucleus.ClassNameConstants;

/**
 * Mapping for an array of bytes.
 */
public class BitSetMapping extends SingleFieldMapping
{
    public Class getJavaType()
    {
        return BitSet.class;
    }

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore
     * field. Returns java.io.Serializable
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForDatastoreMapping(int index)
    {
        return ClassNameConstants.JAVA_IO_SERIALIZABLE;
    }
}