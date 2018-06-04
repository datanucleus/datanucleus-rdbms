/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved. 
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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.datastore.ColumnMapping;

/**
 * Mapping to represent multiple mappings within the single overall java type mapping.
 * This mapping can be used to represent, for example,
 * <UL>
 * <LI>a persistable field (where there are multiple PK fields).</LI>
 * <LI>a reference field (where there are multiple implementations - of Object or interface)</LI>
 * <LI>an element in a collection and the element has no table of its own, but multiple subclasses</LI>
 * <LI>a FK of a PC field (where there may be multiple fields in the PK of the PC object)</LI>
 * </UL>
 */
public abstract class MultiMapping extends JavaTypeMapping
{
    /** The Java mappings represented by this mapping. */
    protected JavaTypeMapping[] javaTypeMappings = new JavaTypeMapping[0];

    /** Number of datastore mappings - for convenience to improve performance **/
    protected int numberOfDatastoreMappings = 0;

    /**
     * Method to add a Java type mapping for a field
     * @param mapping The mapping to add
     */
    public void addJavaTypeMapping(JavaTypeMapping mapping)
    {
        JavaTypeMapping[] jtm = javaTypeMappings; 
        javaTypeMappings = new JavaTypeMapping[jtm.length+1]; 
        System.arraycopy(jtm, 0, javaTypeMappings, 0, jtm.length);
        javaTypeMappings[jtm.length] = mapping;
        // TODO Make sure the table of the mapping is the same as for the MultiMapping
    }

    /**
     * Accessor for the Java type mappings
     * @return The Java type mappings
     */
    public JavaTypeMapping[] getJavaTypeMapping()
    {
        return javaTypeMappings;
    }

    /**
     * Accessor for the number of datastore mappings.
     * @return The number of datastore mappings.
     */
    public int getNumberOfColumnMappings()
    {
        if (numberOfDatastoreMappings == 0)
        {
            int numDatastoreTmp = 0;
            for (int i=0; i<javaTypeMappings.length; i++)
            {
                numDatastoreTmp += javaTypeMappings[i].getNumberOfColumnMappings();
            }
            this.numberOfDatastoreMappings = numDatastoreTmp;
        }
        return numberOfDatastoreMappings;
    }

    public ColumnMapping[] getColumnMappings()
    {
        if (columnMappings.length == 0)
        {
            ColumnMapping[] colMappings = new ColumnMapping[getNumberOfColumnMappings()];
            int num = 0;
            for (int i=0; i<javaTypeMappings.length; i++)
            {
                for (int j=0;j<javaTypeMappings[i].getNumberOfColumnMappings();j++)
                {
                    colMappings[num++] = javaTypeMappings[i].getColumnMapping(j);
                }
            }
            columnMappings = colMappings;
        }
        return super.getColumnMappings();
    }

    /**
     * Accessor for a datastore mapping.
     * @param index The position of the mapping to return
     * @return The datastore mapping
     */
    public ColumnMapping getColumnMapping(int index)
    {
        if (index >= getNumberOfColumnMappings())
        {
            throw new NucleusException("Attempt to get DatastoreMapping with index " + index + 
                " when total number of mappings is " + numberOfDatastoreMappings + " for field=" + mmd).setFatal();
        }

        int currentIndex = 0;
        int numberJavaMappings = javaTypeMappings.length;
        for (int i=0; i<numberJavaMappings; i++)
        {
            int numberDatastoreMappings = javaTypeMappings[i].getNumberOfColumnMappings();
            for (int j=0; j<numberDatastoreMappings; j++)
            {
                if (currentIndex == index)
                {
                    return javaTypeMappings[i].getColumnMapping(j);
                }
                currentIndex++;
            }
        }

        // TODO Should never happen
        throw new NucleusException("Invalid index " + index + " for DatastoreMapping (numColumns=" + getNumberOfColumnMappings() + "), for field=" + mmd).setFatal();
    }
}