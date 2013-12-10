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

import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.table.Column;

/**
 * Mapping to represent a field that is mapped to multiple datastore columns.
 */
public abstract class SingleFieldMultiMapping extends JavaTypeMapping
{
    /**
     * Convenience method to add a datastore field for this mapping.
     * If this mapping is a "full" mapping (for a field in a table) then a Column will be added,
     * otherwise (mapping representing a parameter in a query) will just add a datastore mapping.
     * The datastore mapping is added to the end of the datastoreMappings.
     * @param typeName Java type of the field to add the column for.
     */
    protected void addColumns(String typeName)
    {
        MappingManager mgr = storeMgr.getMappingManager();
        Column column = null;
        if (table != null)
        {
            // Full mapping, so add column to back the datastore mapping
            column = mgr.createColumn(this, typeName, getNumberOfDatastoreMappings());
        }
        mgr.createDatastoreMapping(this, column, typeName);
    }

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore
     * field. This java-type must have an entry in the datastore mappings.
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForDatastoreMapping(int index)
    {
        return datastoreMappings[index].getColumn().getStoredJavaType();
    }

    /**
     * Whether the mapping has a simple (single column) datastore representation.
     * @return Whether it has a simple datastore representation (single column)
     */
    public boolean hasSimpleDatastoreRepresentation()
    {
        return false;
    }
}