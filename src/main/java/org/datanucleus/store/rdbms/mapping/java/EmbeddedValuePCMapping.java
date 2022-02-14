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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.table.Table;

/**
 * Mapping for a PC key object stored in an embedded map within a PC object.
 * Provides mapping for a single Java type (the value PC type) to multiple datastore columns.
 **/
public class EmbeddedValuePCMapping extends EmbeddedMapping
{
    /**
     * Initialise this JavaTypeMapping with the given DatastoreAdapter for the given metadata.
     * @param mmd MetaData for the field to be mapped (if any)
     * @param table The datastore container storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     */
    public void initialize(AbstractMemberMetaData mmd, Table table, ClassLoaderResolver clr)
    {
    	initialize(mmd, table, clr, mmd.getValueMetaData().getEmbeddedMetaData(), mmd.getMap().getValueType(),
            PersistableObjectType.EMBEDDED_MAP_VALUE_PC, DNStateManager.EMBEDDED_MAP_VALUE_PC);
    }
}