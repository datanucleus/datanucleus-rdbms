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
import org.datanucleus.store.rdbms.table.Table;

/**
 * Mapping for a PC key object stored in an embedded map within a PC object.
 * Provides mapping for a single Java type (the key PC type) to multiple datastore columns.
 **/
public class EmbeddedKeyPCMapping extends EmbeddedMapping
{
    /**
     * Initialise this JavaTypeMapping with the given DatastoreAdapter for the given metadata.
     * @param table The datastore container storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     * @param fmd FieldMetaData for the field to be mapped (if any)
     */
    public void initialize(AbstractMemberMetaData fmd, Table table, ClassLoaderResolver clr)
    {
        initialize(fmd, table, clr, fmd.getKeyMetaData().getEmbeddedMetaData(), fmd.getMap().getKeyType(), PersistableObjectType.EMBEDDED_MAP_KEY_PC);
    }
}