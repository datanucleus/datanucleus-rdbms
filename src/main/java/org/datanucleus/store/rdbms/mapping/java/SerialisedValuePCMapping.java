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

import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.table.Column;

/**
 * Mapping for a serialised persistable object being the value of a Map.
 */
public class SerialisedValuePCMapping extends SerialisedPCMapping
{
    /**
     * Method to prepare a field mapping for use in the datastore.
     * This creates the column in the table.
     */
    protected void prepareDatastoreMapping()
    {
        MappingManager mmgr = storeMgr.getMappingManager();
        ColumnMetaData colmd = null;
        if (mmd.getValueMetaData() != null && mmd.getValueMetaData().getColumnMetaData() != null &&
            mmd.getValueMetaData().getColumnMetaData().length > 0)
        {
            colmd = mmd.getValueMetaData().getColumnMetaData()[0];
        }
        Column col = mmgr.createColumn(this,getType(), colmd);
        mmgr.createColumnMapping(this, mmd, 0, col);
    }
}