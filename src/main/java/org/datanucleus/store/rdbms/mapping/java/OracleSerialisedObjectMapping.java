/**********************************************************************
Copyright (c) 2005 Brendan De Beer and others. All rights reserved.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.mapping.column.ColumnMappingPostSet;

/**
 * Mapping for Object and Serializable types with Oracle.
 */
public class OracleSerialisedObjectMapping extends SerialisedMapping
{
    /**
     * Retrieve the empty BLOB created by the insert statement and write out the current BLOB field value to the Oracle BLOB object
     * @param sm the current StateManager
     */
    public void performSetPostProcessing(DNStateManager sm)
    {
        // Generate the contents for the BLOB
        byte[] bytes = new byte[0];
        Object value = sm.provideField(mmd.getAbsoluteFieldNumber());
        if (value != null)
        {
            try
            {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(value);
                bytes = baos.toByteArray();
            }
            catch (IOException e1)
            {
                // Do Nothing
            }
        }

        if (columnMappings[0] instanceof ColumnMappingPostSet)
        {
            // Update the BLOB
            ((ColumnMappingPostSet)columnMappings[0]).setPostProcessing(sm, bytes);
        }
    }
}