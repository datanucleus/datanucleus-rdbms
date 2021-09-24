/**********************************************************************
Copyright (c) 2006 Andy Jefferson and others. All rights reserved.
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

import org.datanucleus.ExecutionContext;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.mapping.column.ColumnMappingPostSet;

/**
 * Mapping for a serialised persistable object for Oracle.
 */
public class OracleSerialisedPCMapping extends SerialisedPCMapping
{
    /**
     * Retrieve the empty BLOB created by the insert statement and write out the current BLOB field value to the Oracle BLOB object
     * @param ownerSM the current ObjectProvider
     */
    public void performSetPostProcessing(ObjectProvider ownerSM)
    {
        Object value = ownerSM.provideField(mmd.getAbsoluteFieldNumber());
        ObjectProvider sm = null;
        if (value != null)
        {
            ExecutionContext ec = ownerSM.getExecutionContext();
            sm = ec.findObjectProvider(value);
            if (sm == null || sm.getExecutionContext().getApiAdapter().getExecutionContext(value) == null)
            {
                // Assign a ObjectProvider to the serialised object since none present
                sm = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, value, false, ownerSM, mmd.getAbsoluteFieldNumber());
            }
        }

        if (sm != null)
        {
            sm.setStoringPC();
        }

        // Generate the contents for the BLOB
        byte[] bytes = new byte[0];
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

        // Update the BLOB
        if (columnMappings[0] instanceof ColumnMappingPostSet)
        {
            ((ColumnMappingPostSet)columnMappings[0]).setPostProcessing(ownerSM, bytes);
        }

        if (sm != null)
        {
            sm.unsetStoringPC();
        }
    }
}