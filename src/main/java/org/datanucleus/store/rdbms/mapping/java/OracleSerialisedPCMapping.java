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
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.rdbms.mapping.column.OracleBlobColumnMapping;

/**
 * Mapping for a serialised persistable object for Oracle.
 */
public class OracleSerialisedPCMapping extends SerialisedPCMapping implements MappingCallbacks
{
    /**
     * Retrieve the empty BLOB created by the insert statement and write out the
     * current BLOB field value to the Oracle BLOB object
     * @param op the current ObjectProvider
     */
    public void insertPostProcessing(ObjectProvider op)
    {
        Object value = op.provideField(mmd.getAbsoluteFieldNumber());
        ObjectProvider sm = null;
        if (value != null)
        {
            ExecutionContext ec = op.getExecutionContext();
            sm = ec.findObjectProvider(value);
            if (sm == null || sm.getExecutionContext().getApiAdapter().getExecutionContext(value) == null)
            {
                // Assign a ObjectProvider to the serialised object since none present
                sm = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, value, false, op, mmd.getAbsoluteFieldNumber());
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
        OracleBlobColumnMapping.updateBlobColumn(op, getTable(), getColumnMapping(0), bytes);

        if (sm != null)
        {
            sm.unsetStoringPC();
        }
    }

    /**
     * @see org.datanucleus.store.rdbms.mapping.MappingCallbacks#postInsert(org.datanucleus.state.ObjectProvider)
     */
    public void postInsert(ObjectProvider op)
    {
    }

    /**
     * @see org.datanucleus.store.rdbms.mapping.MappingCallbacks#postUpdate(org.datanucleus.state.ObjectProvider)
     */
    public void postUpdate(ObjectProvider op)
    {
        insertPostProcessing(op);
    }

    /**
     * @see org.datanucleus.store.rdbms.mapping.MappingCallbacks#postFetch(org.datanucleus.state.ObjectProvider)
     */
    public void postFetch(ObjectProvider op)
    {
    }

    /**
     * @see org.datanucleus.store.rdbms.mapping.MappingCallbacks#preDelete(org.datanucleus.state.ObjectProvider)
     */
    public void preDelete(ObjectProvider op)
    {
    }
}