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

import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.rdbms.mapping.column.OracleBlobColumnMapping;

/**
 * Mapping for Object and Serializable types.
 */
public class OracleSerialisedObjectMapping extends SerialisedMapping implements MappingCallbacks
{
    /**
     * @see org.datanucleus.store.rdbms.mapping.MappingCallbacks#postFetch(org.datanucleus.state.ObjectProvider)
     */
    public void postFetch(ObjectProvider sm)
    {
    }

    /**
     * Retrieve the empty BLOB created by the insert statement and write out the
     * current BLOB field value to the Oracle BLOB object
     * @param op the current ObjectProvider
     */
    public void insertPostProcessing(ObjectProvider op)
    {
        // Generate the contents for the BLOB
        byte[] bytes = new byte[0];
        Object value = op.provideField(mmd.getAbsoluteFieldNumber());
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
     * @see org.datanucleus.store.rdbms.mapping.MappingCallbacks#preDelete(org.datanucleus.state.ObjectProvider)
     */
    public void preDelete(ObjectProvider op)
    {
    }
}