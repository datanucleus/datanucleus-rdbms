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
import org.datanucleus.store.rdbms.mapping.datastore.OracleBlobRDBMSMapping;
import org.datanucleus.store.types.SCOUtils;

/**
 * Oracle variant of the MapMapping for cases where we are serialising
 * the field into a single column. This is necessary so we can intercept perform
 * any necessary postInsert, postUpdate nonsense for inserting BLOBs
 */
public class OracleMapMapping extends MapMapping
{
    /**
     * Retrieve the empty BLOB created by the insert statement and write out the
     * current BLOB field value to the Oracle CLOB object.
     * @param ownerOP ObjectProvider of the owner
     */
    public void postInsert(ObjectProvider ownerOP)
    {
        if (containerIsStoredInSingleColumn())
        {
            java.util.Map value = (java.util.Map) ownerOP.provideField(mmd.getAbsoluteFieldNumber());

            // Do nothing when serialised since we are handled in the main request
            if (value != null)
            {
                // Make sure the keys/values are ok for proceeding
                ExecutionContext ec = ownerOP.getExecutionContext();
                SCOUtils.validateObjectsForWriting(ec, value.keySet());
                SCOUtils.validateObjectsForWriting(ec, value.values());
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
            OracleBlobRDBMSMapping.updateBlobColumn(ownerOP, getTable(), getDatastoreMapping(0), bytes);
        }
        else
        {
            super.postInsert(ownerOP);
        }
    }

    /**
     * @see org.datanucleus.store.rdbms.mapping.MappingCallbacks#postUpdate(org.datanucleus.state.ObjectProvider)
     */
    public void postUpdate(ObjectProvider ownerOP)
    {
        if (containerIsStoredInSingleColumn())
        {
            java.util.Map value = (java.util.Map) ownerOP.provideField(mmd.getAbsoluteFieldNumber());

            if (value != null)
            {
                // Make sure the keys/values are ok for proceeding
                ExecutionContext ec = ownerOP.getExecutionContext();
                SCOUtils.validateObjectsForWriting(ec, value.keySet());
                SCOUtils.validateObjectsForWriting(ec, value.values());
            }

            postInsert(ownerOP);
        }
        else
        {
            super.postUpdate(ownerOP);
        }
    }
}