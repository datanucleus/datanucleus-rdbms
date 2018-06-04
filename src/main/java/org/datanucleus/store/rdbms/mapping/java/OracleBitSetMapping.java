/**********************************************************************
Copyright (c) 2006 Erik Bengtson and others. All rights reserved.
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
2006 Andy Jefferson - commonised approach to BLOB writing as other Oracle mappings
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.rdbms.mapping.datastore.BlobImpl;
import org.datanucleus.store.rdbms.mapping.datastore.OracleBlobColumnMapping;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.TypeConversionHelper;

/**
 * Mapping for a BitSet type for Oracle.
 */
public class OracleBitSetMapping extends BitSetMapping implements MappingCallbacks
{
    /**
     * Method to handle post-processing of the insert of the BLOB/CLOB for Oracle.
     * @param op ObjectProvider of the owner
     */
    public void insertPostProcessing(ObjectProvider op)
    {
        Object value = op.provideField(mmd.getAbsoluteFieldNumber());
        if (value == null)
        {
            return;
        }

        // Generate the contents for the BLOB
        byte[] bytes = new byte[0];
        try
        {
            if (mmd.isSerialized())
            {
                // Serialised field so just perform basic Java serialisation for retrieval
                if (!(value instanceof Serializable))
                {
                    throw new NucleusDataStoreException(Localiser.msg("055005", value.getClass().getName()));
                }
                BlobImpl b = new BlobImpl(value);
                bytes = b.getBytes(0, (int) b.length());
            }
            else if (value instanceof java.util.BitSet)
            {
                bytes = TypeConversionHelper.getByteArrayFromBooleanArray(TypeConversionHelper.getBooleanArrayFromBitSet((java.util.BitSet) value));
            }
            else
            {
                // Fall back to just perform Java serialisation for storage
                if (!(value instanceof Serializable))
                {
                    throw new NucleusDataStoreException(Localiser.msg("055005", value.getClass().getName()));
                }
                BlobImpl b = new BlobImpl(value);
                bytes = b.getBytes(0, (int) b.length());
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "Object", "" + value, mmd, e.getMessage()), e);
        }
        catch (IOException e1)
        {
            // Do nothing
        }

        // Update the BLOB
        OracleBlobColumnMapping.updateBlobColumn(op, getTable(), getColumnMapping(0), bytes);
    }

    /**
     * Method to be called after the insert of the owner class element.
     * @param op ObjectProvider of the owner
     **/
    public void postInsert(ObjectProvider op)
    {        
    }

    /**
     * Method to be called after any update of the owner class element.
     * @param op ObjectProvider of the owner
     */
    public void postUpdate(ObjectProvider op)
    {
        insertPostProcessing(op);
    }

    public void deleteDependent(ObjectProvider op)
    {
    }

    public void postFetch(ObjectProvider op)
    {
    }

    public void preDelete(ObjectProvider op)
    {
    }
}