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
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.mapping.column.BlobImpl;
import org.datanucleus.store.rdbms.mapping.column.ColumnMappingPostSet;
import org.datanucleus.store.types.converters.ArrayConversionHelper;
import org.datanucleus.util.Localiser;

/**
 * Mapping for a BitSet type for Oracle.
 */
public class OracleBitSetMapping extends BitSetMapping
{
    /**
     * Retrieve the empty BLOB created by the insert statement and write out the current BLOB field value to the Oracle BLOB object
     * @param sm the current StateManager
     */
    public void performSetPostProcessing(DNStateManager sm)
    {
        Object value = sm.provideField(mmd.getAbsoluteFieldNumber());
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
                bytes = ArrayConversionHelper.getByteArrayFromBooleanArray(ArrayConversionHelper.getBooleanArrayFromBitSet((java.util.BitSet) value));
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

        if (columnMappings[0] instanceof ColumnMappingPostSet)
        {
            ((ColumnMappingPostSet)columnMappings[0]).setPostProcessing(sm, bytes);
        }
    }
}