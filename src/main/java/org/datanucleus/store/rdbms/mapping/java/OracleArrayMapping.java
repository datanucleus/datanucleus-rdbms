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

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;

import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.mapping.column.BlobImpl;
import org.datanucleus.store.rdbms.mapping.column.ColumnMappingPostSet;
import org.datanucleus.store.types.converters.ArrayConversionHelper;
import org.datanucleus.util.Localiser;

/**
 * Oracle variant of the ArrayMapping for cases where we are serialising the field into a single (BLOB/CLOB) column
 */
public class OracleArrayMapping extends ArrayMapping
{
    public void performSetPostProcessing(DNStateManager sm)
    {
        if (containerIsStoredInSingleColumn())
        {
            if (columnMappings[0] instanceof ColumnMappingPostSet)
            {
                // Create the value to put in the BLOB
                Object value = sm.provideField(mmd.getAbsoluteFieldNumber());
                if (value == null)
                {
                    return;
                }
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
                    else if (value instanceof boolean[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromBooleanArray((boolean[]) value);
                    }
                    else if (value instanceof char[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromCharArray((char[]) value);
                    }
                    else if (value instanceof double[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromDoubleArray((double[]) value);
                    }
                    else if (value instanceof float[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromFloatArray((float[]) value);
                    }
                    else if (value instanceof int[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromIntArray((int[]) value);
                    }
                    else if (value instanceof long[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromLongArray((long[]) value);
                    }
                    else if (value instanceof short[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromShortArray((short[]) value);
                    }
                    else if (value instanceof Boolean[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromBooleanObjectArray((Boolean[]) value);
                    }
                    else if (value instanceof Byte[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromByteObjectArray((Byte[]) value);
                    }
                    else if (value instanceof Character[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromCharObjectArray((Character[]) value);
                    }
                    else if (value instanceof Double[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromDoubleObjectArray((Double[]) value);
                    }
                    else if (value instanceof Float[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromFloatObjectArray((Float[]) value);
                    }
                    else if (value instanceof Integer[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromIntObjectArray((Integer[]) value);
                    }
                    else if (value instanceof Long[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromLongObjectArray((Long[]) value);
                    }
                    else if (value instanceof Short[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromShortObjectArray((Short[]) value);
                    }
                    else if (value instanceof BigDecimal[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromBigDecimalArray((BigDecimal[]) value);
                    }
                    else if (value instanceof BigInteger[])
                    {
                        bytes = ArrayConversionHelper.getByteArrayFromBigIntegerArray((BigInteger[]) value);
                    }
                    else if (value instanceof byte[])
                    {
                        bytes = (byte[]) value;
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

                // Update the BLOB
                ((ColumnMappingPostSet)columnMappings[0]).setPostProcessing(sm, bytes);
            }
        }
    }

    /**
     * Method to be called after the insert of the owner class element.
     * @param ownerSM StateManager of the owner
     */
    public void postInsert(DNStateManager ownerSM)
    {
        if (containerIsStoredInSingleColumn())
        {
            Object value = ownerSM.provideField(mmd.getAbsoluteFieldNumber());
            if (value == null)
            {
                return;
            }
            ExecutionContext ec = ownerSM.getExecutionContext();

            if (mmd.getArray().elementIsPersistent())
            {
                // Make sure all persistable elements have StateManagers
                Object[] arrElements = (Object[])value;
                for (Object elem : arrElements)
                {
                    if (elem != null)
                    {
                        DNStateManager elemOP = ec.findStateManager(elem);
                        if (elemOP == null || ec.getApiAdapter().getExecutionContext(elem) == null)
                        {
                            elemOP = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, elem, false,
                                ownerSM, mmd.getAbsoluteFieldNumber(), PersistableObjectType.EMBEDDED_ARRAY_ELEMENT_PC);
                        }
                    }
                }
            }
        }
        else
        {
            super.postInsert(ownerSM);
        }
    }

    /**
     * Method to be called after any update of the owner class element.
     * @param ownerSM StateManager of the owner
     */
    public void postUpdate(DNStateManager ownerSM)
    {
        if (containerIsStoredInSingleColumn())
        {
            Object value = ownerSM.provideField(mmd.getAbsoluteFieldNumber());
            if (value == null)
            {
                return;
            }
            ExecutionContext ec = ownerSM.getExecutionContext();

            if (mmd.getArray().elementIsPersistent())
            {
                // Make sure all persistable elements have StateManagers
                Object[] arrElements = (Object[])value;
                for (Object elem : arrElements)
                {
                    if (elem != null)
                    {
                        DNStateManager elemOP = ec.findStateManager(elem);
                        if (elemOP == null || ec.getApiAdapter().getExecutionContext(elem) == null)
                        {
                            elemOP = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, elem, false,
                                ownerSM, mmd.getAbsoluteFieldNumber(), PersistableObjectType.EMBEDDED_ARRAY_ELEMENT_PC);
                        }
                    }
                }
            }
        }
        else
        {
            super.postUpdate(ownerSM);
        }
    }
}