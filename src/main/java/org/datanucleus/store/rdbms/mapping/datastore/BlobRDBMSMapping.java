/**********************************************************************
Copyright (c) 2004 Erik Bengtson and others. All rights reserved. 
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
2004 Andy Jefferson - localised messages
2006 Andy Jefferson - migrated to support serialised and non-serialised BLOBs
2010 Andy Jefferson - add getBlob handler
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.io.IOException;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Mapping of a BLOB RDBMS type.
 * A BLOB column can be treated in two ways in terms of storage and retrieval.
 * <ul>
 * <li>Serialise the field into the BLOB using ObjectOutputStream, and deserialise
 * it back using ObjectInputStream</li>
 * <li>Store the field using a byte[] stream, and retrieve it in the same way.</li>
 * </ul>
 */
public class BlobRDBMSMapping extends AbstractLargeBinaryRDBMSMapping
{
    /**
     * Constructor.
     * @param mapping Java type mapping
     * @param storeMgr Store Manager
     * @param col Column
     */
    public BlobRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
        super(mapping, storeMgr, col);
    }

    public int getJDBCType()
    {
        return Types.BLOB;
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.AbstractLargeBinaryRDBMSMapping#getObject(java.lang.Object,
     * int)
     */
    @Override
    public Object getObject(ResultSet rs, int param)
    {
        byte[] bytes = null;
        try
        {
            // Retrieve the bytes of the object directly
            bytes = rs.getBytes(param);
            if (bytes == null)
            {
                return null;
            }
        }
        catch (SQLException sqle)
        {
            try
            {
                // Retrieve the bytes using the Blob (if getBytes not supported e.g HSQLDB 2.0)
                Blob blob = rs.getBlob(param);
                if (blob == null)
                {
                    return null;
                }
                bytes = blob.getBytes(1, (int) blob.length());
                if (bytes == null)
                {
                    return null;
                }
            }
            catch (SQLException sqle2)
            {
                throw new NucleusDataStoreException(Localiser.msg("055002", "Object", "" + param, column, sqle2.getMessage()), sqle2);
            }
        }

        return getObjectForBytes(bytes, param);
    }

    public void setString(PreparedStatement ps, int param, String value)
    {
        try
        {
            if (getDatastoreAdapter().supportsOption(DatastoreAdapter.BLOB_SET_USING_SETSTRING))
            {
                if (value == null)
                {
                    if (column.isDefaultable() && column.getDefaultValue() != null)
                    {
                        ps.setString(param, column.getDefaultValue().toString().trim());
                    }
                    else
                    {
                        ps.setNull(param, getJDBCType());
                    }
                }
                else
                {
                    ps.setString(param, value);
                }
            }
            else
            {
                if (value == null)
                {
                    if (column != null && column.isDefaultable() && column.getDefaultValue() != null)
                    {
                        ps.setBlob(param, new BlobImpl(column.getDefaultValue().toString().trim()));
                    }
                    else
                    {
                        ps.setNull(param, getJDBCType());
                    }
                }
                else
                {
                    ps.setBlob(param, new BlobImpl(value));
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "String", "" + value, column, e.getMessage()), e);
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "String", "" + value, column, e.getMessage()), e);
        }
    }

    public String getString(ResultSet rs, int param)
    {
        String value;

        try
        {
            if (getDatastoreAdapter().supportsOption(DatastoreAdapter.BLOB_SET_USING_SETSTRING))
            {
                value = rs.getString(param);
            }
            else
            {
                byte[] bytes = rs.getBytes(param);
                if (bytes == null)
                {
                    value = null;
                }
                else
                {
                    BlobImpl blob = new BlobImpl(bytes);
                    value = (String) blob.getObject();
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "String", "" + param, column, e.getMessage()), e);
        }

        return value;
    }
}