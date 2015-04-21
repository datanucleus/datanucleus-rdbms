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
2005 David Eaves - contributed ClobRDBMSMapping
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Clob;
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
 * Mapping of a Clob RDBMS type.
 */
public class ClobRDBMSMapping extends LongVarcharRDBMSMapping
{
    /**
     * Constructor.
     * @param mapping The java type mapping for the field.
     * @param storeMgr Manager for the store
     * @param col Column
     */
    public ClobRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
        super(mapping, storeMgr, col);
    }

    public int getJDBCType()
    {
        return Types.CLOB;
    }

    public void setString(PreparedStatement ps, int param, String value)
    {
        if (getDatastoreAdapter().supportsOption(DatastoreAdapter.CLOB_SET_USING_SETSTRING))
        {
            super.setString(ps, param ,value);
        }
        else
        {
            setObject(ps, param, value);
        }
    }
    
    public void setObject(PreparedStatement ps, int param, Object value)
    {
        if (getDatastoreAdapter().supportsOption(DatastoreAdapter.CLOB_SET_USING_SETSTRING))
        {
            super.setObject(ps, param, value);
        }
        else
        {
            try
            {
                if (value == null)
                {
                    ps.setNull(param, getJDBCType());
                }
                else
                {
                    ps.setClob(param, new ClobImpl((String)value));
                }
            }
            catch (SQLException e)
            {
                throw new NucleusDataStoreException(Localiser.msg("055001","Object", "" + value, column, e.getMessage()), e);
            }
            catch (IOException e)
            {
                throw new NucleusDataStoreException(Localiser.msg("055001","Object", "" + value, column, e.getMessage()), e);
            }
        }
    }

    public String getString(ResultSet rs, int param)
    {
        if (getDatastoreAdapter().supportsOption(DatastoreAdapter.CLOB_SET_USING_SETSTRING))
        {
            return super.getString(rs, param);
        }

        return (String) getObject(rs, param);
    }
    
    public Object getObject(ResultSet rs, int param)
    {
        if (getDatastoreAdapter().supportsOption(DatastoreAdapter.CLOB_SET_USING_SETSTRING))
        {
            return super.getObject(rs, param);
        }

        Object value;

        try
        {
            Clob clob = rs.getClob(param);
            if (!rs.wasNull())
            {
                BufferedReader br = new BufferedReader(clob.getCharacterStream());
                try
                {
                    int c;
                    StringBuilder sb = new StringBuilder();
                    while ((c = br.read()) != -1)
                    {
                        sb.append((char)c);
                    }
                    value = sb.toString(); 
                }
                finally
                {
                    br.close();
                }
            }
            else
            {
                value = null;
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","Object", "" + param, column, e.getMessage()), e);
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","Object", "" + param, column, e.getMessage()), e);
        }

        return value;
    }    
}