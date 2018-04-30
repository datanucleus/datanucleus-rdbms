/**********************************************************************
Copyright (c) 2005 Erik Bengtson and others. All rights reserved. 
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
package org.datanucleus.store.rdbms.valuegenerator;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.valuegenerator.AbstractConnectedGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerationException;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.util.Localiser;

/**
 * ValueGenerator using DB2 datastore-based DatastoreUUID generation.
 */
public final class DatastoreUUIDHexGenerator extends AbstractConnectedGenerator<String>
{
    /**
     * Constructor.
     * @param storeMgr StoreManager
     * @param name Symbolic name for this generator
     * @param props Properties defining the behaviour of this generator
     */
    public DatastoreUUIDHexGenerator(StoreManager storeMgr, String name, Properties props)
    {
        super(storeMgr, name, props);
        allocationSize = 10;
        if (properties != null)
        {
            if (properties.containsKey(ValueGenerator.PROPERTY_KEY_CACHE_SIZE))
            {
                try
                {
                    allocationSize = Integer.parseInt((String)properties.get(ValueGenerator.PROPERTY_KEY_CACHE_SIZE));
                }
                catch (Exception e)
                {
                    throw new ValueGenerationException(Localiser.msg("040006",properties.get(ValueGenerator.PROPERTY_KEY_CACHE_SIZE)));
                }
            }
        }
    }

    /**
     * Accessor for the storage class for values generated with this generator.
     * @return Storage class (in this case String.class)
     */
    public static Class getStorageClass()
    {
        return String.class;
    }

    /**
     * Reserve a block of ids.
     * @param size Block size
     * @return The reserved block
     */
    protected synchronized ValueGenerationBlock<String> reserveBlock(long size)
    {
        if (size < 1)
        {
            return null;
        }

        List<String> oids = new ArrayList<>();
        try
        {
            ManagedConnection mconn = connectionProvider.retrieveConnection();

            PreparedStatement ps = null;
            ResultSet rs = null;
            RDBMSStoreManager rdbmsMgr = (RDBMSStoreManager)storeMgr;
            SQLController sqlControl = rdbmsMgr.getSQLController();
            try
            {
                // Find the next ID from the database
                DatastoreAdapter dba = rdbmsMgr.getDatastoreAdapter();

                String stmt = dba.getSelectNewUUIDStmt();
                ps = sqlControl.getStatementForQuery(mconn, stmt);
                for (int i=1; i<size; i++)
                {
                    rs = sqlControl.executeStatementQuery(null, mconn, stmt, ps);
                    if (rs.next())
                    {
                        oids.add(rs.getString(1));
                    }
                }
            }
            catch (SQLException e)
            {
                throw new ValueGenerationException(Localiser.msg("040008",e.getMessage()));
            }
            finally
            {
                try
                {
                    if (rs != null)
                    {
                        rs.close();
                    }
                    if (ps != null)
                    {
                        sqlControl.closeStatement(mconn, ps);
                    }
                }
                catch (SQLException e)
                {
                    // non-recoverable error
                }
            }
        }
        finally
        {
            connectionProvider.releaseConnection();
        }

        return new ValueGenerationBlock(oids);
    }
}