/**********************************************************************
Copyright (c) 2003 Erik Bengtson and others. All rights reserved. 
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
2003 Andy Jefferson - coding standards
2004 Andy Jefferson - change from Integer to Long
2004 Andy Jefferson - removed last-id option since was dangerous in concurrent apps
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.valuegenerator;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.valuegenerator.AbstractConnectedGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerationException;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.util.NucleusLogger;

/**
 * ValueGenerator for RDBMS that uses the "SELECT MAX(column) FROM TABLE" strategy. 
 * The allocation size is limited to 1.
 * This works with numbers, so clients using this generator must cast the ID to Long.
 */
public class MaxGenerator extends AbstractConnectedGenerator<Long>
{
    /**
     * Constructor.
     * @param storeMgr StoreManager
     * @param name Symbolic name for this generator
     * @param props Properties defining the behaviour of this generator
     */
    public MaxGenerator(StoreManager storeMgr, String name, Properties props)
    {
        super(storeMgr, name, props);
        allocationSize = 1;
    }

    /**
     * Method to reserve a block of identities.
     * Note : Only allocates a single id always.
     * @param size The block size
     * @return The reserved block
     */
    public ValueGenerationBlock reserveBlock(long size)
    {
        try
        {
            // search an Id in the database
            ManagedConnection mconn = connectionProvider.retrieveConnection();
            PreparedStatement ps = null;
            ResultSet rs = null;
            SQLController sqlControl = ((RDBMSStoreManager)storeMgr).getSQLController();
            try
            {
                String stmt = getStatement();
                ps = sqlControl.getStatementForUpdate(mconn, stmt, false);

                rs = sqlControl.executeStatementQuery(null, mconn, stmt, ps);
                if (!rs.next())
                {
                    return new ValueGenerationBlock(new Object[] { Long.valueOf(1) });
                }

                return new ValueGenerationBlock(new Object[] { Long.valueOf(rs.getLong(1) + 1)});
            }
            catch (SQLException e)
            {
                NucleusLogger.VALUEGENERATION.warn("Exception thrown getting next value for MaxGenerator", e);
                throw new ValueGenerationException("Exception thrown getting next value for MaxGenerator", e);
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
                    // no recoverable error
                }           
            }
        }
        finally
        {
            connectionProvider.releaseConnection();
        }
    }

    /**
     * Return the SQL statement.
     * TODO Allow this to work in different catalog/schema
     * @return statement
     */
    private String getStatement()
    {
        RDBMSStoreManager srm = (RDBMSStoreManager)storeMgr;
        StringBuilder stmt = new StringBuilder();
        stmt.append("SELECT max(");
        stmt.append(srm.getIdentifierFactory().getIdentifierInAdapterCase((String)properties.get(ValueGenerator.PROPERTY_COLUMN_NAME)));
        stmt.append(") FROM ");
        stmt.append(srm.getIdentifierFactory().getIdentifierInAdapterCase((String)properties.get(ValueGenerator.PROPERTY_TABLE_NAME)));  
        return stmt.toString();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator#getConnectionPreference()
     */
    @Override
    public ConnectionPreference getConnectionPreference()
    {
        // Since the method employed here depends on all previous records being available to this connection, then we must use the existing ExecutionContext connection always.
        return ConnectionPreference.EXISTING;
    }
}