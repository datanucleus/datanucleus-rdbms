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

import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerationException;

/**
 * This generator for Relational Databases uses the "select max(column) from
 * table" strategy. The block size is limited to 1. MaxPoidGenerator works
 * with numbers, so clients using this generator must cast the ID to Long *
 * <P>
 * <B>Required user properties</B>
 * </P>
 * <UL>
 * </UL>
 * <P>
 * <B>Optional user properties</B>
 * <UL>
 * </UL>
 * </P>
 */
public class MaxGenerator extends AbstractRDBMSGenerator
{
    /**
     * Constructor.
     * @param name Symbolic name for this generator
     * @param props Properties defining the behaviour of this generator
     */
    public MaxGenerator(String name, Properties props)
    {
        super(name, props);
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
        // search an Id in the database
        PreparedStatement ps = null;
        ResultSet rs = null;
        RDBMSStoreManager rdbmsMgr = (RDBMSStoreManager)storeMgr;
        SQLController sqlControl = rdbmsMgr.getSQLController();
        try
        {
            String stmt = getStatement();
            ps = sqlControl.getStatementForUpdate(connection, stmt, false);

            rs = sqlControl.executeStatementQuery(null, connection, stmt, ps);
            if (!rs.next())
            {
                return new ValueGenerationBlock(new Object[] { Long.valueOf(1) });
            }

            return new ValueGenerationBlock(new Object[] { Long.valueOf(rs.getLong(1) + 1)});
        }
        catch (SQLException e)
        {
            //TODO adds a message correspondent to the exception. 
            //we need to work to create user friendly messages
            throw new ValueGenerationException(e.getMessage());
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
                    sqlControl.closeStatement(connection, ps);
                }
            }
            catch (SQLException e)
            {
                // no recoverable error
            }           
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
        stmt.append(srm.getIdentifierFactory().getIdentifierInAdapterCase((String)properties.get("column-name")));
        stmt.append(") FROM ");
        stmt.append(srm.getIdentifierFactory().getIdentifierInAdapterCase((String)properties.get("table-name")));  
        return stmt.toString();
    }
}