/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.mapping.datastore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Mapping of an "OTHER" RDBMS type.
 */
public class OtherRDBMSMapping extends AbstractDatastoreMapping
{
    public OtherRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
        super(storeMgr, mapping);
        column = col;
        initialize();
    }

    /**
     * Method to initialise the column mapping.
     */
    protected void initialize()
    {
        initTypeInfo();
    }

    public int getJDBCType()
    {
        return Types.OTHER;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping#setObject(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    public void setObject(PreparedStatement ps, int exprIndex, Object value)
    {
        try
        {
            ps.setObject(exprIndex, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "Object", "" + value, column, e.getMessage()), e);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping#getObject(java.sql.ResultSet, int)
     */
    @Override
    public Object getObject(ResultSet resultSet, int exprIndex)
    {
        try
        {
            return resultSet.getObject(exprIndex);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","Boolean", "" + exprIndex, column, e.getMessage()), e);
        }
    }
}
