/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.mapping.column;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;

/**
 * Mapping of an SQLXML column.
 */
public class SqlXmlColumnMapping extends LongVarcharColumnMapping
{
    public SqlXmlColumnMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
        super(mapping, storeMgr, col);
    }

    public int getJDBCType()
    {
        return Types.SQLXML;
    }

    public void setString(PreparedStatement ps, int param, String value)
    {
        try
        {
            if (value == null)
            {
                ps.setNull(param, getJDBCType());
            }
            else
            {
                SQLXML sqlxml = ps.getConnection().createSQLXML();
                sqlxml.setString(value);
                ps.setSQLXML(param, sqlxml);
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","String","" + value), e);
        }
    }

    public String getString(ResultSet rs, int param)
    {
        String value;

        try
        {
            SQLXML sqlxml = rs.getSQLXML(param);
            value = sqlxml.getString();
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002","String", "" + param, column, e.getMessage()), e);
        }

        return value;
    }
}
