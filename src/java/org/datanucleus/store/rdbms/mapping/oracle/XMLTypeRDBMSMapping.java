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
package org.datanucleus.store.rdbms.mapping.oracle;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.schema.OracleTypeInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;

/**
 * Mapping for an Oracle XMLType type.
 **/
public class XMLTypeRDBMSMapping extends CharRDBMSMapping
{
    /**
     * Constructor.
     * @param mapping Java type mapping
     * @param storeMgr Store Manager
     * @param col Column
     */
    public XMLTypeRDBMSMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
        super(mapping, storeMgr, col);
    }

    protected void initialize()
    {
        initTypeInfo();
    }
    
    public SQLTypeInfo getTypeInfo()
    {
        return storeMgr.getSQLTypeInfoForJDBCType(OracleTypeInfo.TYPES_SYS_XMLTYPE);
    }

    /**
     * Method to extract a String from the ResultSet at the specified position
     * @param rs The Result Set
     * @param param The parameter position
     * @return the String
     */
    public String getString(ResultSet rs, int param)
    {
        String value = null;

        try
        {
            oracle.sql.OPAQUE o = (oracle.sql.OPAQUE)rs.getObject(param);
            if (o != null)
            {
                value = oracle.xdb.XMLType.createXML(o).getStringVal();
            }
            
            if (getDatastoreAdapter().supportsOption(DatastoreAdapter.NULL_EQUALS_EMPTY_STRING))
            {
                if (value != null && value.equals(getDatastoreAdapter().getSurrogateForEmptyStrings()))
                {
                    value = "";
                }
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055001","String","" + param, column, e.getMessage()), e);
        }

        return value;
    }

    /**
     * Method to set a String at the specified position in the JDBC PreparedStatement.
     * @param ps The PreparedStatement
     * @param param Parameter position
     * @param value The value to set
     */
    public void setString(PreparedStatement ps, int param, String value)
    {
        try
        {
            if (value == null)
            {
                if (column.isDefaultable() && column.getDefaultValue() != null)
                {
                    ps.setString(param,column.getDefaultValue().toString().trim());
                }
                else
                {
                    ps.setNull(param,getTypeInfo().getDataType(), "SYS.XMLTYPE");
                }
            }
            else
            {
                ps.setString(param, value);
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(LOCALISER_RDBMS.msg("055001","String","" + value, column, e.getMessage()), e);
        }
    }
}