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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.column;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.exceptions.NullValueException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Mapping of a REAL column.
 */
public class RealColumnMapping extends AbstractColumnMapping
{
    public RealColumnMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
		super(storeMgr, mapping);
		column = col;
		initialize();
	}
    
    private void initialize()
    {
        if (column != null)
        {
            column.checkPrimitive();
        }
		initTypeInfo();
    }

    /**
     * Accessor for whether the mapping is decimal-based.
     * @return Whether the mapping is decimal based
     */
    public boolean isDecimalBased()
    {
        return true;
    }

    public int getJDBCType()
    {
        return Types.REAL;
    }

    public void setFloat(PreparedStatement ps, int param, float value)
    {
        try
        {
            ps.setFloat(param, value);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","float","" + value, column, e.getMessage()), e);
        }
    }

    public float getFloat(ResultSet rs, int param)
    {
        float value;

        try
        {
            value = rs.getFloat(param);
            if (column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull() )
            {
                if (rs.wasNull())
                {
                    throw new NullValueException(Localiser.msg("055003",column));
                }
            }
        }
        catch (SQLException e)
        {           
            try
            {
                // when value is real in database, cause cause a parse error when calling getFloat
                // JDBC error:Value can not be converted to requested type.
                value = Float.parseFloat(rs.getString(param));
                if( column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull() )
                {
                    if (rs.wasNull())
                    {
                        throw new NullValueException(Localiser.msg("055003",column));
                    }
                }
            }
            catch (SQLException e1)
            {
                try
                {
                    throw new NucleusDataStoreException("Can't get float result: param = " + param + " - " +rs.getString(param), e);
                }
                catch (SQLException e2)
                {
                    throw new NucleusDataStoreException(Localiser.msg("055002","float","" + param, column, e.getMessage()), e);
                }
            }
        }

        return value;
    }

    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            if (value == null)
            {
                if (useDefaultWhenNull())
                {
                    ps.setFloat(param, Float.valueOf(column.getDefaultValue().toString()).floatValue());
                }
                else
                {
                    ps.setNull(param, getJDBCType());
                }
            }
            else
            {
                ps.setFloat(param, ((Float)value).floatValue());
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001","Object","" + value, column, e.getMessage()), e);
        }
    }

    public Object getObject(ResultSet rs, int param)
    {
        Object value;

        try
        {
            float f = rs.getFloat(param);
            value = rs.wasNull() ? null : f;
        }
		catch (SQLException e)
		{
            try
            {
                // when value is real in database, cause cause a parse error when calling getFloat
                // JDBC error:Value can not be converted to requested type.
                value = Float.valueOf(Float.parseFloat(rs.getString(param)));
                if (column == null || column.getColumnMetaData() == null || !column.getColumnMetaData().isAllowsNull() )
                {
                    if (rs.wasNull())
                    {
                        throw new NullValueException(Localiser.msg("055003",column));
                    }
                }
            }
            catch (SQLException e1)
            {
                try
                {
                    throw new NucleusDataStoreException("Can't get float result: param = " + param + " - " + rs.getString(param), e);
                }
                catch (SQLException e2)
                {
                    throw new NucleusDataStoreException(Localiser.msg("055002", "Object", "" + param, column, e.getMessage()), e);
                }
            }
        }

        return value;
    }
}