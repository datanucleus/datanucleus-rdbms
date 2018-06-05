/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.java.FileMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * Mapping of a BinaryStream column.
 * Can be used to stream the contents of a File to/from the datastore.
 */
public class BinaryStreamColumnMapping extends AbstractColumnMapping
{
    public BinaryStreamColumnMapping(JavaTypeMapping mapping, RDBMSStoreManager storeMgr, Column col)
    {
        super(storeMgr, mapping);
        column = col;
        initialize();
    }

    private void initialize()
    {
        initTypeInfo();
    }

    public int getJDBCType()
    {
        return Types.LONGVARBINARY;
    }

    /*
     * (non-Javadoc)
     * @see
     * org.datanucleus.store.rdbms.mapping.column.AbstractDatastoreMapping#setObject(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    public void setObject(PreparedStatement ps, int param, Object value)
    {
        try
        {
            if (value == null)
            {
                ps.setNull(param, getJDBCType());
            }
            else if (value instanceof File)
            {
                File file = (File) value;
                ps.setBinaryStream(param, new FileInputStream(file), (int) file.length());
            }
            else
            {
                // TODO Support other types
                throw new NucleusDataStoreException("setObject unsupported for java type " + value.getClass().getName());
            }
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "Object", "" + value, column, e.getMessage()), e);
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055001", "Object", "" + value, column, e.getMessage()), e);
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.datanucleus.store.rdbms.mapping.column.AbstractDatastoreMapping#getObject(java.sql.ResultSet, int)
     */
    @Override
    public Object getObject(ResultSet resultSet, int param)
    {
        Object so = null;

        try
        {
            InputStream is = resultSet.getBinaryStream(param);
            if (!resultSet.wasNull())
            {
                if (getJavaTypeMapping() instanceof FileMapping)
                {
                    so = StreamableSpooler.instance().spoolStream(is);
                }
                else
                {
                    // TODO Support other types
                    throw new NucleusDataStoreException("getObject unsupported for java type mapping of type " + getJavaTypeMapping());
                }
            }
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "Object", "" + param, column, e.getMessage()), e);
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("055002", "Object", "" + param, column, e.getMessage()), e);
        }

        return so;
    }
}