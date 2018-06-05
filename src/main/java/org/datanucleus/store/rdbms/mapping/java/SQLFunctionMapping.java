/**********************************************************************
Copyright (c) 2017 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.mapping.java;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.datanucleus.ExecutionContext;
import org.datanucleus.store.rdbms.mapping.column.ColumnMapping;
import org.datanucleus.store.rdbms.mapping.column.ColumnMappingFactory;

/**
 * Mapping to represent the return value of an SQL function invocation.
 * With a generalised SQL function call we do not know the type of the function result, so only create the datastore mapping on processing of the first result.
 */
public class SQLFunctionMapping extends SingleFieldMapping
{
    Class javaType = null;

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping#getJavaType()
     */
    @Override
    public Class getJavaType()
    {
        return javaType;
    }

    /**
     * Method to prepare a field mapping for use in the datastore.
     * This creates the column in the table.
     */
    protected void prepareDatastoreMapping()
    {
        // No datastore mapping created at this point; created on first result processing
    }

    public Object getObject(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return null;
        }

        if (columnMappings == null || columnMappings.length == 0)
        {
            // Set the datastoreMapping and javaType the first time this is used. Get the type from the ResultSetMetaData.
            try
            {
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int colType = rsmd.getColumnType(exprIndex[0]);
                if (colType == Types.DOUBLE || colType == Types.DECIMAL)
                {
                    javaType = Double.class;
                }
                else if (colType == Types.FLOAT)
                {
                    javaType = Float.class;
                }
                else if (colType == Types.BOOLEAN)
                {
                    javaType = Boolean.class;
                }
                else if (colType == Types.INTEGER || colType == Types.NUMERIC)
                {
                    javaType = Integer.class;
                }
                else if (colType == Types.SMALLINT || colType == Types.TINYINT)
                {
                    javaType = Short.class;
                }
                else if (colType == Types.BIGINT)
                {
                    javaType = BigInteger.class;
                }
                else if (colType == Types.LONGVARCHAR || colType == Types.VARCHAR || colType == Types.NVARCHAR || colType == Types.CHAR || colType == Types.NCHAR)
                {
                    javaType = String.class;
                }
                else
                {
                    javaType = Object.class;
                }
                // TODO Provide more comprehensive support for all types
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }

            Class datastoreMappingClass = storeMgr.getDatastoreAdapter().getColumnMappingClass(javaType.getName(), null, null, ec.getClassLoaderResolver(), null);
            ColumnMapping datastoreMapping = ColumnMappingFactory.createMapping(datastoreMappingClass, this, storeMgr, null);
            columnMappings = new ColumnMapping[1];
            columnMappings[0] = datastoreMapping;
        }

        if (javaType == Double.class)
        {
            return getColumnMapping(0).getDouble(resultSet, exprIndex[0]);
        }
        else if (javaType == Float.class)
        {
            return getColumnMapping(0).getFloat(resultSet, exprIndex[0]);
        }
        else if (javaType == Integer.class)
        {
            return getColumnMapping(0).getInt(resultSet, exprIndex[0]);
        }
        else if (javaType == Long.class)
        {
            return getColumnMapping(0).getLong(resultSet, exprIndex[0]);
        }
        else if (javaType == Short.class)
        {
            return getColumnMapping(0).getShort(resultSet, exprIndex[0]);
        }
        else if (javaType == BigInteger.class)
        {
            return getColumnMapping(0).getLong(resultSet, exprIndex[0]);
        }
        else if (javaType == Boolean.class)
        {
            return getColumnMapping(0).getBoolean(resultSet, exprIndex[0]);
        }
        else if (javaType == String.class)
        {
            return getColumnMapping(0).getString(resultSet, exprIndex[0]);
        }

        return getColumnMapping(0).getObject(resultSet, exprIndex[0]);
    }
}