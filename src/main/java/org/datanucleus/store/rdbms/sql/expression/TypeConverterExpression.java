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
package org.datanucleus.store.rdbms.sql.expression;

import java.util.Date;
import java.util.List;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.TypeConverterMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Wrapper expression handler for a TypeConverterMapping to avoid the need to have an explicit mapping for something using a TypeConverter.
 */
public class TypeConverterExpression extends DelegatedExpression
{
    public TypeConverterExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
        if (!(mapping instanceof TypeConverterMapping))
        {
            throw new NucleusException(Localiser.msg("060019", mapping.getClass().getName()));
        }

        TypeConverterMapping convMapping = (TypeConverterMapping)mapping;
        TypeConverter conv = convMapping.getTypeConverter();
        Class datastoreType = stmt.getRDBMSManager().getNucleusContext().getTypeManager().getDatastoreTypeForTypeConverter(conv, convMapping.getJavaType());
        if (datastoreType == String.class)
        {
            delegate = new StringExpression(stmt, table, mapping);
        }
        else if (Date.class.isAssignableFrom(datastoreType))
        {
            delegate = new TemporalExpression(stmt, table, mapping);
        }
        else if (Number.class.isAssignableFrom(datastoreType))
        {
            delegate = new NumericExpression(stmt, table, mapping);
        }
        else if (Byte[].class.isAssignableFrom(datastoreType) || byte[].class.isAssignableFrom(datastoreType)) // TODO Any other types to use as BinaryExpression?
        {
            delegate = new BinaryExpression(stmt, table, mapping);
        }
        else
        {
            throw new NucleusException(Localiser.msg("060017", mapping.getClass().getName(), datastoreType.getName()));
        }
    }

    @Override
    public SQLExpression invoke(String methodName, List<SQLExpression> args)
    {
        String typeName = mapping.getJavaType().getName();

        try
        {
            return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, typeName, methodName, this, args);
        }
        catch (NucleusException ne)
        {
            if (delegate instanceof StringExpression)
            {
                // Special case of conversion to a String, so try to invoke the method on the String.
                typeName = String.class.getName();
                NucleusLogger.QUERY.info(Localiser.msg("060018", methodName, mapping.getJavaType().getName(), typeName));
                return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, typeName, methodName, this, args);
            }
            throw ne;
        }
    }
}
