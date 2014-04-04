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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.TypeConverterMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;

/**
 * Wrapper literal handler for a TypeConverterMapping to avoid the need to have an explicit mapping for something using a TypeConverter.
 */
public class TypeConverterLiteral extends DelegatedExpression implements SQLLiteral
{
    public TypeConverterLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        if (!(mapping instanceof TypeConverterMapping))
        {
            throw new NucleusException("Attempt to create TypeConverterLiteral for mapping of type " + mapping.getClass().getName());
        }

        TypeConverterMapping convMapping = (TypeConverterMapping)mapping;
        TypeConverter conv = convMapping.getTypeConverter();
        Class datastoreType = TypeConverterHelper.getDatastoreTypeForTypeConverter(conv, convMapping.getJavaType());
        if (datastoreType == String.class)
        {
            delegate = new StringLiteral(stmt, mapping, value, parameterName);
        }
        else if (Date.class.isAssignableFrom(datastoreType))
        {
            delegate = new TemporalLiteral(stmt, mapping, value, parameterName);
        }
        else if (datastoreType == Integer.class || datastoreType == Long.class || datastoreType == Short.class || datastoreType == BigInteger.class)
        {
            delegate = new IntegerLiteral(stmt, mapping, value, parameterName);
        }
        else if (datastoreType == Double.class || datastoreType == Float.class || datastoreType == BigDecimal.class)
        {
            delegate = new FloatingPointLiteral(stmt, mapping, value, parameterName);
        }
        else if (datastoreType == Boolean.class)
        {
            delegate = new BooleanLiteral(stmt, mapping, value, parameterName);
        }
        else if (datastoreType == Byte.class)
        {
            delegate = new ByteLiteral(stmt, mapping, value, parameterName);
        }
        else if (datastoreType == Character.class)
        {
            delegate = new CharacterLiteral(stmt, mapping, value, parameterName);
        }
        else if (datastoreType == Enum.class)
        {
            delegate = new EnumLiteral(stmt, mapping, value, parameterName);
        }
        else
        {
            throw new NucleusException("Could not create TypeConverterLiteral for mapping of type " + mapping.getClass().getName() + " with datastoreType=" + datastoreType.getName() +
                " - no available supported expression");
        }
    }

    public Object getValue()
    {
        return ((SQLLiteral)delegate).getValue();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#setNotParameter()
     */
    public void setNotParameter()
    {
        ((SQLLiteral)delegate).setNotParameter();
    }
}