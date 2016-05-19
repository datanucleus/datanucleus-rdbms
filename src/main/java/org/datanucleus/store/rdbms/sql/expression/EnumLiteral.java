/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
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

import org.datanucleus.ClassNameConstants;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.EnumMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.util.TypeConversionHelper;

/**
 * Representation of an Enum literal.
 */
public class EnumLiteral extends EnumExpression implements SQLLiteral
{
    private final Enum value;

    /**
     * Constructor for an Enum literal with a value.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Name of the parameter that this represents if any (as JDBC "?")
     */
    public EnumLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
        }
        else if (value instanceof Enum)
        {
            this.value = (Enum)value;
        }
        else
        {
            throw new NucleusException("Cannot create " + this.getClass().getName() + " for value of type " + value.getClass().getName());
        }

        if (mapping.getJavaTypeForDatastoreMapping(0).equals(ClassNameConstants.JAVA_LANG_STRING))
        {
            delegate = new StringLiteral(stmt, mapping, getStoredValueForEnum(mapping), parameterName);
        }
        else
        {
            delegate = new IntegerLiteral(stmt, mapping, getStoredValueForEnum(mapping), parameterName);
        }
    }

    @Override
    public void setJavaTypeMapping(JavaTypeMapping mapping)
    {
        super.setJavaTypeMapping(mapping);

        // Reset the delegate in case it has changed
        if (mapping.getJavaTypeForDatastoreMapping(0).equals(ClassNameConstants.JAVA_LANG_STRING))
        {
            delegate = new StringLiteral(stmt, mapping, getStoredValueForEnum(mapping), parameterName);
        }
        else
        {
            delegate = new IntegerLiteral(stmt, mapping, getStoredValueForEnum(mapping), parameterName);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#getValue()
     */
    public Object getValue()
    {
        return value;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#isParameter()
     */
    @Override
    public boolean isParameter()
    {
        return delegate.isParameter();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#setNotParameter()
     */
    public void setNotParameter()
    {
        ((SQLLiteral)delegate).setNotParameter();
    }

    private Object getStoredValueForEnum(JavaTypeMapping mapping)
    {
        Object val = null;
        if (this.value != null)
        {
            val = this.value.ordinal();
            if (mapping instanceof EnumMapping)
            {
                EnumMapping m = (EnumMapping)mapping;
                if (m.getMemberMetaData() != null)
                {
                    val = TypeConversionHelper.getStoredValueFromEnum(m.getMemberMetaData(), m.getRoleForMember(), this.value);
                }
                else
                {
                    return (mapping.getJavaTypeForDatastoreMapping(0).equals(ClassNameConstants.JAVA_LANG_STRING)) ? this.value.name() : this.value.ordinal();
                }
            }
        }
        return val;
    }
}