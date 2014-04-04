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

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.TypeConverterMultiMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * Literal for an object persisted using TypeConverterMultiMapping.
 */
public class TypeConverterMultiLiteral extends TypeConverterMultiExpression implements SQLLiteral
{
    Object value;

    public TypeConverterMultiLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
        }
        else
        {
            if (mapping instanceof TypeConverterMultiMapping && mapping.getJavaType().isAssignableFrom(value.getClass()))
            {
                this.value = value;
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#getValue()
     */
    @Override
    public Object getValue()
    {
        return value;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLLiteral#setNotParameter()
     */
    @Override
    public void setNotParameter()
    {
        // TODO Auto-generated method stub
    }
}
