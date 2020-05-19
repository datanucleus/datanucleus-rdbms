/**********************************************************************
Copyright (c) 2020 Andy Jefferson and others. All rights reserved.
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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;

/**
 * Representation of a Binary literal.
 */
public class BinaryLiteral extends BinaryExpression
{
    private final Byte[] value;

    /**
     * Creates a binary (byte[]) literal.
     * @param stmt the SQL statement
     * @param mapping the mapping
     * @param value the value
     * @param parameterName Name of the parameter that this represents (as JDBC "?")
     */
    public BinaryLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value, String parameterName)
    {
        super(stmt, null, mapping);
        this.parameterName = parameterName;

        if (value == null)
        {
            this.value = null;
        }
        else if (value instanceof Byte[])
        {
            this.value = (Byte[])value;
        }
        else if (value instanceof byte[])
        {
            byte[] bytesValue = (byte[])value;
            this.value = new Byte[bytesValue.length];
            for (int i=0;i<bytesValue.length;i++)
            {
                this.value[i] = bytesValue[i];
            }
        }
        else
        {
            throw new NucleusException("Cannot create " + this.getClass().getName() + " for value of type " + value.getClass().getName());
        }

        if (parameterName != null)
        {
            st.appendParameter(parameterName, mapping, this.value);
        }
        else
        {
            setStatement();
        }
    }

    // TODO Define eq and ne methods

    public Object getValue()
    {
        return value;
    }

    protected void setStatement()
    {
        // TODO Don't think we can do this. Should only be supported when it is a parameter. Maybe throw exception in constructor
        st.append(String.valueOf(value));
    }
}