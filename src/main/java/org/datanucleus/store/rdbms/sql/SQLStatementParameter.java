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
package org.datanucleus.store.rdbms.sql;

import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.util.StringUtils;

/**
 * Representation of an SQLStatement parameter.
 */
public class SQLStatementParameter
{
    /** Name of the parameter (either its name, or its position). */
    final String name;

    /** Mapping for the value of the parameter. */
    JavaTypeMapping mapping;

    /** Optional number to define the column of the mapping that this represents. */
    final int columnNumber;

    /** Value to use for the parameter. */
    final Object value;

    /**
     * Constructor for a parameter using the mapping where there are multiple columns and
     * we are representing a particular column here.
     * @param name Name of the parameter
     * @param mapping Mapping for the parameter
     * @param value The value of the parameter
     * @param columnNumber NUmber of the column for the mapping
     */
    public SQLStatementParameter(String name, JavaTypeMapping mapping, Object value, int columnNumber)
    {
        this.mapping = mapping;
        this.value = value;
        this.name = name;
        this.columnNumber = columnNumber;
    }

    public String getName()
    {
        return name;
    }

    public JavaTypeMapping getMapping()
    {
        return mapping;
    }

    public void setMapping(JavaTypeMapping mapping)
    {
        this.mapping = mapping;
    }

    public int getColumnNumber()
    {
        return columnNumber;
    }

    public Object getValue()
    {
        return value;
    }

    public String toString()
    {
        return "SQLStatementParameter name=" + name + " mapping=" + mapping + 
            " value=" + StringUtils.toJVMIDString(value) + 
            (columnNumber >= 0 ? (" column=" + columnNumber) : "");
    }
}