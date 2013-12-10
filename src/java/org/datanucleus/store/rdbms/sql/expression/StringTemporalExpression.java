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
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;

/**
 * Expression representing a field/property that can be stored as a String or as a Temporal.
 * For example a org.joda.time.DateTime can be represented using this.
 * Delegates any operation to the same operation on the delegate.
 */
public class StringTemporalExpression extends DelegatedExpression
{
    /**
     * Constructor for an expression for a field/property represented as String/Temporal.
     * @param stmt The SQL statement
     * @param table Table
     * @param mapping Mapping
     */
    public StringTemporalExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
        if (mapping.getJavaTypeForDatastoreMapping(0).equals(ClassNameConstants.JAVA_LANG_STRING))
        {
            delegate = new StringExpression(stmt, table, mapping);
        }
        else
        {
            delegate = new TemporalExpression(stmt, table, mapping);
        }
    }
}