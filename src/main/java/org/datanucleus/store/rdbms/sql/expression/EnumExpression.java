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
 * Expression representing an enum field/property.
 * An enum can be represented as a String or as a numeric hence requires its own expression.
 * Implemented as an internal delegate of the correct root expression type.
 */
public class EnumExpression extends DelegatedExpression
{
    /**
     * Constructor for an expression for an enum field/property.
     * @param stmt The SQL statement
     * @param table Table containing the enum
     * @param mapping Mapping for the enum
     */
    public EnumExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
        if (mapping.getJavaTypeForColumnMapping(0).equals(ClassNameConstants.JAVA_LANG_STRING))
        {
            delegate = new StringExpression(stmt, table, mapping);
        }
        else
        {
            delegate = new NumericExpression(stmt, table, mapping);
        }
    }

    @Override
    public void setJavaTypeMapping(JavaTypeMapping mapping)
    {
        super.setJavaTypeMapping(mapping);

        // Reset the delegate in case it has changed
        if (mapping.getJavaTypeForColumnMapping(0).equals(ClassNameConstants.JAVA_LANG_STRING))
        {
            delegate = new StringExpression(stmt, table, mapping);
        }
        else
        {
            delegate = new NumericExpression(stmt, table, mapping);
        }
    }
}