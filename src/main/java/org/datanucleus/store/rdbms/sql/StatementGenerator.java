/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved. 
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

/**
 * Generator of SQLStatements.
 */
public interface StatementGenerator
{
    /** Option to allow null values as elements of an array or collection. */
    static final String OPTION_ALLOW_NULLS = "allowNulls";

    /** Option to add "NUCLEUS_TYPE" to the SELECT clause as kind of discriminator. */
    static final String OPTION_SELECT_NUCLEUS_TYPE = "selectNucleusType";

    /** Option to add a WHERE clause to restrict to the discriminator value. */
    static final String OPTION_RESTRICT_DISCRIM = "restrictDiscriminator";

    /**
     * Accessor for the statement.
     * @return The SQLStatement
     */
    SQLStatement getStatement();

    /**
     * Method to set the parent statement.
     * Must be set before calling getStatement().
     * @param stmt The parent statement
     */
    void setParentStatement(SQLStatement stmt);

    /**
     * Method to set a property.
     * @param name Name of the property
     * @return This generator
     */
    StatementGenerator setOption(String name);

    /**
     * Method to unset a property.
     * @param name Name of the property
     * @return This generator
     */
    StatementGenerator unsetOption(String name);

    /**
     * Whether the generator has a particular option set.
     * @param name Name of the option
     * @return Whether it is set
     */
    boolean hasOption(String name);
}