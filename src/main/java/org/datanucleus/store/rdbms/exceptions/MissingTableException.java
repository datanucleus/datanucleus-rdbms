/**********************************************************************
Copyright (c) 2002 Mike Martin and others. All rights reserved.
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
    Andy Jefferson - coding standards
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.exceptions;

import org.datanucleus.store.exceptions.DatastoreValidationException;
import org.datanucleus.util.Localiser;

/**
 * A <tt>MissingTableException</tt> is thrown if an expected table is
 * not found in the database during schema validation.
 */
public class MissingTableException extends DatastoreValidationException
{
    private static final long serialVersionUID = 8360855107029754952L;

    /**
     * Constructs a missing table exception.
     * @param catalogName Catalog name which the table was searched.
     * @param schemaName Schema name which the table was searched.
     * @param tableName Name of the table that was missing.
     */
    public MissingTableException(String catalogName, String schemaName, String tableName)
    {
        super(Localiser.msg("020011", catalogName, schemaName, tableName));
    }
}