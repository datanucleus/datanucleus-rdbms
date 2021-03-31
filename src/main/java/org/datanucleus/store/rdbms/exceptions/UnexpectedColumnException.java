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

import org.datanucleus.exceptions.DatastoreValidationException;
import org.datanucleus.util.Localiser;

/**
 * A <i>UnexpectedColumnException</i> is thrown if an unexpected column is
 * encountered in the database during schema validation.
 */
public class UnexpectedColumnException extends DatastoreValidationException
{
    private static final long serialVersionUID = 804974383075712052L;

    /**
     * Constructs a unexpected column exception.
     * @param table_name The table in which the column was found.
     * @param column_name The name of the unexpected column.
     * @param schema_name The schema name
     * @param catalog_name The catalog name
     */
    public UnexpectedColumnException(String table_name, String column_name, String schema_name, String catalog_name)
    {
        super(Localiser.msg("020024",column_name,table_name,schema_name, catalog_name));
    }
}