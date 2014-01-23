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
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * A <tt>IncompatibleDataTypeException</tt> is thrown if a column is detected to
 * have an incompatible type in the database during schema validation.
 *
 * @see Column
 */
public class IncompatibleDataTypeException extends DatastoreValidationException
{
    private static final Localiser LOCALISER_RDBMS=Localiser.getInstance("org.datanucleus.store.rdbms.Localisation",
        RDBMSStoreManager.class.getClassLoader());

    /**
     * Constructs an incompatible data type exception.
     * @param column        The column having an incompatible data type.
     * @param expectedType  The expected java.sql.Type of the column taken from the metadata.
     * @param actualType    The actual java.sql.Type of the column taken from the database.
     */
    public IncompatibleDataTypeException(Column column, int expectedType, int actualType)
    {
        super(LOCALISER_RDBMS.msg("020009", column, 
            column.getStoreManager().getDatastoreAdapter().getNameForJDBCType(actualType), 
            column.getStoreManager().getDatastoreAdapter().getNameForJDBCType(expectedType)));
    }
}