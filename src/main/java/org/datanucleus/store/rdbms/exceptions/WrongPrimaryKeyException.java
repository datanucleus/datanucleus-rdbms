/**********************************************************************
Copyright (c) 2003 Mike Martin and others. All rights reserved.
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
 * A <tt>WrongPrimaryKeyException</tt> is thrown if a table is detected not to
 * have the expected primary key in the database during schema validation.
 *
 * @see org.datanucleus.store.rdbms.table.Column
 */
public class WrongPrimaryKeyException extends DatastoreValidationException
{
    private static final long serialVersionUID = 8688198223306432767L;

    /**
     * Constructs a wrong primary key exception.
     * @param table_name The table having the wrong primary key.
     * @param expected_pk The expected primary key of the table.
     * @param actual_pks The actual primary key(s) of the table.
     */
    public WrongPrimaryKeyException(String table_name, String expected_pk, String actual_pks)
    {
        super(Localiser.msg("020020",table_name,expected_pk,actual_pks));
    }
}