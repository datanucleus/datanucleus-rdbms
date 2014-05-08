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
import org.datanucleus.util.Localiser;

/**
 * A <tt>NotATableException</tt> is thrown during schema validation if a
 * table should be a table but is found not to be in the database.
 *
 * @see org.datanucleus.store.rdbms.table.TableImpl
 * @version $Revision: 1.3 $ 
 */
public class NotATableException extends DatastoreValidationException
{
    private static final Localiser LOCALISER_RDBMS=Localiser.getInstance("org.datanucleus.store.rdbms.Localisation",
        RDBMSStoreManager.class.getClassLoader());

    /**
     * Constructs a not-a-table exception.
     * @param tableName Name of the table that is of the wrong type.
     * @param type the type of the object
     */
    public NotATableException(String tableName, String type)
    {
        super(LOCALISER_RDBMS.msg("020012", tableName, type));
    }
}