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

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.util.Localiser;

/**
 * A <tt>TooManyForeignKeysException</tt> is thrown when trying to add a foreign
 * key to a table and the table already has the maximum allowed number of
 * foreign keys.
 */
public class TooManyForeignKeysException extends NucleusDataStoreException
{
    protected static final Localiser LOCALISER=Localiser.getInstance(
        "org.datanucleus.Localisation", org.datanucleus.ClassConstants.NUCLEUS_CONTEXT_LOADER);

    /**
     * Constructs a too-many-foreign-keys exception.
     * @param dba the database adapter
     * @param table_name Name of the table with too many FKs
     */
    public TooManyForeignKeysException(DatastoreAdapter dba, String table_name)
    {
        super(LOCALISER.msg("020015","" + dba.getMaxForeignKeys(),table_name));
        setFatal();
    }
}