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
package org.datanucleus.store.rdbms.exceptions;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.util.Localiser;

/**
 * A <tt>DuplicateColumnException</tt> is thrown if an attempt is made to
 * add a column to a table with a name already in-use by an existing column.
 */
public class DuplicateColumnException extends NucleusException
{
    private static final Localiser LOCALISER=Localiser.getInstance(
        "org.datanucleus.store.rdbms.Localisation", RDBMSStoreManager.class.getClassLoader());

    /** Column that cannot be created because it conflicts with existing column with same identifier. */
    private Column conflictingColumn;

    /**
     * Constructs a duplicate column name exception.
     * @param tableName Name of the table being initialized.
     * @param col1 Column we already have
     * @param col2 Column that we tried to create
     */
    public DuplicateColumnException(String tableName, Column col1, Column col2)
    {
        super(LOCALISER.msg("020007", col1.getIdentifier(), tableName,
            col1.getMemberMetaData() == null ?
                LOCALISER.msg("020008") :
                (col1.getMemberMetaData() != null ? col1.getMemberMetaData().getFullFieldName() : null),
            col2.getMemberMetaData() == null ?
                LOCALISER.msg("020008") :
                (col2.getMemberMetaData() != null ? col2.getMemberMetaData().getFullFieldName() : null)));
        this.conflictingColumn = col2;
        setFatal();
    }

    /**
     * Accessor for the column that could not be created because it conflicts with something already present.
     * @return The column
     */
    public Column getConflictingColumn()
    {
        return conflictingColumn;
    }
}