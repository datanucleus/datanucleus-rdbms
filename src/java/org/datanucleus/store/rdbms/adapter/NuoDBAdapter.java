/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.adapter;

import java.sql.DatabaseMetaData;

import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.Index;

/**
 * Adapter for NuoDB (http://www.nuodb.com).
 * Note that this is a start point, but currently (2.0.1) the NuoDB JDBC driver is limited in what it implements
 * and how well it follows JDBC conventions (or not).
 * Issue 1 : DMD.getIndexInfo will return "ordinal_position" with origin 0 instead of 1 (see TableImpl line 1170).
 * Issue 2 : DMD.getSQLKeywords is not implemented
 * Issue 3 : DMD max lengths are returned as 0!
 * Note that Issue 1 and Issue 2 should be fixed in v2.0.2 when released
 */
public class NuoDBAdapter extends BaseDatastoreAdapter
{
    public NuoDBAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        supportedOptions.remove(DEFERRED_CONSTRAINTS);

        // NuoDB JDBC driver doesn't specify lengths
        if (maxTableNameLength <= 0)
        {
            maxTableNameLength = 128;
        }
        if (maxColumnNameLength <= 0)
        {
            maxColumnNameLength = 128;
        }
        if (maxConstraintNameLength <= 0)
        {
            maxConstraintNameLength = 128;
        }
        if (maxIndexNameLength <= 0)
        {
            maxIndexNameLength = 128;
        }
    }

    /**
     * Returns the appropriate DDL to create an index.
     * It should return something like:
     * <pre>
     * CREATE INDEX FOO_N1 ON FOO (BAR,BAZ) [Extended Settings]
     * CREATE UNIQUE INDEX FOO_U1 ON FOO (BAR,BAZ) [Extended Settings]
     * </pre>
     * @param idx An object describing the index.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    public String getCreateIndexStatement(Index idx, IdentifierFactory factory)
    {
        String idxIdentifier = factory.getIdentifierInAdapterCase(idx.getName());
        return "CREATE " + (idx.getUnique() ? "UNIQUE " : "") + "INDEX " + idxIdentifier + 
           " ON " + idx.getTable().toString() + ' ' +
           idx + (idx.getExtendedIndexSettings() == null ? "" : " " + idx.getExtendedIndexSettings());
    }
}