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

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.key.PrimaryKey;

/**
 * Adapter for NuoDB (http://www.nuodb.com).
 * This adapter was written based on v2.0.2 of the NuoDB JDBC driver.
 */
public class NuoDBAdapter extends BaseDatastoreAdapter
{
    public NuoDBAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        supportedOptions.add(SEQUENCES);
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);
        supportedOptions.add(LOCK_WITH_SELECT_FOR_UPDATE);

        // NuoDB JDBC driver doesn't specify lengths (as of v2.0.2)
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

        // CROSS JOIN syntax is not supported
        supportedOptions.remove(ANSI_CROSSJOIN_SYNTAX);
        supportedOptions.add(CROSSJOIN_ASINNER11_SYNTAX);

        // Doesn't seem to support RESTRICT FK constraints
        supportedOptions.remove(FK_DELETE_ACTION_RESTRICT);
        supportedOptions.remove(FK_UPDATE_ACTION_RESTRICT);
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

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        return "DROP SCHEMA " + schemaName + " CASCADE";
    }

    /**
     * Firebird accepts the PK in the CREATE TABLE statement.
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The PK statement
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        // We generate the PK in the CREATE TABLE statement
        return null;
    }

    /**
     * Accessor for the sequence statement to create the sequence.
     * @param sequence_name Name of the sequence 
     * @param min Minimum value for the sequence
     * @param max Maximum value for the sequence
     * @param start Start value for the sequence
     * @param increment Increment value for the sequence
     * @param cache_size Cache size for the sequence
     * @return The statement for getting the next id from the sequence
     */
    public String getSequenceCreateStmt(String sequence_name,
            Integer min, Integer max, Integer start, Integer increment, Integer cache_size)
    {
        if (sequence_name == null)
        {
            throw new NucleusUserException(LOCALISER.msg("051028"));
        }

        StringBuilder stmt = new StringBuilder("CREATE SEQUENCE ");
        stmt.append(sequence_name);
        if (start != null)
        {
            stmt.append(" START WITH " + start);
        }
        // TODO Support other parameters if NuoDB ever supports them

        return stmt.toString();
    }

    /**
     * Accessor for the statement for getting the next id from the sequence for this datastore.
     * @param sequence_name Name of the sequence 
     * @return The statement for getting the next id for the sequence
     **/
    public String getSequenceNextStmt(String sequence_name)
    {
        if (sequence_name == null)
        {
            throw new NucleusUserException(LOCALISER.msg("051028"));
        }
        StringBuilder stmt=new StringBuilder("SELECT NEXT VALUE FOR ");
        stmt.append(sequence_name);
        stmt.append(" FROM DUAL");

        return stmt.toString();
    }

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @return The keyword for a column using auto-increment
     */
    public String getAutoIncrementKeyword()
    {
        return "GENERATED BY DEFAULT AS IDENTITY";
    }

    /**
     * Method to return the SQL to append to the WHERE clause of a SELECT statement to handle
     * restriction of ranges using the OFFSET/FETCH keywords.
     * @param offset The offset to return from
     * @param count The number of items to return
     * @return The SQL to append to allow for ranges using OFFSET/FETCH.
     */
    public String getRangeByLimitEndOfStatementClause(long offset, long count)
    {
        if (datastoreMajorVersion < 10 || (datastoreMajorVersion == 10 && datastoreMinorVersion < 5))
        {
            return super.getRangeByLimitEndOfStatementClause(offset, count);
        }
        else if (offset <= 0 && count <= 0)
        {
            return "";
        }

        StringBuilder str = new StringBuilder();
        if (offset > 0)
        {
            str.append("OFFSET " + offset + (offset > 1 ? " ROWS " : " ROW "));
        }
        if (count > 0)
        {
            str.append("FETCH NEXT " + (count > 1 ? (count + " ROWS ONLY ") : "ROW ONLY "));
        }
        return str.toString();
    }
}