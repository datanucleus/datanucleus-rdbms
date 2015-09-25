/**********************************************************************
Copyright (c) 2006 Andy Jefferson and others. All rights reserved.
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
2006 Thomas Mueller - updated the dialect for the H2 database engine
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Provides methods for adapting SQL language elements to the H2 Database Engine.
 */
public class H2Adapter extends BaseDatastoreAdapter
{
    private String schemaName;
    
    /**
     * Constructs a H2 adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public H2Adapter(DatabaseMetaData metadata)
    {
        super(metadata);

        // Set schema name
        try
        {
            ResultSet rs = metadata.getSchemas();
            while (rs.next())
            {
                if (rs.getBoolean("IS_DEFAULT"))
                {
                    schemaName = rs.getString("TABLE_SCHEM");
                }
            }
        }
        catch (SQLException e)
        {
            NucleusLogger.DATASTORE_SCHEMA.warn("Exception when trying to get default schema name for datastore", e);
        }

        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);
        supportedOptions.add(LOCK_WITH_SELECT_FOR_UPDATE);
        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(CHECK_IN_END_CREATE_STATEMENTS);
        supportedOptions.add(UNIQUE_IN_END_CREATE_STATEMENTS);
        supportedOptions.add(ORDERBY_NULLS_DIRECTIVES);
        supportedOptions.add(SEQUENCES);
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.remove(TX_ISOLATION_REPEATABLE_READ);
        supportedOptions.remove(TX_ISOLATION_NONE);

        // Create index before FK to avoid duplication since H2 automatically creates index for FK
        supportedOptions.add(CREATE_INDEXES_BEFORE_FOREIGN_KEYS);
    }

    /**
     * Accessor for the vendor ID for this adapter.
     * @return The vendor ID
     */
    public String getVendorID()
    {
        return "h2";
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new org.datanucleus.store.rdbms.schema.H2TypeInfo(rs);
    }

    /**
     * Method to return the maximum length of a datastore identifier of the specified type.
     * If no limit exists then returns -1
     * @param identifierType Type of identifier (see IdentifierFactory.TABLE, etc)
     * @return The max permitted length of this type of identifier
     */
    public int getDatastoreIdentifierMaxLength(IdentifierType identifierType)
    {
        if (identifierType == IdentifierType.TABLE)
        {
            return SQLConstants.MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.COLUMN)
        {
            return SQLConstants.MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.CANDIDATE_KEY)
        {
            return SQLConstants.MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.FOREIGN_KEY)
        {
            return SQLConstants.MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.INDEX)
        {
            return SQLConstants.MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.PRIMARY_KEY)
        {
            return SQLConstants.MAX_IDENTIFIER_LENGTH;
        }
        else if (identifierType == IdentifierType.SEQUENCE)
        {
            return SQLConstants.MAX_IDENTIFIER_LENGTH;
        }
        else
        {
            return super.getDatastoreIdentifierMaxLength(identifierType);
        }
    }

    public String getCreateDatabaseStatement(String catalogName, String schemaName)
    {
        return "CREATE SCHEMA IF NOT EXISTS " + schemaName;
    }

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        return "DROP SCHEMA IF EXISTS " + schemaName;
    }

    /**
     * Accessor for the SQL statement to add a column to a table.
     * @param table The table
     * @param col The column
     * @return The SQL necessary to add the column
     */
    public String getAddColumnStatement(Table table, Column col)
    {
        return "ALTER TABLE " + table.toString() + " ADD COLUMN " + col.getSQLDefinition();
    }

    /**
     * Method to return the SQL to append to the WHERE clause of a SELECT statement to handle
     * restriction of ranges using the LIMIT keyword.
     * @param offset The offset to return from
     * @param count The number of items to return
     * @return The SQL to append to allow for ranges using LIMIT.
     */
    public String getRangeByLimitEndOfStatementClause(long offset, long count)
    {
        if (offset >= 0 && count > 0)
        {
            return "LIMIT " + count + " OFFSET " + offset + " ";
        }
        else if (offset <= 0 && count > 0)
        {
            return "LIMIT " + count + " ";
        }
        else if (offset >= 0 && count < 0)
        {
            // H2 doesn't allow just offset so use Integer.MAX_VALUE as count
            return "LIMIT " + Integer.MAX_VALUE + " OFFSET " + offset + " ";
        }
        else
        {
            return "";
        }
    }

    /**
     * Accessor for the Schema Name for this datastore.
     * 
     * @param conn Connection to the datastore
     * @return The schema name
     **/
    public String getSchemaName(Connection conn)
    throws SQLException
    {
        return schemaName;
    }

    /**
     * HSQL 1.7.0 does not support ALTER TABLE to define a primary key
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The PK statement
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        // PK is created by the CREATE TABLE statement so we just return null
        return null;
    }

    /**
     * Accessor for the auto-increment sql statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest auto-increment key
     **/
    public String getAutoIncrementStmt(Table table, String columnName)
    {
        return "CALL IDENTITY()";
    }

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @return The keyword for a column using auto-increment
     **/
    public String getAutoIncrementKeyword()
    {
        return "IDENTITY";
    }

    /**
     * Method to retutn the INSERT statement to use when inserting into a table that has no
     * columns specified. This is the case when we have a single column in the table and that column
     * is autoincrement/identity (and so is assigned automatically in the datastore).
     * @param table The table
     * @return The INSERT statement
     */
    public String getInsertStatementForNoColumns(Table table)
    {
        return "INSERT INTO " + table.toString() + " VALUES(NULL)";
    }

    /**
     * Accessor for whether the specified type is allow to be part of a PK.
     * @param datatype The JDBC type
     * @return Whether it is permitted in the PK
     */
    public boolean isValidPrimaryKeyType(JdbcType datatype)
    {
        return true;
    }

    // ---------------------------- Sequence Support ---------------------------

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
            Integer min,Integer max, Integer start,Integer increment, Integer cache_size)
    {
        if (sequence_name == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }

        StringBuilder stmt = new StringBuilder("CREATE SEQUENCE IF NOT EXISTS ");
        stmt.append(sequence_name);
        if (min != null)
        {
            stmt.append(" START WITH " + min);
        }
        else if (start != null)
        {
            stmt.append(" START WITH " + start);
        }
        if (max != null)
        {
            throw new NucleusUserException(Localiser.msg("051022"));
        }
        if (increment != null)
        {
            stmt.append(" INCREMENT BY " + increment);
        }
        if (cache_size != null)
        {
            stmt.append(" CACHE " + cache_size);
        }

        return stmt.toString();
    }

    /**
     * Accessor for the statement for getting the next id from the sequence for this datastore.
     * @param sequence_name Name of the sequence 
     * @return The statement for getting the next id for the sequence
     */
    public String getSequenceNextStmt(String sequence_name)
    {
        if (sequence_name == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }
        StringBuilder stmt=new StringBuilder("CALL NEXT VALUE FOR ");
        stmt.append(sequence_name);

        return stmt.toString();
    }

    /**
     * return whether this exception represents a cancelled statement.
     * @param sqle the exception
     * @return whether it is a cancel
     */
    public boolean isStatementCancel(SQLException sqle)
    {
        if (sqle.getErrorCode() == 90051)
        {
            return true;
        }
        return false;
    }
}