/**********************************************************************
Copyright (c) 2003 Erik Bengtson and others. All rights reserved.
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
2004 Andy Jefferson - updated to specify the transaction isolation level limitation
2004 Andy Jefferson - changed to use CREATE TABLE of DatabaseAdapter
2004 Andy Jefferson - added MappingManager
2007 R Scott - Sequence support
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.StoreSchemaHandler;

/**
 * Provides methods for adapting SQL language elements to the Hypersonic SQL Server database.
 */
public class HSQLAdapter extends BaseDatastoreAdapter
{
    /**
     * Constructs a Hypersonic SQL adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public HSQLAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);
        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(SEQUENCES);
        supportedOptions.add(UNIQUE_IN_END_CREATE_STATEMENTS);
        if (datastoreMajorVersion < 2)
        {
            // HSQL 2.0 introduced support for batching and use of getGeneratedKeys
            supportedOptions.remove(STATEMENT_BATCHING);
            supportedOptions.remove(GET_GENERATED_KEYS_STATEMENT);
        }
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.remove(CHECK_IN_CREATE_STATEMENTS);
        supportedOptions.remove(AUTO_INCREMENT_KEYS_NULL_SPECIFICATION);
        if (datastoreMajorVersion >= 2)
        {
            // HSQL 2.0 introduced SELECT ... FOR UPDATE
            supportedOptions.add(LOCK_WITH_SELECT_FOR_UPDATE);
            supportedOptions.add(ORDERBY_NULLS_DIRECTIVES); // Likely came in at 2.0
        }

        // HSQL Introduced in v 1.7.2 the use of CHECK, but ONLY as statements
        // at the end of the CREATE TABLE statement. We currently don't support
        // anything there other than primary key.
        // This was introduced in version 1.7.2
        if (datastoreMajorVersion < 1 ||
            (datastoreMajorVersion == 1 && datastoreMinorVersion < 7) ||
            (datastoreMajorVersion == 1 && datastoreMinorVersion == 7 && datastoreRevisionVersion < 2))
        {
            supportedOptions.remove(CHECK_IN_END_CREATE_STATEMENTS);
        }
        else
        {
            supportedOptions.add(CHECK_IN_END_CREATE_STATEMENTS);
        }

        // Cannot access parent query columns in a subquery (at least in HSQLDB 1.8)
        supportedOptions.remove(ACCESS_PARENTQUERY_IN_SUBQUERY_JOINED);

        if (datastoreMajorVersion < 1 || (datastoreMajorVersion == 1 && datastoreMinorVersion < 7))
        {
            // HSQL before 1.7.* doesn't support foreign keys
            supportedOptions.remove(FK_DELETE_ACTION_CASCADE);
            supportedOptions.remove(FK_DELETE_ACTION_RESTRICT);
            supportedOptions.remove(FK_DELETE_ACTION_DEFAULT);
            supportedOptions.remove(FK_DELETE_ACTION_NULL);
            supportedOptions.remove(FK_UPDATE_ACTION_CASCADE);
            supportedOptions.remove(FK_UPDATE_ACTION_RESTRICT);
            supportedOptions.remove(FK_UPDATE_ACTION_DEFAULT);
            supportedOptions.remove(FK_UPDATE_ACTION_NULL);
        }
        else if (datastoreMajorVersion < 2)
        {
            // HSQL 2.0 introduced RESTRICT on FKs
            supportedOptions.remove(FK_DELETE_ACTION_RESTRICT);
            supportedOptions.remove(FK_UPDATE_ACTION_RESTRICT);
        }

        if (datastoreMajorVersion < 2)
        {
            // HSQL 2.0 introduced support for REPEATABLE_READ
            supportedOptions.remove(TX_ISOLATION_REPEATABLE_READ);
            if (datastoreMinorVersion <= 7)
            {
                // 1.8 introduced support for READ_UNCOMMITTED, SERIALIZABLE
                supportedOptions.remove(TX_ISOLATION_READ_COMMITTED);
                supportedOptions.remove(TX_ISOLATION_SERIALIZABLE);
            }
        }
        supportedOptions.remove(TX_ISOLATION_NONE);
    }

    /**
     * Initialise the types for this datastore.
     * @param handler SchemaHandler that we initialise the types for
     * @param mconn Managed connection to use
     */
    public void initialiseTypes(StoreSchemaHandler handler, ManagedConnection mconn)
    {
        super.initialiseTypes(handler, mconn);

        // Add on any missing JDBC types
        // CLOB - not present before v2.0
        SQLTypeInfo sqlType = new org.datanucleus.store.rdbms.schema.HSQLTypeInfo(
            "LONGVARCHAR", (short)Types.CLOB, 2147483647, "'", "'", null, 1, true, (short)3,
            false, false, false, "LONGVARCHAR", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.CLOB, sqlType, true);

        // BLOB - not present before v2.0
        sqlType = new org.datanucleus.store.rdbms.schema.HSQLTypeInfo(
            "LONGVARBINARY", (short)Types.BLOB, 2147483647, "'", "'", null, 1, false, (short)3,
            false, false, false, "LONGVARBINARY", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BLOB, sqlType, true);

        // LONGVARCHAR - not present in 2.0+
        sqlType = new org.datanucleus.store.rdbms.schema.HSQLTypeInfo(
            "LONGVARCHAR", (short)Types.LONGVARCHAR, 2147483647, "'", "'", null, 1, true, (short)3,
            false, false, false, "LONGVARCHAR", (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.LONGVARCHAR, sqlType, true);

        // Update any types that need extra info relative to the JDBC info
        Collection<SQLTypeInfo> sqlTypes = getSQLTypeInfoForJdbcType(handler, mconn, (short)Types.BLOB);
        if (sqlTypes != null)
        {
            Iterator<SQLTypeInfo> iter = sqlTypes.iterator();
            while (iter.hasNext())
            {
                sqlType = iter.next();
                sqlType.setAllowsPrecisionSpec(false); // Can't add precision on a BLOB
            }
        }

        sqlTypes = getSQLTypeInfoForJdbcType(handler, mconn, (short)Types.CLOB);
        if (sqlTypes != null)
        {
            Iterator<SQLTypeInfo> iter = sqlTypes.iterator();
            while (iter.hasNext())
            {
                sqlType = iter.next();
                sqlType.setAllowsPrecisionSpec(false); // Can't add precision on a CLOB
            }
        }

        sqlTypes = getSQLTypeInfoForJdbcType(handler, mconn, (short)Types.LONGVARBINARY);
        if (sqlTypes != null)
        {
            Iterator<SQLTypeInfo> iter = sqlTypes.iterator();
            while (iter.hasNext())
            {
                sqlType = iter.next();
                sqlType.setAllowsPrecisionSpec(false); // Can't add precision on a LONGVARBINARY
            }
        }

        sqlTypes = getSQLTypeInfoForJdbcType(handler, mconn, (short)Types.LONGVARCHAR);
        if (sqlTypes != null)
        {
            Iterator<SQLTypeInfo> iter = sqlTypes.iterator();
            while (iter.hasNext())
            {
                sqlType = iter.next();
                sqlType.setAllowsPrecisionSpec(false); // Can't add precision on a LONGVARCHAR
            }
        }
    }

    /**
     * Accessor for the vendor ID for this adapter.
     * @return The vendor ID
     */
    public String getVendorID()
    {
        return "hsql";
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

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        return "DROP SCHEMA IF EXISTS " + schemaName + " CASCADE";
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
            // HSQLDB doesn't allow just offset so use Integer.MAX_VALUE as count
            return "LIMIT " + Integer.MAX_VALUE + " OFFSET " + offset + " ";
        }
        else
        {
            return "";
        }
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new org.datanucleus.store.rdbms.schema.HSQLTypeInfo(rs);
    }

    /**
     * Accessor for the Schema Name for this datastore.
     * HSQL 1.7.0 does not support schemas (catalog)
     * 
     * @param conn Connection to the datastore
     * @return The schema name
     * @throws SQLException Thrown if error occurs in determining the schema name.
     */
    public String getSchemaName(Connection conn)
    throws SQLException
    {
        return "";
    }

    /**
     * HSQL 1.7.0 does not support ALTER TABLE to define a primary key
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The PK statement
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        return null;
    }

    /**
     * Returns the appropriate SQL to drop the given table.
     * It should return something like:
     * <blockquote><pre>
     * DROP TABLE FOO
     * </pre></blockquote>
     *
     * @param table The table to drop.
     * @return  The text of the SQL statement.
     */
    public String getDropTableStatement(Table table)
    {
        return "DROP TABLE " + table.toString();
    }

    /**
     * Accessor for the auto-increment sql statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest auto-increment key
     */
    public String getAutoIncrementStmt(Table table, String columnName)
    {
        return "CALL IDENTITY()";
    }

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @return The keyword for a column using auto-increment
     */
    public String getAutoIncrementKeyword()
    {
        // Note that we don't use "IDENTITY" here since that defaults to
        // GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY and this may not be intended as PK
        return "GENERATED BY DEFAULT AS IDENTITY";
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
        return "INSERT INTO " + table.toString() + " VALUES (null)";
    }

    /**
     * Accessor for whether the specified type is allow to be part of a PK.
     * @param datatype The JDBC type
     * @return Whether it is permitted in the PK
     */
    public boolean isValidPrimaryKeyType(int datatype)
    {
        if (datatype == Types.BLOB ||
            datatype == Types.CLOB ||
            datatype == Types.LONGVARBINARY ||
            datatype == Types.OTHER ||
            datatype == Types.LONGVARCHAR)
        {
            return false;
        }
        return true;
    }

    /**
     * Accessor for a statement that will return the statement to use to get the datastore date.
     * @return SQL statement to get the datastore date
     */
    public String getDatastoreDateStatement()
    {
        return "CALL NOW()";
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
            Integer min,Integer max,Integer start,Integer increment,Integer cache_size)
    {
        if (sequence_name == null)
        {
            throw new NucleusUserException(LOCALISER.msg("051028"));
        }

        StringBuffer stmt = new StringBuffer("CREATE SEQUENCE ");
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
            throw new NucleusUserException(LOCALISER.msg("051022"));
        }
        if (increment != null)
        {
            stmt.append(" INCREMENT BY " + increment);
        }
        if (cache_size != null)
        {
            throw new NucleusUserException(LOCALISER.msg("051023"));
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
            throw new NucleusUserException(LOCALISER.msg("051028"));
        }
        StringBuffer stmt=new StringBuffer("CALL NEXT VALUE FOR ");
        stmt.append(sequence_name);

        return stmt.toString();
    }
}