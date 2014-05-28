/**********************************************************************
Copyright (c) 2002 Mike Martin (TJDO) and others. All rights reserved.
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
2003 Andy Jefferson - coding standards
2004 Andy Jefferson - added sequence methods
2004 Andy Jefferson - patch from Rey Amarego for "escape" method
2004 Andy Jefferson - update to cater for bitReallyBoolean (Tibor Kiss)
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Hashtable;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.schema.ForeignKeyInfo;
import org.datanucleus.store.rdbms.schema.PostgresqlTypeInfo;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to the PostgreSQL database.
 */
public class PostgreSQLAdapter extends BaseDatastoreAdapter
{
    /** List of Postgresql keywords that aren't in SQL92, SQL99 */
    public static final String POSTGRESQL_RESERVED_WORDS =
        "ALL,ANALYSE,ANALYZE,DO,FREEZE,ILIKE,ISNULL,OFFSET,PLACING,VERBOSE";
        
    protected Hashtable psqlTypes;

    /**
     * Constructor.
     * @param metadata MetaData for the DB
     */
    public PostgreSQLAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        if (datastoreMajorVersion < 7)
        {
            // TODO Localise this message
            throw new NucleusDataStoreException("PostgreSQL version is " + datastoreMajorVersion + '.' + datastoreMinorVersion + ", 7.0 or later required");
        }
        else if (datastoreMajorVersion == 7 && datastoreMinorVersion <= 2)
        {
            // The driver correctly reports the max table name length as 32.
            // However, constraint names are apparently limited to 31.  In this case we get better looking names by simply treating them all as limited to 31.
            --maxTableNameLength;
            --maxConstraintNameLength;
            --maxIndexNameLength;
        }

        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(POSTGRESQL_RESERVED_WORDS));

        supportedOptions.add(LOCK_WITH_SELECT_FOR_UPDATE);
        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);
        supportedOptions.add(SEQUENCES);
        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(ORDERBY_NULLS_DIRECTIVES);
        supportedOptions.remove(AUTO_INCREMENT_COLUMN_TYPE_SPECIFICATION);
        supportedOptions.remove(AUTO_INCREMENT_KEYS_NULL_SPECIFICATION);
        supportedOptions.remove(DISTINCT_WITH_SELECT_FOR_UPDATE);
        supportedOptions.remove(PERSIST_OF_UNASSIGNED_CHAR);
        if (datastoreMajorVersion < 7 || (datastoreMajorVersion == 7 && datastoreMinorVersion < 2))
        {
            supportedOptions.remove(ALTER_TABLE_DROP_CONSTRAINT_SYNTAX);
        }
        else
        {
            supportedOptions.add(ALTER_TABLE_DROP_CONSTRAINT_SYNTAX);
        }
        supportedOptions.add(BIT_IS_REALLY_BOOLEAN);
        supportedOptions.add(CHAR_COLUMNS_PADDED_WITH_SPACES);
        supportedOptions.add(STORED_PROCEDURES);
        supportedOptions.remove(TX_ISOLATION_NONE);
        supportedOptions.remove(ALLOW_TABLE_ALIAS_IN_UPDATE_SET_CLAUSE);
    }

    /**
     * Initialise the types for this datastore.
     * @param handler SchemaHandler that we initialise the types for
     * @param mconn Managed connection to use
     */
    public void initialiseTypes(StoreSchemaHandler handler, ManagedConnection mconn)
    {
        super.initialiseTypes(handler, mconn);

        // Add on any missing JDBC types when not available from driver

        // Not present in PSQL 8.1.405
        SQLTypeInfo sqlType = new PostgresqlTypeInfo("char", (short)Types.CHAR, 65000, null, null, null, 0, false, (short)3, false, false, false, "char", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.CHAR, sqlType, true);

        sqlType = new PostgresqlTypeInfo("text", (short)Types.CLOB, 9, null, null, null, 0, false, (short)3, false, false, false, null, (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.CLOB, sqlType, true);

        sqlType = new PostgresqlTypeInfo("BYTEA", (short)Types.BLOB, 9, null, null, null, 0, false, (short)3, false, false, false, null, (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BLOB, sqlType, true);

        // Not present in PSQL 9.2.8 - just mirror what BIT does
        sqlType = new PostgresqlTypeInfo("bool", (short)Types.BOOLEAN, 0, null, null, null, 1, false, (short)3, true, false, false, "bool", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BOOLEAN, sqlType, true);

        // Not present in PSQL 9.2.8 - just mirror what SMALLINT does
        sqlType = new PostgresqlTypeInfo("int2", (short)Types.TINYINT, 0, null, null, null, 1, false, (short)3, false, false, false, "int2", (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.TINYINT, sqlType, true);
    }

    /**
     * Accessor for the vendor id.
     * @return The vendor id.
     **/
    public String getVendorID()
    {
        return "postgresql";
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        SQLTypeInfo info = new PostgresqlTypeInfo(rs);

        // Since PostgreSQL supports many user defined data types and uses many type aliases the 
        // default methods have trouble finding the right associations between JDBC and PostgreSQL 
        // data types.  We filter the returned type info to be sure we use the appropriate base 
        // PostgreSQL types for the important JDBC types.*/
        if (psqlTypes == null)
        {
            psqlTypes = new Hashtable();
            psqlTypes.put("" + Types.BIT, "bool");
            psqlTypes.put("" + Types.TIMESTAMP, "timestamptz");
            psqlTypes.put("" + Types.BIGINT, "int8");
            psqlTypes.put("" + Types.CHAR, "char");
            psqlTypes.put("" + Types.DATE, "date");
            psqlTypes.put("" + Types.DOUBLE, "float8");
            psqlTypes.put("" + Types.INTEGER, "int4");
            psqlTypes.put("" + Types.LONGVARCHAR, "text");
            psqlTypes.put("" + Types.CLOB, "text");
            psqlTypes.put("" + Types.BLOB, "bytea");
            psqlTypes.put("" + Types.NUMERIC, "numeric");
            psqlTypes.put("" + Types.REAL, "float4");
            psqlTypes.put("" + Types.SMALLINT, "int2");
            psqlTypes.put("" + Types.TIME, "time");
            psqlTypes.put("" + Types.VARCHAR, "varchar");

            // TODO Enable alternative OTHER types
            psqlTypes.put("" + Types.OTHER, "uuid");

            // PostgreSQL provides 2 types for "char" mappings - "char" and "bpchar". PostgreSQL recommend
            // bpchar for default usage, but sadly you cannot say "bpchar(200)" in an SQL statement. Due to
            // this we use "char" since you can say "char(100)" (and internally in PostgreSQL it becomes bpchar)
            // PostgreSQL 8.1 JDBC driver somehow puts "char" as Types.OTHER rather than Types.CHAR ! so this is
            // faked in createTypeInfo() above.

            // PostgreSQL (7.3, 7.4) doesn't provide a SQL type to map to JDBC types FLOAT, DECIMAL, BLOB, BOOLEAN
        }
        Object obj = psqlTypes.get("" + info.getDataType());
        if (obj != null)
        {
            String  psql_type_name = (String)obj;
            if (!info.getTypeName().equalsIgnoreCase(psql_type_name))
            {
                // We don't support this JDBC type using *this* PostgreSQL SQL type
                NucleusLogger.DATASTORE.debug(Localiser.msg("051007", info.getTypeName(), getNameForJDBCType(info.getDataType())));
                return null;
            }
        }
        return info;
    }

    /**
     * Method to create a column info for the current row.
     * Overrides the dataType/columnSize/decimalDigits to cater for Postgresql particularities.
     * @param rs ResultSet from DatabaseMetaData.getColumns()
     * @return column info
     */
    public RDBMSColumnInfo newRDBMSColumnInfo(ResultSet rs)
    {
        RDBMSColumnInfo info = new RDBMSColumnInfo(rs);

        String typeName = info.getTypeName();
        if (typeName.equalsIgnoreCase("text"))
        {
            // Equate "text" to Types.LONGVARCHAR
            info.setDataType((short)Types.LONGVARCHAR);
        }
        else if (typeName.equalsIgnoreCase("bytea"))
        {
            // Equate "bytea" to Types.LONGVARBINARY
            info.setDataType((short)Types.LONGVARBINARY);
        }

        /*
         * The PostgreSQL drivers sometimes produce some truly funky metadata.
         * I saw these next two occur during unit testing when
         * decimal_widget.big_decimal_field reported a columnSize of 65535 and
         * a decimalDigits of 65534, instead of the correct answers of 18 and 2.
         *
         * A -1 for either of these will cause their validation to be bypassed,
         * which is a shame but it's all we can do.  No one should be surprised
         * if we end up needing more of these.
         */
        int columnSize = info.getColumnSize();
        if (columnSize > PostgresqlTypeInfo.MAX_PRECISION)
        {
            info.setColumnSize(-1);
        }

        int decimalDigits = info.getDecimalDigits();
        if (decimalDigits > PostgresqlTypeInfo.MAX_PRECISION)
        {
            info.setDecimalDigits(-1);
        }

        return info;
    }

    /**
     * Method to return ForeignKeyInfo for the current row of the ResultSet which will have been
     * obtained from a call to DatabaseMetaData.getImportedKeys() or DatabaseMetaData.getExportedKeys().
     * @param rs The result set returned from DatabaseMetaData.get??portedKeys()
     * @return The foreign key info 
     */
    public ForeignKeyInfo newFKInfo(ResultSet rs)
    {
        org.datanucleus.store.rdbms.schema.ForeignKeyInfo info = super.newFKInfo(rs);

        // PostgreSQL sometimes return strange FK info. This checks for the FK name with various
        // extraneous info on the end with a separator of \000.
        String fkName = (String)info.getProperty("fk_name");
        int firstBackslashIdx = fkName.indexOf('\\');
        if (firstBackslashIdx > 0)
        {
            info.addProperty("fk_name", fkName.substring(0, firstBackslashIdx));
        }
        return info;
    }

    // ------------------------------------ Schema Methods -------------------------------------

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
     * Method to return the INSERT statement to use when inserting into a table and we dont want to
     * specify columns. This is the case when we have a single column in the table and that column
     * is autoincrement/identity (and so is assigned automatically in the datastore).
     * Postgresql expects something like
     * <pre>
     * INSERT INTO tbl VALUES(DEFAULT)
     * </pre>
     * @param table The table
     * @return The statement for the INSERT
     */
    public String getInsertStatementForNoColumns(Table table)
    {
        return "INSERT INTO " + table.toString() + " VALUES (DEFAULT)";
    }

    /**
     * PostgreSQL allows specification of PRIMARY KEY in the CREATE TABLE, so
     * we need nothing here.
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The PK statement
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        return null;
    }
 
    /**
     * Accessor for the statement for dropping a table.
     * PostgreSQL has supported the DROP TABLE tbl-name CASCADE since 7.3.
     * @param table The table to drop.
     * @return The statement for dropping a table.
     **/
    public String getDropTableStatement(Table table)
    {
        /* DROP TABLE t CASCADE is supported beginning in 7.3 */
        if (datastoreMajorVersion < 7 ||
            (datastoreMajorVersion == 7 && datastoreMinorVersion < 3))
        {
            return "DROP TABLE " + table.toString();
        }
        else
        {
            return "DROP TABLE " + table.toString() + " CASCADE";
        }
    }

    /**
     * Returns the appropriate DDL to create an index.
     * Overrides the superclass variant since Postgresql doesn't support having index names specified in
     * a particular schema (i.e "{schema}.{indexName}").
     * @param idx An object describing the index.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    public String getCreateIndexStatement(Index idx, IdentifierFactory factory)
    {
        String idxIdentifier = factory.getIdentifierInAdapterCase(idx.getName());
        return 
           "CREATE " + (idx.getUnique() ? "UNIQUE " : "") + "INDEX " + idxIdentifier + " ON " + idx.getTable().toString() + ' ' +
           idx + (idx.getExtendedIndexSettings() == null ? "" : " " + idx.getExtendedIndexSettings());
    }

    // ---------------------------- Identity Support ---------------------------

    /**
     * Accessor for the autoincrement sql access statement for this datastore.
     * @param table Table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest auto-increment key
     **/
    public String getAutoIncrementStmt(Table table, String columnName)
    {
        StringBuilder stmt=new StringBuilder("SELECT currval('");

        // PostgreSQL creates a sequence for each SERIAL column with name of the form "{table}_seq"
        // in the current catalog/schema. PostgreSQL doesn't use catalog so ignore that
        if (table.getSchemaName() != null)
        {
            stmt.append(table.getSchemaName().replace(getIdentifierQuoteString(), ""));
            stmt.append(getCatalogSeparator());
        }

        String tableName = table.getIdentifier().toString();
        boolean quoted = tableName.startsWith(getIdentifierQuoteString());
        if (quoted)
        {
            stmt.append(getIdentifierQuoteString());
        }
        stmt.append(tableName.replace(getIdentifierQuoteString(), ""));
        stmt.append("_");
        stmt.append(columnName.replace(getIdentifierQuoteString(), ""));
        stmt.append("_seq");
        if (quoted)
        {
            stmt.append(getIdentifierQuoteString());
        }

        stmt.append("')");

        return stmt.toString();
    }

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @return The keyword for a column using auto-increment
     **/
    public String getAutoIncrementKeyword()
    {
        return "SERIAL";
    }

    // ---------------------------- Sequence Support ---------------------------

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#sequenceExists(java.sql.Connection, java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public boolean sequenceExists(Connection conn, String catalogName, String schemaName, String seqName)
    {
        String stmtStr = "SELECT relname FROM pg_class WHERE relname=?";
        PreparedStatement ps = null;
        ResultSet rs = null;
        try
        {
            NucleusLogger.DATASTORE_NATIVE.debug(stmtStr + " : for sequence=" + seqName);
            ps = conn.prepareStatement(stmtStr);
            ps.setString(1, seqName);
            rs = ps.executeQuery();
            if (rs.next())
            {
                return true;
            }
            return false;
        }
        catch (SQLException sqle)
        {
            NucleusLogger.DATASTORE_RETRIEVE.debug("Exception while executing query for sequence " + seqName + " : " + stmtStr + " - " + sqle.getMessage());
        }
        finally
        {
            try
            {
                try
                {
                    if (rs != null && !rs.isClosed())
                    {
                        rs.close();
                    }
                }
                finally
                {
                    ps.close();
                }
            }
            catch (SQLException sqle)
            {
            }
        }

        return super.sequenceExists(conn, catalogName, schemaName, seqName);
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
            throw new NucleusUserException(Localiser.msg("051028"));
        }

        StringBuilder stmt = new StringBuilder("CREATE SEQUENCE ");
        stmt.append(sequence_name);
        if (min != null)
        {
            stmt.append(" MINVALUE " + min);
        }
        if (max != null)
        {
            stmt.append(" MAXVALUE " + max);
        }
        if (start != null)
        {
            stmt.append(" START WITH " + start);
        }
        if (increment != null)
        {
            stmt.append(" INCREMENT BY " + increment);
        }
        if (cache_size != null)
        {
            stmt.append(" CACHE " + cache_size);
        }
        else
        {
            stmt.append(" CACHE 1");
        }

        return stmt.toString();
    }

    /**
     * Accessor for the statement for getting the next id from the sequence
     * for this datastore.
     * @param sequence_name Name of the sequence 
     * @return The statement for getting the next id for the sequence
     **/
    public String getSequenceNextStmt(String sequence_name)
    {
        if (sequence_name == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }

        StringBuilder stmt=new StringBuilder("SELECT nextval('");
        stmt.append(sequence_name);
        stmt.append("')");

        return stmt.toString();
    }

    /**
     * Whether the datastore will support setting the query fetch size to the supplied value.
     * @param size The value to set to
     * @return Whether it is supported.
     */
    public boolean supportsQueryFetchSize(int size)
    {
        if (driverMajorVersion > 7)
        {
            // Supported for Postgresql 8
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Method to return the SQL to append to the WHERE clause of a SELECT statement to handle
     * restriction of ranges using the LIMUT keyword.
     * @param offset The offset to return from
     * @param count The number of items to return
     * @return The SQL to append to allow for ranges using LIMIT.
     */
    public String getRangeByLimitEndOfStatementClause(long offset, long count)
    {
        if (offset <= 0 && count <= 0)
        {
            return "";
        }

        if (datastoreMajorVersion < 8 || (datastoreMajorVersion == 8 && datastoreMinorVersion <= 3))
        {
            String str = "";
            if (count > 0)
            {
                str += "LIMIT " + count + " ";
            }
            if (offset >= 0)
            {
                str += "OFFSET " + offset + " ";
            }
            return str;
        }
        else
        {
            // Use SQL 2008 standard OFFSET/FETCH keywords
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

    /**
     * The character for escaping patterns.
     * @return Escape character(s)
     */
    public String getEscapePatternExpression()
    {
        if (datastoreMajorVersion > 8 || (datastoreMajorVersion == 8 && datastoreMinorVersion >= 3))
        {
            return "ESCAPE E'\\\\'";
        }
        return "ESCAPE '\\\\'";
    }

    /**
     * return whether this exception represents a cancelled statement.
     * @param sqle the exception
     * @return whether it is a cancel
     */
    public boolean isStatementCancel(SQLException sqle)
    {
        if (sqle.getErrorCode() == 57014)
        {
            return true;
        }
        return false;
    }
}