/**********************************************************************
Copyright (c) 2015 Jeff Albion and others. All rights reserved.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Properties;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.table.TableImpl;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to the SQL Anywhere database.
 */
public class SQLAnywhereAdapter extends BaseDatastoreAdapter
{
    /**
     * SQL Anywhere uses a product version of "major.minor.revision.build"
     */
    protected int datastoreBuildVersion = -1;
    protected int driverBuildVersion = -1;
    protected boolean usingjConnect = true;
                 
    /**
     * Constructor.
     * @param metadata MetaData for the DB
     */
    public SQLAnywhereAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        // Determine the build versions of the database / driver
        try
        {
            datastoreBuildVersion = Integer.parseInt(datastoreProductVersion.substring(datastoreProductVersion.lastIndexOf(".") + 1));
            if (driverName.equals("SQL Anywhere JDBC Driver"))
            {
                usingjConnect = false;
                driverBuildVersion = Integer.parseInt(driverVersion.substring(driverVersion.lastIndexOf(".") + 1));
            }
            else
            {
                // Assume jConnect if it isn't SQLAJDBC
                // TODO: jConnect version detection
                driverBuildVersion = -1;
            }
        }
        catch (Throwable t)
        {
            // Parsing error for the version, ignore
        }

        // Determine the keyword list. This list is obtained from the JDBC driver using
        // DatabaseMetaData.getSQLKeywords(), but there are also user configurable options
        // Instead, attempt to query the set of reserved words for SQL Anywhere directly.
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try
        {
            // Query the standard keywords
            conn = metadata.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT \"reserved_word\" FROM sa_reserved_words() ORDER BY \"reserved_word\"");
            while (rs.next())
            {
                reservedKeywords.add(rs.getString(1).trim().toUpperCase());
            }
            rs.close();

            // Also use user-specified keywords, if they are specified
            rs = stmt.executeQuery("SELECT \"option\", \"setting\" FROM SYS.SYSOPTION WHERE \"option\" = 'reserved_keywords' or \"option\" = 'non_keywords'");
            while (rs.next())
            {
                if (rs.getString(1).toLowerCase().equals("reserved_keywords"))
                {
                    String originalUserKeywords = rs.getString(2).trim().toUpperCase();
                    StringTokenizer tokens = new StringTokenizer(originalUserKeywords, ",");
                    Set<String> userReservedWordSet = new HashSet<>();
                    while (tokens.hasMoreTokens())
                    {
                        userReservedWordSet.add(tokens.nextToken().trim().toUpperCase());
                    }

                    // If LIMIT isn't enabled by the customized database keywords, set it to enable LIMIT
                    if (!userReservedWordSet.contains("LIMIT"))
                    {
                        userReservedWordSet.add("LIMIT");
                        Statement setOptionStmt = null;
                        try
                        {
                            setOptionStmt = conn.createStatement();
                            setOptionStmt.executeUpdate("SET OPTION PUBLIC.reserved_keywords = 'LIMIT" +
                                    (originalUserKeywords.length() != 0 ? "," : "") + originalUserKeywords + "'");
                        }
                        finally
                        {
                            if (setOptionStmt != null)
                            {
                                setOptionStmt.close();
                            }
                        }
                    }

                    reservedKeywords.addAll(userReservedWordSet);
                    // Allow the user to override and remove keywords for compatibility, if necessary
                }
                else if (rs.getString(1).toLowerCase().equals("non_keywords"))
                {
                    reservedKeywords.removeAll(StringUtils.convertCommaSeparatedStringToSet(rs.getString(2).trim().toUpperCase()));
                }
            }
            rs.close();
            stmt.close();
        }
        catch (Throwable t)
        {
            // JDBC metadata is still used for keywords, and assume LIMIT is set elsewhere
        }
        finally
        {
            try
            {
                if (rs != null && !rs.isClosed())
                {
                    rs.close();
                }
            }
            catch (SQLException sqle)
            {
                rs = null;
            }
            try
            {
                if (stmt != null && !stmt.isClosed())
                {
                    stmt.close();
                }
            }
            catch (SQLException sqle)
            {
                stmt = null;
            }
            try
            {
                if (conn != null && !conn.isClosed())
                {
                    conn.close();
                }
            }
            catch (SQLException sqle)
            {
                conn = null;
            }
        }

        // Provide supported / unsupported capabilities for SQL Anywhere
        // See: DatastoreAdapter.java for options, BaseDatastoreAdapter.java for defaults
        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(STORED_PROCEDURES);
        if (datastoreMajorVersion >= 12)
        {
            supportedOptions.add(SEQUENCES);
        }
        supportedOptions.add(PROJECTION_IN_TABLE_REFERENCE_JOINS);
        supportedOptions.add(CATALOGS_IN_TABLE_DEFINITIONS);
        supportedOptions.add(SCHEMAS_IN_TABLE_DEFINITIONS);
        supportedOptions.add(IDENTIFIERS_LOWERCASE);
        supportedOptions.add(IDENTIFIERS_MIXEDCASE);
        supportedOptions.add(IDENTIFIERS_UPPERCASE);
        supportedOptions.add(IDENTIFIERS_LOWERCASE_QUOTED);
        supportedOptions.add(IDENTIFIERS_MIXEDCASE_QUOTED);
        supportedOptions.add(IDENTIFIERS_UPPERCASE_QUOTED);
        supportedOptions.add(ALTER_TABLE_DROP_FOREIGN_KEY_CONSTRAINT);
        supportedOptions.add(STATEMENT_BATCHING);
        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);
        supportedOptions.add(IDENTITY_PK_IN_CREATE_TABLE_COLUMN_DEF);
        supportedOptions.add(LOCK_ROW_USING_SELECT_FOR_UPDATE);
        supportedOptions.add(LOCK_ROW_USING_OPTION_AFTER_FROM);
        supportedOptions.add(OPERATOR_BITWISE_AND);
        supportedOptions.add(OPERATOR_BITWISE_OR);
        supportedOptions.add(OPERATOR_BITWISE_XOR);
        supportedOptions.remove(GET_GENERATED_KEYS_STATEMENT); // Statement.getGeneratedKeys() not supported
        supportedOptions.remove(DEFERRED_CONSTRAINTS); // No
        supportedOptions.remove(ANSI_JOIN_SYNTAX); // Deprecated
        supportedOptions.remove(ANSI_CROSSJOIN_SYNTAX); // Deprecated
        supportedOptions.remove(IDENTITY_KEYS_NULL_SPECIFICATION); // No
        supportedOptions.remove(BOOLEAN_COMPARISON); // No

        // Add the supported and unsupported JDBC types for lookups
        supportedJdbcTypesById.clear();
        supportedJdbcTypesById.put(Integer.valueOf(Types.BIGINT), "BIGINT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.BINARY), "BINARY");
        supportedJdbcTypesById.put(Integer.valueOf(Types.BIT), "BIT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.BLOB), "LONG BINARY");
        supportedJdbcTypesById.put(Integer.valueOf(Types.BOOLEAN), "BIT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.CHAR), "CHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.CLOB), "LONG VARCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.DATE), "DATE");
        supportedJdbcTypesById.put(Integer.valueOf(Types.DECIMAL), "DECIMAL");
        supportedJdbcTypesById.put(Integer.valueOf(Types.DOUBLE), "DOUBLE");
        supportedJdbcTypesById.put(Integer.valueOf(Types.FLOAT), "FLOAT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.INTEGER), "INTEGER");
        supportedJdbcTypesById.put(Integer.valueOf(Types.LONGVARBINARY), "LONG BINARY");
        supportedJdbcTypesById.put(Integer.valueOf(Types.LONGVARCHAR), "LONG VARCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.NUMERIC), "NUMERIC");
        supportedJdbcTypesById.put(Integer.valueOf(Types.REAL), "REAL");
        supportedJdbcTypesById.put(Integer.valueOf(Types.SMALLINT), "SMALLINT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.SQLXML), "XML");
        supportedJdbcTypesById.put(Integer.valueOf(Types.TIME), "TIME");
        supportedJdbcTypesById.put(Integer.valueOf(Types.TIMESTAMP), "TIMESTAMP");
        supportedJdbcTypesById.put(Integer.valueOf(Types.TINYINT), "TINYINT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.VARBINARY), "BINARY");
        supportedJdbcTypesById.put(Integer.valueOf(Types.VARCHAR), "VARCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.NVARCHAR), "NVARCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.NCHAR), "NCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.NCLOB), "LONG NVARCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.OTHER), "OTHER");
        unsupportedJdbcTypesById.clear();
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.ARRAY), "ARRAY"); // Maybe...?
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.DATALINK), "DATALINK");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.DISTINCT), "DISTINCT");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.JAVA_OBJECT), "JAVA_OBJECT");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.NULL), "NULL");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.REF), "REF");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.STRUCT), "STRUCT"); // Maybe ROW would work here?
    }

    public String getVendorID()
    {
        return "sqlanywhere";
    }

    public String getCreateDatabaseStatement(String catalogName, String schemaName)
    {
        throw new UnsupportedOperationException("SQL Anywhere does not support CREATE DATABASE via a schema name");
    }

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        throw new UnsupportedOperationException("SQL Anywhere does not support DROP DATABASE via a schema name");
    }

    /**
     * Returns the appropriate SQL to create the given table having the given columns. No column constraints
     * or key definitions should be included. It should return something like:
     * 
     * <pre>
     * CREATE TABLE FOO ( BAR VARCHAR(30), BAZ INTEGER )
     * </pre>
     * @param table The table to create.
     * @param columns The columns of the table.
     * @param props Properties for controlling the table creation
     * @param factory Factory for identifiers
     * @return The text of the SQL statement.
     */
    public String getCreateTableStatement(TableImpl table, Column[] columns, Properties props, IdentifierFactory factory)
    {
        StringBuilder createStmt = new StringBuilder();
        String indent = "    ";
        if (getContinuationString().length() == 0)
        {
            indent = "";
        }

        // CREATE TABLE with column specifiers
        createStmt.append("CREATE TABLE ").append(table.toString()).append(getContinuationString()).append("(").append(getContinuationString());
        for (int i = 0; i < columns.length; ++i)
        {
            if (i > 0)
            {
                createStmt.append(",").append(getContinuationString());
            }

            createStmt.append(indent).append(columns[i].getSQLDefinition());
        }

        // PRIMARY KEY(col[,col])
        if (supportsOption(PRIMARYKEY_IN_CREATE_STATEMENTS))
        {
            PrimaryKey pk = table.getPrimaryKey();
            if (pk != null && pk.getNumberOfColumns() > 0)
            {
                boolean includePk = true;
                if (supportsOption(IDENTITY_PK_IN_CREATE_TABLE_COLUMN_DEF))
                {
                    for (Column pkCol : pk.getColumns())
                    {
                        if (pkCol.isIdentity())
                        {
                            // This column is auto-increment and is specified in the column def so ignore here
                            includePk = false;
                            break;
                        }
                    }
                }

                if (includePk)
                {
                    createStmt.append(",").append(getContinuationString());
                    if (pk.getName() != null)
                    {
                        String identifier = factory.getIdentifierInAdapterCase(pk.getName());
                        createStmt.append(indent).append("CONSTRAINT ").append(identifier).append(" ").append(pk.toString());
                    }
                    else
                    {
                        createStmt.append(indent).append(pk.toString());
                    }
                }
            }
        }

        // UNIQUE( col [,col] )
        if (supportsOption(UNIQUE_IN_END_CREATE_STATEMENTS))
        {
            StringBuilder uniqueConstraintStmt = new StringBuilder();
            for (int i = 0; i < columns.length; ++i)
            {
                if (columns[i].isUnique())
                {
                    if (uniqueConstraintStmt.length() < 1)
                    {
                        uniqueConstraintStmt.append(",").append(getContinuationString());
                        uniqueConstraintStmt.append(indent).append(" UNIQUE (");
                    }
                    else
                    {
                        uniqueConstraintStmt.append(",");
                    }
                    uniqueConstraintStmt.append(columns[i].getIdentifier().toString());
                }
            }
            if (uniqueConstraintStmt.length() > 1)
            {
                uniqueConstraintStmt.append(")");
                createStmt.append(uniqueConstraintStmt.toString());
            }
        }

        // FOREIGN KEY(col [,col] ) REFERENCES {TBL} (col [,col])
        if (supportsOption(FK_IN_END_CREATE_STATEMENTS))
        {
            StringBuilder fkConstraintStmt = new StringBuilder();
            ClassLoaderResolver clr = table.getStoreManager().getNucleusContext().getClassLoaderResolver(null);
            List<ForeignKey> fks = table.getExpectedForeignKeys(clr);
            if (fks != null && !fks.isEmpty())
            {
                for (ForeignKey fk : fks)
                {
                    // TODO Ensure that the other table exists, for now assume it does
                    createStmt.append(",").append(getContinuationString());
                    if (fk.getName() != null)
                    {
                        String identifier = factory.getIdentifierInAdapterCase(fk.getName());
                        createStmt.append(indent).append("CONSTRAINT ").append(identifier).append(" ").append(fk.toString());
                    }
                    else
                    {
                        createStmt.append(indent).append(fk.toString());
                    }
                }
            }
            if (fkConstraintStmt.length() > 1)
            {
                createStmt.append(fkConstraintStmt.toString());
            }
        }

        // CHECK (column_identifier IN (literal[,literal]))
        if (supportsOption(CHECK_IN_END_CREATE_STATEMENTS))
        {
            StringBuilder checkConstraintStmt = new StringBuilder();
            for (int i = 0; i < columns.length; ++i)
            {
                if (columns[i].getCheckConstraints() != null)
                {
                    checkConstraintStmt.append(",").append(getContinuationString());
                    checkConstraintStmt.append(indent).append(columns[i].getCheckConstraints());
                }
            }
            if (checkConstraintStmt.length() > 1)
            {
                createStmt.append(checkConstraintStmt.toString());
            }
        }

        createStmt.append(getContinuationString()).append(")");

        return createStmt.toString();
    }

    /**
     * Accessor for the DROP TABLE statement for SQL Anywhere SQL Anywhere doesn't support CASCADE so just
     * return a simple 'DROP TABLE table-name'
     * @param table The table to drop.
     * @return The DROP TABLE statement
     **/
    public String getDropTableStatement(Table table)
    {
        return "DROP TABLE " + table.toString();
    }

    /**
     * The option to specify in "SELECT ... WITH (option)" to lock instances Null if not supported.
     * @return The option to specify with "SELECT ... WITH (option)"
     **/
    public String getSelectWithLockOption()
    {
        return "XLOCK";
    }

    /**
     * Method to return the basic SQL for a DELETE TABLE statement. Returns the String as
     * <code>DELETE MYTABLE FROM MYTABLE t1</code>.
     * @param tbl The SQLTable to delete
     * @return The delete table string
     */
    public String getDeleteTableStatement(SQLTable tbl)
    {
        return "DELETE " + tbl.getAlias() + " FROM " + tbl.toString();
    }

    /**
     * Method to define a primary key definition
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The PK statement
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        return null; // supported in CREATE TABLE...
        // return "ALTER TABLE " + pk.getTable().toString() + " ADD " + pk.toString();
    }

    /**
     * Method to define a foreign key definition
     * @param fk An object describing the foreign key.
     * @param factory Identifier factory
     * @return The FK statement
     */
    public String getAddForeignKeyStatement(ForeignKey fk, IdentifierFactory factory)
    {
        return null;
        // return "ALTER TABLE " + fk.getTable().toString() + " ADD " + fk.toString();
    }

    /**
     * Method to return the SQLText for an UPDATE TABLE statement. Returns the SQLText for
     * <code>UPDATE T1 SET x1 = val1, x2 = val2 FROM MYTBL T1</code>. Override if the datastore doesn't
     * support that standard syntax.
     * @param tbl The primary table
     * @param setSQL The SQLText for the SET component
     * @return SQLText for the update statement
     */
    public SQLText getUpdateTableStatement(SQLTable tbl, SQLText setSQL)
    {
        SQLText sql = new SQLText("UPDATE ").append(tbl.getAlias().toString()); // "UPDATE T1"
        sql.append(" ").append(setSQL); // " SET x1 = val1, x2 = val2"
        sql.append(" FROM ").append(tbl.toString()); // " FROM MYTBL T1"
        return sql;
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        SQLTypeInfo info = new SQLTypeInfo(rs);

        // Discard the tinyint type because it doesn't support negative values.
        if (info.getTypeName().toLowerCase().startsWith("tinyint"))
        {
            return null;
        }
        return info;
    }

    /**
     * Method to create a column info for the current row.
     * @param rs ResultSet from DatabaseMetaData.getColumns()
     * @return column info
     */
    public RDBMSColumnInfo newRDBMSColumnInfo(ResultSet rs)
    {
        RDBMSColumnInfo info = new RDBMSColumnInfo(rs);

        short dataType = info.getDataType();
        switch (dataType)
        {
            case Types.DATE :
            case Types.TIME :
            case Types.TIMESTAMP :
                info.setDecimalDigits(0); // Ensure this is set to 0
                break;
            default :
                break;
        }

        return info;
    }

    /**
     * Accessor for the auto-increment sql statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest auto-increment key
     */
    public String getIdentityLastValueStmt(Table table, String columnName)
    {
        return "SELECT @@IDENTITY";
    }

    /**
     * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
     * @param storeMgr The Store Manager
     * @return The keyword for a column using auto-increment
     */
    public String getIdentityKeyword(StoreManager storeMgr)
    {
        return "NOT NULL DEFAULT AUTOINCREMENT";
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
            stmt.append(" NO CACHE");
        }

        // default to NO CYCLE
        return stmt.toString();
    }

    /**
     * Determine if a sequence exists
     * @param conn Connection to database
     * @param catalogName Database catalog name
     * @param schemaName Database schema name
     * @param seqName Name of the sequence
     * @return The statement for getting the next id for the sequence
     */
    public boolean sequenceExists(Connection conn, String catalogName, String schemaName, String seqName)
    {
        try
        {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYS.SYSSEQUENCE WHERE SEQUENCENAME = '" + seqName + "'");
            boolean sequenceFound = rs.next();
            rs.close();
            stmt.close();
            return sequenceFound;
        }
        catch (Throwable t)
        {
            return false;
        }
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
            throw new NucleusUserException(Localiser.msg("051028"));
        }

        StringBuilder stmt = new StringBuilder("SELECT ");
        stmt.append(sequence_name);
        stmt.append(".nextval");

        return stmt.toString();
    }

    /**
     * The function to creates a unique value of type uniqueidentifier. SQL Anywhere generates 36-character
     * hex uuids, with hypens
     * @return The SQL statement. e.g. "SELECT newid()"
     **/
    public String getSelectNewUUIDStmt()
    {
        return "SELECT newid()";
    }

    /**
     * Method to return the SQL to append to the WHERE clause of a SELECT statement to handle restriction of
     * ranges using the LIMIT keyword.
     * @param offset The offset to return from
     * @param count The number of items to return
     * @param hasOrdering Whether ordering is present
     * @return The SQL to append to allow for ranges using LIMIT.
     */
    @Override
    public String getRangeByLimitEndOfStatementClause(long offset, long count, boolean hasOrdering)
    {
        if (offset >= 0 && count > 0)
        {
            return "LIMIT " + offset + "," + count + " ";
        }
        else if (offset <= 0 && count > 0)
        {
            return "LIMIT " + count + " ";
        }
        else if (offset >= 0 && count < 0)
        {
            // LIMIT doesnt allow just offset so use Long.MAX_VALUE as count
            return "LIMIT " + offset + "," + Long.MAX_VALUE + " ";
        }
        return "";
    }

    /**
     * Load all datastore mappings for this RDBMS database.
     * @param mgr the PluginManager
     * @param clr the ClassLoaderResolver
     */
    protected void loadColumnMappings(PluginManager mgr, ClassLoaderResolver clr)
    {
        // Load up built-in types for this datastore
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BitColumnMapping.class, JDBCType.BIT, "BIT", true);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BitColumnMapping.class, JDBCType.BOOLEAN, "BIT", true);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.BooleanColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Boolean.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", true);
        registerColumnMapping(Byte.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", true);
        registerColumnMapping(Character.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);

        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", true);
        registerColumnMapping(Double.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.FloatColumnMapping.class, JDBCType.FLOAT, "FLOAT", true);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DoubleColumnMapping.class, JDBCType.DOUBLE, "DOUBLE", false);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.RealColumnMapping.class, JDBCType.REAL, "REAL", false);
        registerColumnMapping(Float.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", false);

        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", true);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);
        registerColumnMapping(Integer.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INT", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);
        registerColumnMapping(Long.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", false);

        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.SmallIntColumnMapping.class, JDBCType.SMALLINT, "SMALLINT", true);
        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerColumnMapping(Short.class.getName(), org.datanucleus.store.rdbms.mapping.column.TinyIntColumnMapping.class, JDBCType.TINYINT, "TINYINT", false);

        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", true);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarcharColumnMapping.class, JDBCType.LONGVARCHAR, "LONGVARCHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.ClobColumnMapping.class, JDBCType.CLOB, "CLOB", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.NVarcharColumnMapping.class, JDBCType.NVARCHAR, "NVARCHAR", false);
        registerColumnMapping(String.class.getName(), org.datanucleus.store.rdbms.mapping.column.NCharColumnMapping.class, JDBCType.NCHAR, "NCHAR", false);

        registerColumnMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.column.DecimalColumnMapping.class, JDBCType.DECIMAL, "DECIMAL", true);
        registerColumnMapping(BigDecimal.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);

        registerColumnMapping(BigInteger.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", true);

        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.DateColumnMapping.class, JDBCType.DATE, "DATE", true);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.sql.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimeColumnMapping.class, JDBCType.TIME, "TIME", true);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", false);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.sql.Time.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", true);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.DateColumnMapping.class, JDBCType.DATE, "DATE", false);
        registerColumnMapping(java.sql.Timestamp.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimeColumnMapping.class, JDBCType.TIME, "TIME", false);

        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimestampColumnMapping.class, JDBCType.TIMESTAMP, "TIMESTAMP", true);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.DateColumnMapping.class, JDBCType.DATE, "DATE", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.TimeColumnMapping.class, JDBCType.TIME, "TIME", false);
        registerColumnMapping(java.util.Date.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", false);

        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(java.io.Serializable.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);

        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.LongVarBinaryColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.BlobColumnMapping.class, JDBCType.BLOB, "BLOB", false);
        registerColumnMapping(byte[].class.getName(), org.datanucleus.store.rdbms.mapping.column.VarBinaryColumnMapping.class, JDBCType.VARBINARY, "VARBINARY", false);

        registerColumnMapping(java.io.File.class.getName(), org.datanucleus.store.rdbms.mapping.column.BinaryStreamColumnMapping.class, JDBCType.LONGVARBINARY, "LONGVARBINARY", true);

        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.BigIntColumnMapping.class, JDBCType.BIGINT, "BIGINT", true);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.IntegerColumnMapping.class, JDBCType.INTEGER, "INTEGER", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.NumericColumnMapping.class, JDBCType.NUMERIC, "NUMERIC", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.CharColumnMapping.class, JDBCType.CHAR, "CHAR", false);
        registerColumnMapping(DatastoreId.class.getName(), org.datanucleus.store.rdbms.mapping.column.VarCharColumnMapping.class, JDBCType.VARCHAR, "VARCHAR", false);

        super.loadColumnMappings(mgr, clr);
    }
}