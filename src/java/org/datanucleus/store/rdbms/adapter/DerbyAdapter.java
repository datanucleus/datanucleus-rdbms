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
2004 Erik Bengtson - added Cloudscape 10 and Apache Derby specifics
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to the Cloudscape/Derby database.
 */
public class DerbyAdapter extends BaseDatastoreAdapter
{
    /**
     * Cloudscape 10.0 beta reserved words, includes SQL92 reserved words
     */
    private static final String CLOUDSCAPE_RESERVED_WORDS =
        "ADD,ALL,ALLOCATE,ALTER,AND,ANY,ARE,AS," +
        "ASC,ASSERTION,AT,AUTHORIZATION,AVG,BEGIN,BETWEEN,BIT," +
        "BIT_LENGTH,BOOLEAN,BOTH,BY,CALL,CASCADE,CASCADED,CASE," +
        "CAST,CHAR,CHARACTER,CHARACTER_LENGTH,CHAR_LENGTH,CHECK,CLOSE,COLLATE," +
        "COLLATION,COLUMN,COMMIT,CONNECT,CONNECTION,CONSTRAINT,CONSTRAINTS,CONTINUE," +
        "CONVERT,CORRESPONDING,COUNT,CREATE,CROSS,CURRENT,CURRENT_DATE,CURRENT_TIME," +
        "CURRENT_TIMESTAMP,CURRENT_USER,CURSOR,DEALLOCATE,DEC,DECIMAL,DECLARE,DEFERRABLE," +
        "DEFERRED,DELETE,DESC,DESCRIBE,DIAGNOSTICS,DISCONNECT,DISTINCT,DOUBLE," +
        "DROP,ELSE,END,ENDEXEC,ESCAPE,EXCEPT,EXCEPTION,EXEC," +
        "EXECUTE,EXISTS,EXPLAIN,EXTERNAL,EXTRACT,FALSE,FETCH,FIRST," +
        "FLOAT,FOR,FOREIGN,FOUND,FROM,FULL,FUNCTION,GET," +
        "GET_CURRENT_CONNECTION,GLOBAL,GO,GOTO,GRANT,GROUP,HAVING,HOUR," +
        "IDENTITY,IMMEDIATE,IN,INDICATOR,INITIALLY,INNER,INOUT,INPUT," +
        "INSENSITIVE,INSERT,INT,INTEGER,INTERSECT,INTO,IS,ISOLATION," +
        "JOIN,KEY,LAST,LEADING,LEFT,LIKE,LOCAL,LONGINT," +
        "LOWER,LTRIM,MATCH,MAX,MIN,MINUTE,NATIONAL,NATURAL," +
        "NCHAR,NVARCHAR,NEXT,NO,NOT,NULL,NULLIF,NUMERIC," +
        "OCTET_LENGTH,OF,ON,ONLY,OPEN,OPTION,OR,ORDER," +
        "OUT,OUTER,OUTPUT,OVERLAPS,PAD,PARTIAL,PREPARE,PRESERVE," +
        "PRIMARY,PRIOR,PRIVILEGES,PROCEDURE,PUBLIC,READ,REAL,REFERENCES," +
        "RELATIVE,RESTRICT,REVOKE,RIGHT,ROLLBACK,ROWS,RTRIM,RUNTIMESTATISTICS," +
        "SCHEMA,SCROLL,SECOND,SELECT,SESSION_USER,SET,SMALLINT,SOME," +
        "SPACE,SQL,SQLCODE,SQLERROR,SQLSTATE,SUBSTR,SUBSTRING,SUM," +
        "SYSTEM_USER,TABLE,TEMPORARY,TIMEZONE_HOUR,TIMEZONE_MINUTE,TINYINT,TO,TRAILING," +
        "TRANSACTION,TRANSLATE,TRANSLATION,TRIM,TRUE,UNION,UNIQUE,UNKNOWN," +
        "UPDATE,UPPER,USER,USING,VALUES,VARCHAR,VARYING,VIEW," +
        "WHENEVER,WHERE,WITH,WORK,WRITE,YEAR";

    /**
     * Constructs an Apache Derby adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public DerbyAdapter(DatabaseMetaData metadata)
    {
        super(metadata);
        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(CLOUDSCAPE_RESERVED_WORDS));

        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(LOCK_WITH_SELECT_FOR_UPDATE);
        supportedOptions.add(CREATE_INDEXES_BEFORE_FOREIGN_KEYS);
        supportedOptions.add(STORED_PROCEDURES);
        supportedOptions.add(SEQUENCES);
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.remove(NULLS_IN_CANDIDATE_KEYS);
        supportedOptions.remove(DEFAULT_KEYWORD_WITH_NOT_NULL_IN_COLUMN_OPTIONS);
        if (datastoreMajorVersion >= 10)
        {
            supportedOptions.remove(NULLS_KEYWORD_IN_COLUMN_OPTIONS);
        }
        else
        {
            supportedOptions.add(NULLS_KEYWORD_IN_COLUMN_OPTIONS);
        }
        if (datastoreMajorVersion < 10 || (datastoreMajorVersion == 10 && datastoreMinorVersion < 6))
        {
            // Only supports ANSI "CROSS JOIN" from 10.6 onwards, prior to that use "INNER JOIN tbl ON 1=1"
            supportedOptions.remove(ANSI_CROSSJOIN_SYNTAX);
            supportedOptions.add(CROSSJOIN_ASINNER11_SYNTAX);
            supportedOptions.remove(SEQUENCES);
        }
        if (datastoreMajorVersion >= 11 || datastoreMinorVersion > 4)
        {
            supportedOptions.add(ORDERBY_NULLS_DIRECTIVES);
        }
    }

    /**
     * Creates the auxiliary functions/procedures in the schema 
     * @param conn the connection to the datastore
     */
    public void initialiseDatastore(Object conn)
    {
        try
        {
            Statement st = ((Connection) conn).createStatement();

            // ASCII Function
            try
            {
                // Try to drop the function to check existence
                st.execute("DROP FUNCTION NUCLEUS_ASCII");
            }
            catch (SQLException sqle) {}
            try
            {
                // Create the function
                st.execute("CREATE FUNCTION NUCLEUS_ASCII(C CHAR(1)) RETURNS INTEGER "+
                    "EXTERNAL NAME 'org.datanucleus.store.rdbms.adapter.DerbySQLFunction.ascii' "+
                    "CALLED ON NULL INPUT "+
                    "LANGUAGE JAVA PARAMETER STYLE JAVA");
            }
            catch (SQLException sqle)
            {
                NucleusLogger.DATASTORE.warn(LOCALISER.msg("051027", sqle));
            }

            // Matches Function
            try
            {
                // Try to drop the function to check existence
                st.execute("DROP FUNCTION NUCLEUS_MATCHES");
            }
            catch (SQLException sqle) {}
            try
            {
                // Create the function
                st.execute("CREATE FUNCTION NUCLEUS_MATCHES(TEXT VARCHAR(8000), PATTERN VARCHAR(8000)) " +
                    "RETURNS INTEGER "+
                    "EXTERNAL NAME 'org.datanucleus.store.rdbms.adapter.DerbySQLFunction.matches' "+
                    "CALLED ON NULL INPUT LANGUAGE JAVA PARAMETER STYLE JAVA");
            }
            catch (SQLException sqle)
            {
                NucleusLogger.DATASTORE.warn(LOCALISER.msg("051027", sqle));
            }

            st.close();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    /**
     * Accessor for the schema name.
     * @param conn The Connection to use
     * @return The schema name used by this connection
     * @throws SQLException
     */
    public String getSchemaName(Connection conn)
    throws SQLException
    {
        // see http://incubator.apache.org/derby/faq.html#schema_exist
        // a connection's current schema name defaults to the connection's user name
        return conn.getMetaData().getUserName().toUpperCase();
    }

    /**
     * Accessor for the catalog name.
     * @param conn The Connection to use
     * @return The catalog name used by this connection
     * @throws SQLException
     */
    public String getCatalogName(Connection conn)
    throws SQLException
    {
        String catalog = conn.getCatalog();
        // the ProbeTable approach returns empty string instead of null here, so do the same
        return catalog != null ? catalog : "";
    }

    /**
     * Accessor for the vendor id.
     * @return The vendor id.
     */
    public String getVendorID()
    {
        return "derby";
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new org.datanucleus.store.rdbms.schema.DerbyTypeInfo(rs);
    }

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        throw new UnsupportedOperationException("Derby does not support dropping schema with cascade. You need to drop all tables first");
    }

    public String getDropTableStatement(Table table)
    {
        return "DROP TABLE " + table.toString();
    }

    /**
     * Returns the appropriate SQL to add a candidate key to its table.
     * It should return something like:
     * <p>
     * <pre>
     * CREATE [UNIQUE] INDEX FOO_CK ON TBL (COL1 [, COL2])
     * </pre>
     * @param ck An object describing the candidate key.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    public String getAddCandidateKeyStatement(CandidateKey ck, IdentifierFactory factory)
    {
        if (ck.getName() != null)
        {
            String identifier = factory.getIdentifierInAdapterCase(ck.getName());
            return "CREATE UNIQUE INDEX " + identifier + " ON " + ck.getTable().toString() +
                " " + ck.getColumnList();
        }
        else
        {
            return "ALTER TABLE " + ck.getTable().toString() + " ADD " + ck;
        }
    }

	/**
	 * Accessor for the auto-increment SQL statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
	 * @return The statement for getting the latest auto-increment key
	 **/
	public String getAutoIncrementStmt(Table table, String columnName)
	{
		return "VALUES IDENTITY_VAL_LOCAL()";
	}

	/**
	 * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
	 * @return The keyword for a column using auto-increment
	 **/
    public String getAutoIncrementKeyword()
    {
        return "generated always as identity (start with 1)";
    }

    /**
     * Verifies if the given <code>columnDef</code> is auto incremented by the datastore.
     * @param columnDef the datastore type name
     * @return true when the <code>columnDef</code> has values auto incremented by the datastore
     **/
    public boolean isIdentityFieldDataType(String columnDef)
    {
        if (columnDef != null && columnDef.toUpperCase().indexOf("AUTOINCREMENT") >= 0)
        {
            return true;
        }
        return false;
    }

    /**
     * Method to return the INSERT statement to use when inserting into a table that has no
     * columns specified. This is the case when we have a single column in the table and that column
     * is autoincrement/identity (and so is assigned automatically in the datastore).
     * @param table The table
     * @return The INSERT statement
     */
    public String getInsertStatementForNoColumns(Table table)
    {
        // Note that calling "VALUES IDENTITY_VAL_LOCAL()" immediately after this with Derby 10.0.2.1 returns null!
        // This is a bug in Derby - Apache JIRA "DERBY-439"
        return "INSERT INTO " + table.toString() + " VALUES (DEFAULT)";
    }

    /**
     * Accessor for a statement that will return the statement to use to get the datastore date.
     * @return SQL statement to get the datastore date
     */
    public String getDatastoreDateStatement()
    {
        return "VALUES CURRENT_TIMESTAMP";
    }

    /**
     * Method returning the text to append to the end of the SELECT to perform the equivalent
     * of "SELECT ... FOR UPDATE" (on some RDBMS).
     * Derby doesn't support "FOR UPDATE" in all situations and has a similar one "WITH RR"
     * See https://issues.apache.org/jira/browse/DERBY-3900
     * @return The "FOR UPDATE" style text
     */
    public String getSelectForUpdateText()
    {
        return "WITH RR";
    }

    /**
     * Method to return if it is valid to select the specified mapping for the specified statement
     * for this datastore adapter. Sometimes, dependent on the type of the column(s), and what other
     * components are present in the statement, it may be invalid to select the mapping.
     * This implementation returns true, so override in database-specific subclass as required.
     * @param stmt The statement
     * @param m The mapping that we want to select
     * @return Whether it is valid
     */
    public boolean validToSelectMappingInStatement(SQLStatement stmt, JavaTypeMapping m)
    {
        if (m.getNumberOfDatastoreMappings() <= 0)
        {
            return true;
        }

        for (int i=0;i<m.getNumberOfDatastoreMappings();i++)
        {
            Column col = m.getDatastoreMapping(i).getColumn();
            if (col.getJdbcType() == Types.CLOB || col.getJdbcType() == Types.BLOB)
            {
                // ERROR X0X67: Columns of type 'CLOB'/'BLOB' may not be used in 
                // CREATE INDEX, ORDER BY, GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT
                if (stmt.isDistinct())
                {
                    NucleusLogger.QUERY.debug("Not selecting " + m + " since is for BLOB/CLOB and using DISTINCT");
                    return false;
                }
                else if (stmt.getNumberOfUnions() > 0)
                {
                    NucleusLogger.QUERY.debug("Not selecting " + m + " since is for BLOB/CLOB and using UNION");
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Accessor for the function to use for converting to numeric.
     * @return The numeric conversion function for this datastore.
     */
    public String getNumericConversionFunction()
    {
        return "NUCLEUS_ASCII";
    }

    /**
     * return whether this exception represents a cancelled statement.
     * @param sqle the exception
     * @return whether it is a cancel
     */
    public boolean isStatementCancel(SQLException sqle)
    {
        if (sqle.getSQLState().equalsIgnoreCase("XCL52"))
        {
            return true; // Could also be timeout
        }
        return false;
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

        StringBuilder stmt = new StringBuilder("CREATE SEQUENCE ");
        stmt.append(sequence_name);
        if (start != null)
        {
            stmt.append(" START WITH " + start);
        }
        if (increment != null)
        {
            stmt.append(" INCREMENT BY " + increment);
        }
        if (max != null)
        {
            stmt.append(" MAXVALUE " + max);
        }
        else
        {
            stmt.append(" NO MAXVALUE");
        }
        if (min != null)
        {
            stmt.append(" MINVALUE " + min);
        }
        else
        {
            stmt.append(" NO MINVALUE");
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
        StringBuilder stmt=new StringBuilder("VALUES NEXT VALUE FOR ");
        stmt.append(sequence_name);
        stmt.append(" ");
        return stmt.toString();
    }
}
