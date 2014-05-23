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
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to the DB2 database.
 * @see BaseDatastoreAdapter
 */
public class DB2Adapter extends BaseDatastoreAdapter
{
    /**
     * A string containing the list of DB2 keywords 
     * This list is normally obtained dynamically from the driver using "DatabaseMetaData.getSQLKeywords()".
     * Based on database DB2 version 7.
     */
    public static final String DB2_RESERVED_WORDS =
        "ACCESS,ALIAS,ALLOW,ASUTIME,AUDIT,AUX,AUXILIARY,BUFFERPOOL," +
        "CAPTURE,CCSID,CLUSTER,COLLECTION,COLLID,COMMENT,CONCAT," +
        "CONTAINS,COUNT_BIG,CURRENT_LC_PATH,CURRENT_SERVER," +
        "CURRENT_TIMEZONE,DATABASE,DAYS,DB2GENERAL,DB2SQL,DBA," +
        "DBINFO,DBSPACE,DISALLOW,DSSIZE,EDITPROC,ERASE,EXCLUSIVE," +
        "EXPLAIN,FENCED,FIELDPROC,FILE,FINAL,GENERATED,GRAPHIC,HOURS," +
        "IDENTIFIED,INDEX,INTEGRITY,ISOBID,JAVA,LABEL,LC_CTYPE,LINKTYPE," +
        "LOCALE,LOCATORS,LOCK,LOCKSIZE,LONG,MICROSECOND,MICROSECONDS," +
        "MINUTES,MODE,MONTHS,NAME,NAMED,NHEADER,NODENAME,NODENUMBER," +
        "NULLS,NUMPARTS,OBID,OPTIMIZATION,OPTIMIZE,PACKAGE,PAGE," +
        "PAGES,PART,PCTFREE,PCTINDEX,PIECESIZE,PLAN,PRIQTY,PRIVATE," +
        "PROGRAM,PSID,QYERYNO,RECOVERY,RENAME,RESET,RESOURCE,RRN,RUN," +
        "SCHEDULE,SCRATCHPAD,SECONDS,SECQTY,SECURITY,SHARE,SIMPLE," +
        "SOURCE,STANDARD,STATISTICS,STAY,STOGROUP,STORES,STORPOOL," +
        "STYLE,SUBPAGES,SYNONYM,TABLESPACE,TYPE,VALIDPROC,VARIABLE," +
        "VARIANT,VCAT,VOLUMES,WLM,YEARS";
    
    /**
     * Constructs a DB2 adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public DB2Adapter(DatabaseMetaData metadata)
    {
        super(metadata);

        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(DB2_RESERVED_WORDS));

        // Update supported options
        supportedOptions.add(LOCK_WITH_SELECT_FOR_UPDATE);
        supportedOptions.add(IDENTITY_COLUMNS);
        supportedOptions.add(SEQUENCES);
        supportedOptions.add(ANALYSIS_METHODS);
        supportedOptions.add(STORED_PROCEDURES);
        supportedOptions.add(USE_UNION_ALL);
        supportedOptions.add(ORDERBY_NULLS_DIRECTIVES);
        supportedOptions.remove(BOOLEAN_COMPARISON);
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.remove(NULLS_IN_CANDIDATE_KEYS);
        supportedOptions.remove(NULLS_KEYWORD_IN_COLUMN_OPTIONS);

        supportedOptions.remove(FK_DELETE_ACTION_DEFAULT);
        supportedOptions.remove(FK_UPDATE_ACTION_DEFAULT);
        supportedOptions.remove(FK_UPDATE_ACTION_CASCADE);
        supportedOptions.remove(FK_UPDATE_ACTION_NULL);
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
        SQLTypeInfo sqlType = new org.datanucleus.store.rdbms.schema.DB2TypeInfo(
            "FLOAT", (short)Types.FLOAT, 53, null, null, null, 1, false, (short)2,
            false, false, false, null, (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.FLOAT, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.schema.DB2TypeInfo(
            "NUMERIC", (short)Types.NUMERIC, 31, null, null, "PRECISION,SCALE", 1,
             false, (short)2, false, false, false, null, (short)0, (short)31, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.NUMERIC, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.schema.DB2TypeInfo(
            "BIGINT", (short)Types.BIGINT, 20, null, null, null, 1, false, (short)2,
            false, true, false, null, (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BIGINT, sqlType, true);

        sqlType = new org.datanucleus.store.rdbms.schema.DB2TypeInfo(
            "XML", (short)Types.SQLXML, 2147483647, null, null, null, 1, false, (short)2,
            false, false, false, null, (short)0, (short)0, 0);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.SQLXML, sqlType, true);

        // DB2 doesn't have "BIT" JDBC type mapped, so map as SMALLINT
        sqlType = new org.datanucleus.store.rdbms.schema.DB2TypeInfo(
            "SMALLINT", (short)Types.SMALLINT, 5, null, null, null, 1, false, (short)2,
            false, true, false, null, (short)0, (short)0, 10);
        addSQLTypeForJDBCType(handler, mconn, (short)Types.BIT, sqlType, true);
    }

    public String getVendorID()
    {
        return "db2";
    }
    
    public String getSchemaName(Connection conn) throws SQLException
    {
        Statement stmt = conn.createStatement();
        
        try
        {
            String stmtText = "VALUES (CURRENT SCHEMA)";
            ResultSet rs = stmt.executeQuery(stmtText);

            try
            {
                if (!rs.next())
                {
                    throw new NucleusDataStoreException("No result returned from " + stmtText).setFatal();
                }

                return rs.getString(1).trim();
            }
            finally
            {
                rs.close();
            }
        }
        finally
        {
            stmt.close();
        }
    }

    /**
     * Method to return the maximum length of a datastore identifier of the specified type.
     * If no limit exists then returns -1
     * @param identifierType Type of identifier (see IdentifierFactory.TABLE, etc)
     * @return The max permitted length of this type of identifier
     */
    public int getDatastoreIdentifierMaxLength(IdentifierType identifierType)
    {
        if (identifierType == IdentifierType.CANDIDATE_KEY)
        {
            return 18;
        }
        else if (identifierType == IdentifierType.FOREIGN_KEY)
        {
            return 18;
        }
        else if (identifierType == IdentifierType.INDEX)
        {
            return 18;
        }
        else if (identifierType == IdentifierType.PRIMARY_KEY)
        {
            return 18;
        }
        else
        {
            return super.getDatastoreIdentifierMaxLength(identifierType);
        }
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new org.datanucleus.store.rdbms.schema.DB2TypeInfo(rs);
    }

    /**
     * Method to create a column info for the current row.
     * Overrides the dataType/columnSize/decimalDigits to cater for DB2 particularities.
     * @param rs ResultSet from DatabaseMetaData.getColumns()
     * @return column info
     */
    public RDBMSColumnInfo newRDBMSColumnInfo(ResultSet rs)
    {
        RDBMSColumnInfo info = new RDBMSColumnInfo(rs);

        short dataType = info.getDataType();
        switch (dataType)
        {
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                // Values > 0 inexplicably get returned here.
                info.setDecimalDigits(0);
                break;
            default:
                break;
        }

        return info;
    }

    public String getDropDatabaseStatement(String schemaName, String catalogName)
    {
        throw new UnsupportedOperationException("DB2 does not support dropping schema with cascade. You need to drop all tables first");
    }

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
		return "VALUES IDENTITY_VAL_LOCAL()";
	}

	/**
	 * Accessor for the auto-increment keyword for generating DDLs (CREATE TABLEs...).
	 * @return The keyword for a column using auto-increment
	 */
	public String getAutoIncrementKeyword()
	{
		return "generated always as identity (start with 1)";
	}

    /**
     * Continuation string to use where the SQL statement goes over more than 1
     * line. DB2 doesn't convert newlines into continuation characters and so
     * we just provide a space so that it accepts the statement.
     * @return Continuation string.
     */
    public String getContinuationString()
    {
        return "";
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
        
        StringBuilder stmt = new StringBuilder("CREATE SEQUENCE ");
        stmt.append(sequence_name);
        stmt.append(" AS INTEGER ");

        if (start != null)
        {
            stmt.append(" START WITH " + start);
        }
        if (increment != null)
        {
            stmt.append(" INCREMENT BY " + increment);
        }
        if (min != null)
        {
            stmt.append(" MINVALUE " + min);
        }
        if (max != null)
        {
            stmt.append(" MAXVALUE " + max);
        }        
        if (cache_size != null)
        {
            stmt.append(" CACHE " + cache_size);
        }
        else
        {
            stmt.append(" NOCACHE");
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
        StringBuilder stmt=new StringBuilder("VALUES NEXTVAL FOR ");
        stmt.append(sequence_name);

        return stmt.toString();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatabaseAdapter#getRangeByRowNumberColumn()
     */
    public String getRangeByRowNumberColumn()
    {
        return "row_number()over()";
    }

    /**
     * return whether this exception represents a cancelled statement.
     * @param sqle the exception
     * @return whether it is a cancel
     */
    public boolean isStatementCancel(SQLException sqle)
    {
        if (sqle.getErrorCode() == -952)
        {
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatabaseAdapter#isStatementTimeout(java.sql.SQLException)
     */
    @Override
    public boolean isStatementTimeout(SQLException sqle)
    {
        if (sqle.getSQLState() != null && sqle.getSQLState().equalsIgnoreCase("57014") &&
            (sqle.getErrorCode() == -952 || sqle.getErrorCode() == -905))
        {
            return true;
        }

        return super.isStatementTimeout(sqle);
    }
}