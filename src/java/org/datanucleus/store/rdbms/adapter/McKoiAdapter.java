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
2003 Andy Jefferson - coding standards
2004 Erik Bengtson - added sequence handling
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.transaction.TransactionIsolation;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to the McKoi database Server database.
 */
public class McKoiAdapter extends BaseDatastoreAdapter
{
    private static final String MCKOI_RESERVED_WORDS =
        "ACCOUNT,ACTION,ADD,AFTER,ALL,ALTER," +
        "AND,ANY,AS,ASC,AUTO,BEFORE," +
        "BETWEEN,BIGINT,BINARY,BIT,BLOB,BOOLEAN," +
        "BOTH,BY,CACHE,CALL,CALLBACK,CANONICAL_DECOMPOSITION," +
        "CASCADE,CAST,CHAR,CHARACTER,CHECK,CLOB," +
        "COLLATE,COLUMN,COMMIT,COMMITTED,COMPACT,CONSTRAINT," +
        "COUNT,CREATE,CROSS,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP," +
        "CYCLE,DATE,DECIMAL,DEFAULT,DEFERRABLE,DEFERRED," +
        "DELETE,DESC,DESCRIBE,DISTINCT,DOUBLE,DROP," +
        "EACH,EXCEPT,EXECUTE,EXISTS,EXPLAIN,FLOAT," +
        "FOR,FOREIGN,FROM,FULL_DECOMPOSITION,FUNCTION,GRANT," +
        "GROUP,GROUPS,HAVING,IDENTICAL_STRENGTH,IF,IGNORE," +
        "IMMEDIATE,IN,INCREMENT,INDEX,INDEX_BLIST,INDEX_NONE," +
        "INITIALLY,INNER,INSERT,INT,INTEGER,INTERSECT," +
        "INTO,IS,ISOLATION,JAVA,JAVA_OBJECT,JOIN," +
        "KEY,LANGUAGE,LEADING,LEFT,LEVEL,LIKE," +
        "LIMIT,LOCK,LONG,LONGVARBINARY,LONGVARCHAR,MAX," +
        "MAXVALUE,MINVALUE,NAME,NATURAL,NEW,NO," +
        "NO_DECOMPOSITION,NOT,NUMERIC,OLD,ON,OPTIMIZE," +
        "OPTION,OR,ORDER,OUTER,PASSWORD,PRIMARY," +
        "PRIMARY_STRENGTH,PRIVILEGES,PROCEDURE,PUBLIC,READ,REAL," +
        "REFERENCES,REGEX,REPEATABLE,RESTRICT,RETURN,RETURNS," +
        "REVOKE,RIGHT,ROLLBACK,ROW,SCHEMA,SECONDARY_STRENGTH," +
        "SELECT,SEQUENCE,SERIALIZABLE,SET,SHOW,SHUTDOWN," +
        "SMALLINT,SOME,START,STRING,TABLE,TEMPORARY," +
        "TERTIARY_STRENGTH,TEXT,TIME,TIMESTAMP,TINYINT,TO," +
        "TRAILING,TRANSACTION,TRIGGER,TRIM,UNCOMMITTED,UNION," +
        "UNIQUE,UNLOCK,UPDATE,USAGE,USE,USER," +
        "USING,VALUES,VARBINARY,VARCHAR,VARYING,VIEW," +
        "WHERE,WITH";
        
    /**
     * Constructs a McKoi SQL adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public McKoiAdapter(DatabaseMetaData metadata)
    {
        super(metadata);
        
        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(MCKOI_RESERVED_WORDS));

        supportedOptions.add(SEQUENCES);
        supportedOptions.add(USE_UNION_ALL);
        supportedOptions.remove(ESCAPE_EXPRESSION_IN_LIKE_PREDICATE);
        supportedOptions.remove(TX_ISOLATION_READ_COMMITTED);
        supportedOptions.remove(TX_ISOLATION_READ_UNCOMMITTED);
        supportedOptions.remove(TX_ISOLATION_REPEATABLE_READ);
        supportedOptions.remove(TX_ISOLATION_NONE);
    }

    public String getVendorID()
    {
        return "mckoi";
    }

    public boolean isReservedKeyword(String word)
    {
        /*
         * mcKoi does not support this "AAA".COL or AAA."COL"
         * mcKoi only works like this "AAA"."COL". For this
         * reason we will say that everything is a reserved word McKoi 1.0.3
         */
        return true;
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

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new org.datanucleus.store.rdbms.schema.McKoiTypeInfo(rs);
    }

    /**
     * Accessor for the "required" transaction isolation level if it has to be a certain value
     * for this adapter. McKoi requires TRANSACTION_SERIALIZABLE.
     * @return Transaction isolation level (-1 implies no restriction)
     */
    public int getRequiredTransactionIsolationLevel()
    {
        return TransactionIsolation.SERIALIZABLE;
    }

    /**
     * Returns the appropriate SQL to drop the given table. It should return something like:
     * <pre>DROP TABLE FOO</pre>
     * @param table The table to drop.
     * @return The text of the SQL statement.
     */
    public String getDropTableStatement(Table table)
    {
        return "DROP TABLE " + table.toString();
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
            Integer min, Integer max, Integer start, Integer increment, Integer cache_size)
    {
        if (sequence_name == null)
        {
            throw new NucleusUserException(LOCALISER.msg("051028"));
        }
        
        StringBuilder stmt = new StringBuilder("CREATE SEQUENCE ");
        stmt.append(sequence_name);
        if (increment != null)
        {
            stmt.append(" INCREMENT " + increment);
        }
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
            stmt.append(" START " + start);
        }
        if (cache_size != null)
        {
            stmt.append(" CACHE " + cache_size);
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
            throw new NucleusUserException(LOCALISER.msg("051028"));
        }
        StringBuilder stmt=new StringBuilder("SELECT ");
        stmt.append(" NEXTVAL('"+sequence_name+"') ");

        return stmt.toString();
    }
}