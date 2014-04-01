/**********************************************************************
Copyright (c) 2009 Anton Troshin. All rights reserved.
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

**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.sql.DatabaseMetaData;

import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.mapping.RDBMSMappingManager;
import org.datanucleus.store.rdbms.mapping.datastore.TimesTenVarBinaryRDBMSMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to the Oracle Times Ten database
 */
public class TimesTenAdapter extends BaseDatastoreAdapter
{
    /**
     * A string containing the list of TimesTen reserved keywords
     */
    public static final String RESERVED_WORDS =
            "AGING,                  CROSS,           GROUP," +
            "ALL,                    CURRENT_SCHEMA,  HAVING," +
            "ANY,                    CURRENT_USER,    INNER," +
            "AS,                     CURSOR,          INT," +
            "BETWEEN,                DATASTORE_OWNER, INTEGER," +
            "BIGINT,                 DATE,            INTERSECT," +
            "BINARY,                 DEC,             INTERVAL," +
            "BINARY_DOUBLE_INFINITY, DECIMAL,         INTO," +
            "BINARY_DOUBLE_NAN,      DEFAULT,         IS," +
            "BINARY_FLOAT_INFINITY,  DESTROY,         JOIN," +
            "BINARY_FLOAT_NAN,       DISTINCT,        LEFT," +
            "CASE,                   DOUBLE,          LIKE," +
            "CHAR,                   FIRST,           LONG," +
            "CHARACTER,              FLOAT,           MINUS," +
            "COLUMN,                 FOR,             NATIONAL," +
            "CONNECTION,             FOREIGN,         NCHAR," +
            "CONSTRAINT,             FROM,            NO," +
            "NULL,                   RIGHT,           TINYINT," +
            "NUMERIC,                ROWNUM,          TT_SYSDATE," +
            "NVARCHAR,               ROWS,            UNION," +
            "ON,                     SELECT,          UNIQUE," +
            "ORA_SYSDATE,            SELF,            UPDATE," +
            "ORDER,                  SESSION_USER,    USER," +
            "PRIMARY,                SET,             USING," +
            "PROPAGATE,              SMALLINT,        VARBINARY," +
            "PUBLIC,                 SOME,            VARCHAR," +
            "READONLY,               SYSDATE,         VARYING," +
            "REAL,                   SYSTEM_USER,     WHEN," +
            "RETURN,                 TIME,            WHERE";


    /**
     * Constructor.
     * Overridden so we can add on our own list of NON SQL92 reserved words
     * which is returned incorrectly with the JDBC driver.
     *
     * @param metadata MetaData for the DB
     */
    public TimesTenAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(RESERVED_WORDS));

        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.add(UNIQUE_IN_END_CREATE_STATEMENTS);
        supportedOptions.remove(CHECK_IN_CREATE_STATEMENTS);
        supportedOptions.remove(NULLS_KEYWORD_IN_COLUMN_OPTIONS);
        supportedOptions.remove(ANSI_JOIN_SYNTAX);
        supportedOptions.remove(FK_DELETE_ACTION_NULL);
        supportedOptions.remove(FK_DELETE_ACTION_CASCADE);
        supportedOptions.remove(FK_DELETE_ACTION_DEFAULT);
        supportedOptions.remove(FK_DELETE_ACTION_RESTRICT);
        supportedOptions.remove(FK_UPDATE_ACTION_DEFAULT);
        supportedOptions.remove(FK_UPDATE_ACTION_RESTRICT);
        supportedOptions.remove(FK_UPDATE_ACTION_NULL);
        supportedOptions.remove(FK_UPDATE_ACTION_CASCADE);
        supportedOptions.remove(TX_ISOLATION_READ_UNCOMMITTED);
        supportedOptions.remove(TX_ISOLATION_REPEATABLE_READ);
        supportedOptions.remove(TX_ISOLATION_NONE);
    }

    @Override
    public String getVendorID()
    {
        return "timesten";
    }

    /**
     * Initialise the types for this datastore.
     * @param handler SchemaHandler that we initialise the types for
     * @param mconn Managed connection to use
     */
    public void initialiseTypes(StoreSchemaHandler handler, ManagedConnection mconn)
    {
        super.initialiseTypes(handler, mconn);

        // Re-register mapping for this adapter
        RDBMSStoreManager storeMgr = (RDBMSStoreManager)handler.getStoreManager();
        MappingManager mapMgr = storeMgr.getMappingManager();
        if (mapMgr instanceof RDBMSMappingManager)
        {
            RDBMSMappingManager rdbmsMapMgr = (RDBMSMappingManager)mapMgr;
            rdbmsMapMgr.deregisterDatastoreMappingsForJDBCType("VARBINARY");
            rdbmsMapMgr.registerDatastoreMapping("java.io.Serializable", TimesTenVarBinaryRDBMSMapping.class,
                "VARBINARY", "VARBINARY", true);
        }
    }

    /**
     * Returns the appropriate SQL to add a candidate key to its table.
     * It should return something like:
     * <pre>
     * ALTER TABLE FOO ADD CONSTRAINT FOO_CK (BAZ)
     * ALTER TABLE FOO ADD (BAZ)
     * </pre>
     *
     * @param ck An object describing the candidate key.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    public String getAddCandidateKeyStatement(CandidateKey ck, IdentifierFactory factory)
    {
        Index idx = new Index(ck);
        idx.setName(ck.getName());
        return getCreateIndexStatement(idx, factory);
    }

    /**
     * Accessor for the SQL statement to add a column to a table.
     *
     * @param table The table
     * @param col The column
     * @return The SQL necessary to add the column
     */
    public String getAddColumnStatement(Table table, Column col)
    {
        String stmnt = super.getAddColumnStatement(table, col);
        // Add non-nullable column has not been implemented in TimesTen
        return stmnt.replaceAll("NOT NULL", "" );
    }

    /**
     * Returns the appropriate SQL to add a foreign key to its table.
     * It should return something like:
     * <pre>
     * ALTER TABLE FOO ADD CONSTRAINT FOO_FK1 FOREIGN KEY (BAR, BAZ) REFERENCES ABC (COL1, COL2)
     * ALTER TABLE FOO ADD FOREIGN KEY (BAR, BAZ) REFERENCES ABC (COL1, COL2)
     * </pre>
     *
     * @param fk An object describing the foreign key.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    @Override
    public String getAddForeignKeyStatement(ForeignKey fk, IdentifierFactory factory)
    {
        //Bug TimesTen TT3000: self-referencing foreign keys are not allowed
        if (isSelfReferencingForeignKey(fk))
        {
            return getDatastoreDateStatement();
        }
        return super.getAddForeignKeyStatement(fk, factory);
    }

    /**
     * Returns true if foreign key is self-referencing
     * @param fk foreign key
     * @return true if foreign key is self-referencing
     */
    private static boolean isSelfReferencingForeignKey(ForeignKey fk)
    {
        if (fk != null)
        {
            String sql = fk.toString();
            Table obj = fk.getTable();
            if (obj != null)
            {
                String container = obj.toString();
                return isSelfReferencingForeignKey(sql, container);
            }
        }
        return false;
    }

    /**
     * Returns true if foreign key is self-referencing
     *
     * @param sql foreign key creation statement
     * @param ref referenced table
     * @return true if foreign key is self-referencing
     */
    private static boolean isSelfReferencingForeignKey(String sql, String ref)
    {
        if (sql != null && ref != null)
        {
            final String REFERENCES = "REFERENCES";
            // FOREIGN KEY (PARENT_BASEPROFILE_ID_OID) REFERENCES BASEPROFILE (BASEPROFILE_ID)
            int refi = sql.indexOf(REFERENCES);
            if (refi != -1)
            {
                String cut = sql.substring(refi + REFERENCES.length());
                int spacei = cut.trim().indexOf(" ");
                if (spacei != -1)
                {
                    return cut.substring(0, spacei + 1).trim().equalsIgnoreCase(ref);
                }
                else
                {
                    return cut.trim().equalsIgnoreCase(ref);
                }
            }
        }
        return false;
    }

    /**
     * Accessor for a statement that will return the statement to use to get the datastore date.
     * @return SQL statement to get the datastore date
     */
    public String getDatastoreDateStatement()
    {
        return "select tt_sysdate from dual";
    }
}