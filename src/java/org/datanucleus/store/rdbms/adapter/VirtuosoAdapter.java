/**********************************************************************
Copyright (c) 2013 Enman SARL. All rights reserved. 
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
2013 Emmanuel Poitier - initial version
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.util.NucleusLogger;

/**
 * Provides methods for adapting SQL language elements to the Virtuoso database.
 * See http://virtuoso.openlinksw.com/dataspace/dav/wiki/Main/
 */
public class VirtuosoAdapter extends BaseDatastoreAdapter
{
    public VirtuosoAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.add(PRIMARYKEY_IN_CREATE_STATEMENTS);
        supportedOptions.add(STORED_PROCEDURES);
        supportedOptions.add(IDENTITY_COLUMNS);
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new org.datanucleus.store.rdbms.schema.VirtuosoTypeInfo(rs);
    }

    public String getVendorID()
    {
        return "virtuoso";
    }

    /**
     * MySQL, when using AUTO_INCREMENT, requires the primary key specified
     * in the CREATE TABLE, so we do nothing here. 
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The PK statement
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        return null;
    }

    /**
     * Method to return the DROP TABLE statement.
     * Override the default omitting the CASCADE part since Virtuoso doesn't support that.
     * @param table The table
     * @return The drop statement
     **/ 
    public String getDropTableStatement(Table table)
    {
        return "DROP TABLE " + table.toString();
    }

    /**
     * Provide the existing indexes in the database for the table.
     * @param conn the JDBC connection
     * @param catalog the catalog name
     * @param schema the schema name.
     * @param table the table name
     * @return a ResultSet with the format @see DatabaseMetaData#getIndexInfo(java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
     * @throws SQLException if an error occurs
     */
    public ResultSet getExistingIndexes(Connection conn, String catalog, String schema, String table) 
    throws SQLException
    {
        String GET_INDEXES_STMT = "SELECT name_part(SYS_KEYS.KEY_TABLE,0) AS table_cat, " + 
                "name_part(SYS_KEYS.KEY_TABLE,1) AS table_schem, name_part(SYS_KEYS.KEY_TABLE,2) AS table_name, " +
                "iszero(SYS_KEYS.KEY_IS_UNIQUE) AS non_unique, name_part(SYS_KEYS.KEY_TABLE,0) AS index_qualifier, " +
                "SYS_KEYS.KEY_NAME AS index_name, ((SYS_KEYS.KEY_IS_OBJECT_ID*8)+(3-(2*iszero(SYS_KEYS.KEY_CLUSTER_ON_ID)))) AS type, " +
                "(SYS_KEY_PARTS.KP_NTH+1) AS ordinal_position, SYS_COLS.\\COLUMN AS column_name, NULL AS asc_or_desc, NULL AS cardinality, " +
                "NULL AS pages, NULL AS filter_condition FROM DB.DBA.SYS_KEYS SYS_KEYS, DB.DBA.SYS_KEY_PARTS SYS_KEY_PARTS, "+
                "DB.DBA.SYS_COLS SYS_COLS WHERE name_part(SYS_KEYS.KEY_TABLE,0) LIKE ? AND " + 
                "__any_grants(SYS_KEYS.KEY_TABLE) AND name_part(SYS_KEYS.KEY_TABLE,1) LIKE ? AND " +
                "name_part(SYS_KEYS.KEY_TABLE,2) LIKE ? AND SYS_KEYS.KEY_MIGRATE_TO IS NULL " +
                "AND SYS_KEY_PARTS.KP_KEY_ID=SYS_KEYS.KEY_ID AND SYS_KEY_PARTS.KP_NTH < SYS_KEYS.KEY_DECL_PARTS " +
                "AND SYS_COLS.COL_ID=SYS_KEY_PARTS.KP_COL AND SYS_COLS.\\COLUMN<>'_IDN' AND SYS_KEYS.KEY_IS_MAIN=0";

        if (catalog == null) catalog = conn.getCatalog();

        NucleusLogger.DATASTORE_SCHEMA.debug("Retrieving table indexes using the following SQL : " + GET_INDEXES_STMT);
        NucleusLogger.DATASTORE_SCHEMA.debug("Catalog: " + catalog + " Schema: " + schema + " Table: " + table);
        PreparedStatement stmt = conn.prepareStatement(GET_INDEXES_STMT);
        stmt.setString(1,catalog);
        stmt.setString(2,schema);
        stmt.setString(3,table);
        return stmt.executeQuery();
    }

    /**
     * Accessor for the auto-increment sql statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest auto-increment key
     **/
    public String getAutoIncrementStmt(Table table, String columnName)
    {
        return "SELECT identity_value()";
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
     * Accessor for a statement that will return the statement to use to get the datastore date.
     * @return SQL statement to get the datastore date
     */
    public String getDatastoreDateStatement()
    {
        return "SELECT now()";
    }
}