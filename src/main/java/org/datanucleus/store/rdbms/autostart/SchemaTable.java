/**********************************************************************
Copyright (c) 2003 Andy Jefferson and others. All rights reserved.
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
2003 Andy Jefferson - reinstated SchemaTable, but with different purpose
                     The new SchemaTable supports insert, deleteAll and
                     fetchAll initially.
2004 Andy Jefferson - changed to use Logger
2004 Andy Jefferson - Added type and version columns
2004 Andy Jefferson - Changed to use StoreData. Added owner column
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.autostart;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.TableImpl;

/**
 * Class defining DataNucleus schema definition tables. Represents a table in the
 * datastore storing the class and table mappings. This table is used when
 * restarting a DataNucleus system so that it is 'aware' of what classes were
 * supported the previous time this datastore was used. It uses this
 * information to pre-populate the RDBMSManager with the classes stored in this
 * table. The table names are not used as such, other than as a record of what
 * table a class maps to - because it goes off and finds the MetaData for the
 * class which, with the DataNucleus naming scheme, defines the table name anyway.
 **/ 
public class SchemaTable extends TableImpl
{
    private JavaTypeMapping classMapping=null;
    private JavaTypeMapping tableMapping=null;
    private JavaTypeMapping typeMapping=null;
    private JavaTypeMapping ownerMapping=null;
    private JavaTypeMapping versionMapping=null;
    private JavaTypeMapping interfaceNameMapping=null;

    private String insertStmt=null;
    private String deleteStmt=null;
    private String deleteAllStmt=null;
    private String fetchAllStmt=null;
    private String fetchStmt=null;

    /**
     * Constructor.
     * @param storeMgr The RDBMSManager for this datastore
     * @param tableName Name of the starter table (optional, uses NUCLEUS_TABLES when this is null)
     **/
    public SchemaTable(RDBMSStoreManager storeMgr, String tableName)
    {
        super(storeMgr.getIdentifierFactory().newTableIdentifier(
            (tableName != null ? tableName : "NUCLEUS_TABLES")), storeMgr);
    }

    /**
     * Method to initialise the table.
     * @param clr The ClassLoaderResolver
     **/
    public void initialize(ClassLoaderResolver clr)
    {
        assertIsUninitialized();

        IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
        MappingManager mapMgr = getStoreManager().getMappingManager();
        classMapping = mapMgr.getMapping(String.class);
        Column class_column= addColumn(String.class.getName(), idFactory.newColumnIdentifier("CLASS_NAME"), classMapping, null);
        mapMgr.createDatastoreMapping(classMapping, class_column, String.class.getName());
        class_column.getColumnMetaData().setLength(128);
        class_column.getColumnMetaData().setJdbcType(JdbcType.VARCHAR);
        class_column.setPrimaryKey();

        tableMapping = mapMgr.getMapping(String.class);
        Column table_column= addColumn(String.class.getName(), idFactory.newColumnIdentifier("TABLE_NAME"), tableMapping, null);
        mapMgr.createDatastoreMapping(tableMapping, table_column, String.class.getName());
        table_column.getColumnMetaData().setLength(128);
        table_column.getColumnMetaData().setJdbcType(JdbcType.VARCHAR);

        typeMapping = mapMgr.getMapping(String.class);
        Column type_column= addColumn(String.class.getName(), idFactory.newColumnIdentifier("TYPE"), typeMapping, null);
        mapMgr.createDatastoreMapping(typeMapping, type_column, String.class.getName());
        type_column.getColumnMetaData().setLength(4);
        type_column.getColumnMetaData().setJdbcType(JdbcType.VARCHAR);

        // TODO Change type to SMALLINT/BIT
        ownerMapping = mapMgr.getMapping(String.class);
        Column owner_column = addColumn(String.class.getName(), idFactory.newColumnIdentifier("OWNER"), ownerMapping, null);
        mapMgr.createDatastoreMapping(ownerMapping, owner_column, String.class.getName());
        owner_column.getColumnMetaData().setLength(2);
        owner_column.getColumnMetaData().setJdbcType(JdbcType.VARCHAR);

        versionMapping = mapMgr.getMapping(String.class);
        Column version_column = addColumn(String.class.getName(), idFactory.newColumnIdentifier("VERSION"), versionMapping, null);
        mapMgr.createDatastoreMapping(versionMapping, version_column, String.class.getName());
        version_column.getColumnMetaData().setLength(20);
        version_column.getColumnMetaData().setJdbcType(JdbcType.VARCHAR);

        interfaceNameMapping = mapMgr.getMapping(String.class);
        Column interfaceName_column = addColumn(String.class.getName(), idFactory.newColumnIdentifier("INTERFACE_NAME"), interfaceNameMapping, null);
        mapMgr.createDatastoreMapping(interfaceNameMapping, interfaceName_column, String.class.getName());
        interfaceName_column.getColumnMetaData().setLength(255);
        interfaceName_column.getColumnMetaData().setJdbcType(JdbcType.VARCHAR);
        interfaceName_column.setNullable(true);
        
        // Set up JDBC statements for supported operations
        insertStmt = "INSERT INTO " + identifier.getFullyQualifiedName(false) + " (" + class_column.getIdentifier() + "," + table_column.getIdentifier() + "," + type_column.getIdentifier() + "," + 
            owner_column.getIdentifier() + "," + version_column.getIdentifier() + "," + interfaceName_column.getIdentifier() + ") VALUES (?,?,?,?,?,?)";
        deleteStmt = "DELETE FROM " + identifier.getFullyQualifiedName(false) + " WHERE " + idFactory.getIdentifierInAdapterCase("CLASS_NAME") + "=?";
        deleteAllStmt = "DELETE FROM " + identifier.getFullyQualifiedName(false);
        fetchAllStmt = "SELECT " + class_column.getIdentifier() + "," + table_column.getIdentifier() + "," + type_column.getIdentifier() + "," + 
            owner_column.getIdentifier() + "," + version_column.getIdentifier() + "," + interfaceName_column.getIdentifier() + " FROM " + identifier.getFullyQualifiedName(false) + " ORDER BY " + table_column.getIdentifier();
        fetchStmt = "SELECT 1 FROM " + identifier.getFullyQualifiedName(false) + " WHERE " + idFactory.getIdentifierInAdapterCase("CLASS_NAME") + " = ? ";

        state = TABLE_STATE_INITIALIZED;
    }

    /**
     * Accessor for a mapping for the ID (persistable) for this table.
     * @return The (persistable) ID mapping.
     **/
    public JavaTypeMapping getIdMapping()
    {
        throw new NucleusException("Attempt to get ID mapping of SchemaTable!").setFatal();
    }

    /**
     * Accessor for the classes already supported by this Schema Table.
     * @param conn Connection for this datastore.
     * @return The HashSet of class names (StoreData)
     * @throws SQLException Thrown when an error occurs in the process.
     **/
    public HashSet getAllClasses(ManagedConnection conn)
    throws SQLException
    {
        HashSet schema_data = new HashSet();

        if (storeMgr.getDdlWriter() != null && !tableExists((Connection) conn.getConnection()))
        {
            // do not query non-existing schema table when DDL is only written to file
            return schema_data;
        }
        else
        {
            SQLController sqlControl = storeMgr.getSQLController();
            PreparedStatement ps = sqlControl.getStatementForQuery(conn, fetchAllStmt);
            try
            {
                ResultSet rs = sqlControl.executeStatementQuery(null, conn, fetchAllStmt, ps);
                try
                {
                    while (rs.next())
                    {
                        StoreData data = new RDBMSStoreData(rs.getString(1), rs.getString(2), rs.getString(4).equals("1") ? true : false, 
                                rs.getString(3).equals("FCO") ? StoreData.FCO_TYPE : StoreData.SCO_TYPE, rs.getString(6));
                        schema_data.add(data);
                    }
                }
                finally
                {
                    rs.close();
                }
            }
            finally
            {
                sqlControl.closeStatement(conn, ps);
            }

            return schema_data;
        }
    }

    /**
     * Method to insert a row in the SchemaTable. This is called when DataNucleus is
     * now supporting a new class (and hence DB table).
     * @param data Data for the class
     * @param conn Connection to the datastore
     * @throws SQLException Thrown when an error occurs inserting the schema. 
     **/
    public void addClass(RDBMSStoreData data, ManagedConnection conn)
    throws SQLException
    {
        if (storeMgr.getDdlWriter() != null)
        {
            // No interest in actually adding the class to the table
            return;
        }
        if (hasClass(data, conn))
        {
            // Data already exists, so remove the chance of a duplicate insert
            return;
        }

        SQLController sqlControl = storeMgr.getSQLController();
        PreparedStatement ps = sqlControl.getStatementForUpdate(conn, insertStmt, false);
        try
        {
            int jdbc_id = 1;

            classMapping.setString(null, ps, MappingHelper.getMappingIndices(jdbc_id, classMapping), data.getName());
            jdbc_id += classMapping.getNumberOfDatastoreMappings();

            tableMapping.setString(null, ps, MappingHelper.getMappingIndices(jdbc_id, tableMapping), data.hasTable() ? data.getTableName() : "");
            jdbc_id += tableMapping.getNumberOfDatastoreMappings();

            typeMapping.setString(null, ps, MappingHelper.getMappingIndices(jdbc_id, typeMapping), data.isFCO() ? "FCO" : "SCO");
            jdbc_id += typeMapping.getNumberOfDatastoreMappings();

            ownerMapping.setString(null, ps, MappingHelper.getMappingIndices(jdbc_id, ownerMapping), data.isTableOwner() ? "1" : "0");
            jdbc_id += ownerMapping.getNumberOfDatastoreMappings();

            // TODO Sort out version
            versionMapping.setString(null, ps, MappingHelper.getMappingIndices(jdbc_id, versionMapping), "DataNucleus");
            jdbc_id += versionMapping.getNumberOfDatastoreMappings();

            interfaceNameMapping.setString(null, ps, MappingHelper.getMappingIndices(jdbc_id, interfaceNameMapping), data.getInterfaceName());
            jdbc_id += interfaceNameMapping.getNumberOfDatastoreMappings();

            sqlControl.executeStatementUpdate(null, conn, insertStmt, ps, true);

            // TODO : handle any warning messages
        }
        finally
        {
            sqlControl.closeStatement(conn, ps);
        }
    }

    /**
     * Method to verify the a class is already stored in the table.
     * @param data Data for the class
     * @param conn Connection to the datastore
     * @return if the SchemaTable already has the class
     * @throws SQLException Thrown when an error occurs inserting the schema. 
     **/
    private boolean hasClass(StoreData data, ManagedConnection conn)
    throws SQLException
    {
        if (!tableExists((Connection) conn.getConnection()))
        {
            return false;
        }
        else
        {
            SQLController sqlControl = storeMgr.getSQLController();
            PreparedStatement ps = sqlControl.getStatementForQuery(conn, fetchStmt);
            try
            {
                int jdbc_id = 1;
                tableMapping.setString(null, ps, MappingHelper.getMappingIndices(jdbc_id, tableMapping), data.getName());

                ResultSet rs = sqlControl.executeStatementQuery(null, conn, fetchStmt, ps);
                try
                {
                    if (rs.next())
                    {
                        return true;
                    }
                }
                finally
                {
                    rs.close();
                }
            }
            finally
            {
                sqlControl.closeStatement(conn, ps);
            }
            return false;
        }
    }
    
    /**
     * Method to delete a class from the SchemaTable.
     * This is called when DataNucleus is required to clean out support for a particular class.
     * @param class_name Name of class to delete
     * @param conn Connection to the datastore
     * @throws SQLException Thrown when an error occurs deleting the schema. 
     **/
    public void deleteClass(String class_name, ManagedConnection conn)
    throws SQLException
    {
        SQLController sqlControl = storeMgr.getSQLController();
        PreparedStatement ps = sqlControl.getStatementForUpdate(conn, deleteStmt, false);
        try
        {
            ps.setString(1, class_name);

            sqlControl.executeStatementUpdate(null, conn, deleteStmt, ps, true);

            // TODO : handle any warning messages
        }
        finally
        {
            sqlControl.closeStatement(conn, ps);
        }
    }

    /**
     * Method to delete all classes from the SchemaTable.
     * This is called when DataNucleus is required to clean out its supported classes
     * (and hence DB table).
     *
     * @param conn Connection to the datastore
     * @throws SQLException Thrown when an error occurs deleting the schema. 
     **/
    public void deleteAllClasses(ManagedConnection conn)
    throws SQLException
    {
        SQLController sqlControl = storeMgr.getSQLController();
        PreparedStatement ps = sqlControl.getStatementForUpdate(conn, deleteAllStmt, false);
        try
        {
            sqlControl.executeStatementUpdate(null, conn, deleteAllStmt, ps, true);

            // TODO : handle any warning messages
        }
        finally
        {
            sqlControl.closeStatement(conn, ps);
        }
    }

    /**
     * Convenience existence checker. This shouldn't be needed since exists already does this!
     * @param conn Connection to use
     * @return Whether it has been created
     * @throws SQLException
     */
    private boolean tableExists(Connection conn)
    throws SQLException
    {
        try
        {
            exists(conn, false);
            return true;
        }
        catch (MissingTableException mte)
        {
            return false;
        }
    }

    /**
     * Accessor the for the mapping for a field/property stored in this table.
     * @param mmd MetaData for the field whose mapping we want
     * @return The mapping
     */
    public JavaTypeMapping getMemberMapping(AbstractMemberMetaData mmd)
    {
        return null;
    }    
}