/**********************************************************************
Copyright (c) 2002 Mike Martin and others. All rights reserved.
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
2003 Erik Bengtson  - removed unused variable
2003 Andy Jefferson - localised
2003 Andy Jefferson - addition of checkExists, made getExpectedPK protected
2004 Andy Jefferson - rewritten to remove many levels of inheritance
2004 Andy Jefferson - added validation of columns, and auto create option
2004 Andy Jefferson - separated out validatePK
2008 Andy Jefferson - changed all schema accessors to use StoreSchemaHandler
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.exceptions.NoTableManagedException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.exceptions.MissingColumnException;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;
import org.datanucleus.store.rdbms.exceptions.NotATableException;
import org.datanucleus.store.rdbms.exceptions.UnexpectedColumnException;
import org.datanucleus.store.rdbms.exceptions.WrongPrimaryKeyException;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.schema.ForeignKeyInfo;
import org.datanucleus.store.rdbms.schema.IndexInfo;
import org.datanucleus.store.rdbms.schema.PrimaryKeyInfo;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.RDBMSSchemaHandler;
import org.datanucleus.store.rdbms.schema.RDBMSTableFKInfo;
import org.datanucleus.store.rdbms.schema.RDBMSTableIndexInfo;
import org.datanucleus.store.rdbms.schema.RDBMSTablePKInfo;
import org.datanucleus.store.schema.StoreSchemaData;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Class representing a table in a datastore (RDBMS).
 * Provides a series of methods for validating the aspects of the table, namely
 * <UL>
 * <LI>validate - Validate the table</LI>
 * <LI>validateColumns - Validate the columns in the table</LI>
 * <LI>validatePrimaryKey - Validate the primary key</LI>
 * <LI>validateIndices - Validate the indices on the table</LI>
 * <LI>validateForeignKeys - Validate all FKs for the table</LI>
 * <LI>validateConstraints - Validate the indices and FKs.</LI>
 * </UL>
 */
public abstract class TableImpl extends AbstractTable
{
    /**
     * Constructor.
     * @param name The name of the table (in SQL).
     * @param storeMgr The StoreManager for this table.
     */
    public TableImpl(DatastoreIdentifier name, RDBMSStoreManager storeMgr)
    {
        super(name, storeMgr);
    }

    /**
     * Accessor for the primary key for this table. 
     * Will always return a PrimaryKey but if we have defined no columns, the pk.size() will be 0.
     * @return The primary key.
     */
    public PrimaryKey getPrimaryKey()
    {
        PrimaryKey pk = new PrimaryKey(this);
        Iterator i = columns.iterator();
        while (i.hasNext())
        {
            Column col = (Column) i.next();
            if (col.isPrimaryKey())
            {
                pk.addColumn(col);
            }
        }

        return pk;
    }

    /**
     * Method to validate the table in the datastore.
     * @param conn The JDBC Connection
     * @param validateColumnStructure Whether to validate the column structure, or just the column existence
     * @param autoCreate Whether to update the table to fix any validation errors. Only applies to missing columns.
     * @param autoCreateErrors Exceptions found in the "auto-create" process
     * @return Whether the database was modified
     * @throws SQLException Thrown when an error occurs in the JDBC calls
     */
    @Override
    public boolean validate(Connection conn, boolean validateColumnStructure, boolean autoCreate, Collection<Throwable> autoCreateErrors)
    throws SQLException
    {
        assertIsInitialized();

        long startTime = System.currentTimeMillis();
        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("057032", this));
        }

        // Check existence and validity
        RDBMSSchemaHandler handler = (RDBMSSchemaHandler)storeMgr.getSchemaHandler();
        String tableType = handler.getTableType(conn, this);
        if (tableType == null)
        {
            throw new MissingTableException(getCatalogName(), getSchemaName(), this.toString());
        }
        else if (!tableType.equals("TABLE") && !tableType.equals("BASE TABLE") && !tableType.equals("PARTITIONED TABLE"))
        {
            throw new NotATableException(this.toString(), tableType);
        }

        // Validate the column(s)
        validateColumns(conn, validateColumnStructure, autoCreate, autoCreateErrors);

        // Validate the primary key(s)
        try
        {
            validatePrimaryKey(conn);
        }
        catch (WrongPrimaryKeyException wpke)
        {
            if (autoCreateErrors != null)
            {
                autoCreateErrors.add(wpke);
            }
            else
            {
                throw wpke;
            }
        }

        state = TABLE_STATE_VALIDATED;
        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("045000", (System.currentTimeMillis() - startTime)));
        }

        return false;
    }

    /**
     * Utility to validate the columns of the table.
     * Will throw a MissingColumnException if a column is not found (and is not required to auto create it)
     * @param conn Connection to use for validation
     * @param validateColumnStructure Whether to validate down to the structure of the columns, or just their existence
     * @param autoCreate Whether to auto create any missing columns
     * @param autoCreateErrors Exceptions found in the "auto-create" process
     * @return Whether it validates
     * @throws SQLException Thrown if an error occurs in the validation process
     */
    public boolean validateColumns(Connection conn, boolean validateColumnStructure, boolean autoCreate, Collection autoCreateErrors)
    throws SQLException
    {
        Map<DatastoreIdentifier, Column> unvalidated = new HashMap(columnsByIdentifier);
        List<StoreSchemaData> tableColInfo = storeMgr.getColumnInfoForTable(this, conn);
        Iterator i = tableColInfo.iterator();
        while (i.hasNext())
        {
            RDBMSColumnInfo ci = (RDBMSColumnInfo) i.next();

            // Create an identifier to use for the real column - use "CUSTOM" because we don't want truncation
            DatastoreIdentifier colIdentifier = storeMgr.getIdentifierFactory().newColumnIdentifier(ci.getColumnName(), 
                this.storeMgr.getNucleusContext().getTypeManager().isDefaultEmbeddedType(String.class), null, true);
            Column col = unvalidated.get(colIdentifier);
            if (col != null)
            {
                if (validateColumnStructure)
                {
                    col.initializeColumnInfoFromDatastore(ci);
                    col.validate(ci);
                    unvalidated.remove(colIdentifier);
                }
                else
                {
                    unvalidated.remove(colIdentifier);
                }
            }
        }

        if (unvalidated.size() > 0)
        {
            if (autoCreate)
            {
                // Add all missing columns
                List<String> stmts = new ArrayList<>();
                for (Column col : unvalidated.values())
                {
                    String addColStmt = dba.getAddColumnStatement(this, col);
                    stmts.add(addColStmt);
                }

                try
                {
                    executeDdlStatementList(stmts, conn);
                }
                catch (SQLException sqle)
                {
                    if (autoCreateErrors != null)
                    {
                        autoCreateErrors.add(sqle);
                    }
                    else
                    {
                        throw sqle;
                    }
                }

                // Invalidate the cached information for this table since it now has a new column
                storeMgr.invalidateColumnInfoForTable(this);
            }
            else
            {
                MissingColumnException mce = new MissingColumnException(this, unvalidated.values());
                if (autoCreateErrors != null)
                {
                    autoCreateErrors.add(mce);
                }
                else
                {
                    throw mce;
                }
            }
        }
        state = TABLE_STATE_VALIDATED;

        return true;
    }

    /**
     * Utility to load the structure/metadata of primary key columns of the table.
     * @param conn Connection to use for validation
     * @throws SQLException Thrown if an error occurs in the initialization process
     */
    public void initializeColumnInfoForPrimaryKeyColumns(Connection conn)
    throws SQLException
    {
        for (Column col : columnsByIdentifier.values())
        {
            if (col.isPrimaryKey())
            {
                RDBMSColumnInfo ci = storeMgr.getColumnInfoForColumnName(this, conn, col.getIdentifier());
                if (ci != null)
                {
                    col.initializeColumnInfoFromDatastore(ci);
                }
            }
        }
    }

    /**
     * Initialize the default value for columns if null using the values from the datastore.
     * @param conn The JDBC Connection
     * @throws SQLException Thrown if an error occurs in the default initialisation.
     */
    public void initializeColumnInfoFromDatastore(Connection conn)
    throws SQLException
    {
        Map<DatastoreIdentifier, Column> columns = new HashMap(columnsByIdentifier);
        Iterator i = storeMgr.getColumnInfoForTable(this, conn).iterator();
        while (i.hasNext())
        {
            RDBMSColumnInfo ci = (RDBMSColumnInfo) i.next();
            DatastoreIdentifier colName = storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, ci.getColumnName());
            Column col = columns.get(colName);
            if (col != null)
            {
                col.initializeColumnInfoFromDatastore(ci);
            }
        }
    }

    /**
     * Utility method to validate the primary key of the table.
     * Will throw a WrongPrimaryKeyException if the PK is incorrect.
     * TODO Add an auto_create parameter on this
     * @param conn Connection to use
     * @return Whether it validates
     * @throws SQLException When an error occurs in the valdiation
     */
    protected boolean validatePrimaryKey(Connection conn)
    throws SQLException
    {
        Map actualPKs = getExistingPrimaryKeys(conn);
        PrimaryKey expectedPK = getPrimaryKey();
        if (expectedPK.getNumberOfColumns() == 0)
        {
            if (!actualPKs.isEmpty())
            {
                throw new WrongPrimaryKeyException(this.toString(), expectedPK.toString(), StringUtils.collectionToString(actualPKs.values()));
            }
        }
        else
        {
            if (actualPKs.size() != 1 || !actualPKs.values().contains(expectedPK))
            {
                throw new WrongPrimaryKeyException(this.toString(), expectedPK.toString(), StringUtils.collectionToString(actualPKs.values()));
            }
        }

        return true;
    }

    /**
     * Method to validate any constraints, and auto create them if required.
     * @param conn The JDBC Connection
     * @param autoCreate Whether to auto create the constraints if not existing
     * @param autoCreateErrors Errors found in the "auto-create" process
     * @param clr The ClassLoaderResolver
     * @return Whether the database was modified
     * @throws SQLException Thrown when an error occurs in the JDBC calls
     */
    public boolean validateConstraints(Connection conn, boolean autoCreate, Collection autoCreateErrors, ClassLoaderResolver clr)
    throws SQLException
    {
        assertIsInitialized();
        boolean cksWereModified;
        boolean fksWereModified;
        boolean idxsWereModified;
        if (dba.supportsOption(DatastoreAdapter.CREATE_INDEXES_BEFORE_FOREIGN_KEYS))
        {
            idxsWereModified = validateIndices(conn, autoCreate, autoCreateErrors, clr);
            fksWereModified = validateForeignKeys(conn, autoCreate, autoCreateErrors, clr);
            cksWereModified = validateCandidateKeys(conn, autoCreate, autoCreateErrors);
        }
        else
        {
            cksWereModified = validateCandidateKeys(conn, autoCreate, autoCreateErrors);
            fksWereModified = validateForeignKeys(conn, autoCreate, autoCreateErrors, clr);
            idxsWereModified = validateIndices(conn, autoCreate, autoCreateErrors, clr);
        }

        return fksWereModified || idxsWereModified || cksWereModified;
    }

    /**
     * Method used to create all constraints for a brand new table.
     * @param conn The JDBC Connection
     * @param autoCreateErrors Errors found in the "auto-create" process
     * @param clr The ClassLoaderResolver
     * @return Whether the database was modified
     * @throws SQLException Thrown when an error occurs in the JDBC calls
     */
    public boolean createConstraints(Connection conn, Collection autoCreateErrors, ClassLoaderResolver clr)
    throws SQLException
    {
        assertIsInitialized();

        boolean cksWereModified;
        boolean fksWereModified;
        boolean idxsWereModified;
        if (dba.supportsOption(DatastoreAdapter.CREATE_INDEXES_BEFORE_FOREIGN_KEYS))
        {
            idxsWereModified = createIndices(conn, autoCreateErrors, clr, Collections.EMPTY_MAP);
            fksWereModified = createForeignKeys(conn, autoCreateErrors, clr, Collections.EMPTY_MAP);
            cksWereModified = createCandidateKeys(conn, autoCreateErrors, Collections.EMPTY_MAP);
        }
        else
        {
            cksWereModified = createCandidateKeys(conn, autoCreateErrors, Collections.EMPTY_MAP);
            fksWereModified = createForeignKeys(conn, autoCreateErrors, clr, Collections.EMPTY_MAP);
            idxsWereModified = createIndices(conn, autoCreateErrors, clr, Collections.EMPTY_MAP);
        }

        return fksWereModified || idxsWereModified || cksWereModified;
    }

    /**
     * Method to validate any foreign keys on this table in the datastore, and
     * auto create any that are missing where required.
     * 
     * @param conn The JDBC Connection
     * @param autoCreate Whether to auto create the FKs if not existing
     * @param autoCreateErrors Errors found during the auto-create process
     * @return Whether the database was modified
     * @throws SQLException Thrown when an error occurs in the JDBC calls
     */
    private boolean validateForeignKeys(Connection conn, boolean autoCreate, Collection autoCreateErrors, ClassLoaderResolver clr)
    throws SQLException
    {
        boolean dbWasModified = false;

        // Validate and/or create all foreign keys.
        Map actualForeignKeysByName = null;
        int numActualFKs = 0;
        if (storeMgr.getCompleteDDL())
        {
            actualForeignKeysByName = new HashMap();
        }
        else
        {
            actualForeignKeysByName = getExistingForeignKeys(conn);
            numActualFKs = actualForeignKeysByName.size();
            if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("058103", "" + numActualFKs, this));
            }
        }

        // Auto Create any missing foreign keys
        if (autoCreate)
        {
            dbWasModified = createForeignKeys(conn, autoCreateErrors, clr, actualForeignKeysByName);
        }
        else
        {
            Map stmtsByFKName = getSQLAddFKStatements(actualForeignKeysByName, clr);
            if (stmtsByFKName.isEmpty())
            {
                if (numActualFKs > 0)
                {
                    if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("058104", "" + numActualFKs,this));
                    }
                }
            }
            else
            {
                // We support existing schemas so don't raise an exception.
                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("058101", this, stmtsByFKName.values()));
            }
        }
        return dbWasModified;
    }

    /**
     * Method to create any foreign keys on this table in the datastore
     * 
     * @param conn The JDBC Connection
     * @param autoCreateErrors Errors found during the auto-create process
     * @param actualForeignKeysByName the current foreign keys 
     * @return Whether the database was modified
     * @throws SQLException Thrown when an error occurs in the JDBC calls
     */
    private boolean createForeignKeys(Connection conn, Collection autoCreateErrors, ClassLoaderResolver clr, Map actualForeignKeysByName)
    throws SQLException
    {
        // Auto Create any missing foreign keys
        Map<String, String> stmtsByFKName = getSQLAddFKStatements(actualForeignKeysByName, clr);
        Statement stmt = conn.createStatement();
        try
        {
            for (String stmtText : stmtsByFKName.values())
            {
                try
                {
                    executeDdlStatement(stmt, stmtText);
                }
                catch (SQLException sqle)
                {
                    if (autoCreateErrors != null)
                    {
                        autoCreateErrors.add(sqle);
                    }
                    else
                    {
                        throw sqle;
                    }
                }
            }
        }
        finally
        {
            stmt.close();
        }
        return !stmtsByFKName.isEmpty();
    }
    
    /**
     * Method to validate any indices for this table, and auto create any missing ones where required.
     * @param conn The JDBC Connection
     * @param autoCreate Whether to auto create any missing indices
     * @param autoCreateErrors Errors found during the auto-create process
     * @return Whether the database was changed
     * @throws SQLException Thrown when an error occurs in the JDBC calls
     */
    private boolean validateIndices(Connection conn, boolean autoCreate, Collection autoCreateErrors, ClassLoaderResolver clr)
    throws SQLException
    {
        boolean dbWasModified = false;

        // Retrieve the number of indexes in the datastore for this table.
        Map<DatastoreIdentifier, Index> actualIndicesByName = null;
        int numActualIdxs = 0;
        if (storeMgr.getCompleteDDL())
        {
            actualIndicesByName = new HashMap<>();
        }
        else
        {
            actualIndicesByName = getExistingIndices(conn);
            for (Index idx : actualIndicesByName.values())
            {
                if (idx.getTable().getIdentifier().toString().equals(identifier.toString()))
                {
                    // Table of the index is the same as this table so must be ours
                    ++numActualIdxs;
                }
            }
            if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("058004", "" + numActualIdxs, this));
            }
        }

        // Auto Create any missing indices
        if (autoCreate)
        {
            dbWasModified = createIndices(conn, autoCreateErrors, clr, actualIndicesByName);
        }
        else
        {
            Map stmtsByIdxName = getSQLCreateIndexStatements(actualIndicesByName, clr);
            if (stmtsByIdxName.isEmpty())
            {
                if (numActualIdxs > 0)
                {
                    if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("058005", "" + numActualIdxs, this));
                    }
                }
            }
            else
            {
                // We support existing schemas so don't raise an exception.
                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("058003", this, stmtsByIdxName.values()));
            }
        }
        return dbWasModified;
    }

    /**
     * Method to create any indices for this table
     * @param conn The JDBC Connection
     * @param autoCreateErrors Errors found during the auto-create process
     * @param actualIndicesByName the actual indices by name
     * @return Whether the database was changed
     * @throws SQLException Thrown when an error occurs in the JDBC calls
     */
    private boolean createIndices(Connection conn, Collection autoCreateErrors, ClassLoaderResolver clr, Map actualIndicesByName)
    throws SQLException
    {
        // Auto Create any missing indices
        Map<String, String> stmtsByIdxName = getSQLCreateIndexStatements(actualIndicesByName, clr);
        Statement stmt = conn.createStatement();
        try
        {
            for (String stmtText : stmtsByIdxName.values())
            {
                try
                {
                    executeDdlStatement(stmt, stmtText);
                }
                catch (SQLException sqle)
                {
                    if (autoCreateErrors != null)
                    {
                        autoCreateErrors.add(sqle);
                    }
                    else
                    {
                        throw sqle;
                    }
                }
            }
        }
        finally
        {
            stmt.close();
        }
        return !stmtsByIdxName.isEmpty();
    }
    
    /**
     * Method to validate any Candidate keys on this table in the datastore, and
     * auto create any that are missing where required.
     * 
     * @param conn The JDBC Connection
     * @param autoCreate Whether to auto create the Candidate Keys if not existing
     * @param autoCreateErrors Errors found during the auto-create process
     * @return Whether the database was modified
     * @throws SQLException Thrown when an error occurs in the JDBC calls
     */
    private boolean validateCandidateKeys(Connection conn, boolean autoCreate, Collection autoCreateErrors)
    throws SQLException
    {
        boolean dbWasModified = false;

        // Validate and/or create all candidate keys.
        Map<DatastoreIdentifier, CandidateKey> actualCandidateKeysByName = null;
        int numActualCKs = 0;
        if (storeMgr.getCompleteDDL())
        {
            actualCandidateKeysByName = new HashMap<>();
        }
        else
        {
            actualCandidateKeysByName = getExistingCandidateKeys(conn);
            numActualCKs = actualCandidateKeysByName.size();
            if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("058204", "" + numActualCKs, this));
            }
        }

        // Auto Create any missing candidate keys
        if (autoCreate)
        {
            dbWasModified = createCandidateKeys(conn, autoCreateErrors, actualCandidateKeysByName);
        }
        else
        {
            //validate only
            Map<String, String> stmtsByCKName = getSQLAddCandidateKeyStatements(actualCandidateKeysByName);
            if (stmtsByCKName.isEmpty())
            {
                if (numActualCKs > 0)
                {
                    if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("058205", "" + numActualCKs,this));
                    }
                }
            }
            else
            {
                // We support existing schemas so don't raise an exception.
                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("058201", this, stmtsByCKName.values()));
            }
        }

        return dbWasModified;
    }

    /**
     * Method to create any Candidate keys on this table in the datastore, and
     * auto create any that are missing where required.
     * 
     * @param conn The JDBC Connection
     * @param autoCreateErrors Errors found during the auto-create process
     * @return Whether the database was modified
     * @throws SQLException Thrown when an error occurs in the JDBC calls
     */
    private boolean createCandidateKeys(Connection conn, Collection autoCreateErrors, Map actualCandidateKeysByName)
    throws SQLException
    {
        Map<String, String> stmtsByCKName = getSQLAddCandidateKeyStatements(actualCandidateKeysByName);
        Statement stmt = conn.createStatement();
        try
        {
            for (String stmtText : stmtsByCKName.values())
            {
                try
                {
                    executeDdlStatement(stmt, stmtText);
                }
                catch (SQLException sqle)
                {
                    if (autoCreateErrors != null)
                    {
                        autoCreateErrors.add(sqle);
                    }
                    else
                    {
                        throw sqle;
                    }
                }
            }
        }
        finally
        {
            stmt.close();
        }
        return !stmtsByCKName.isEmpty();
    }

    /**
     * Method to drop the constraints for the table from the datastore.
     * @param conn The JDBC Connection
     * @throws SQLException Thrown when an error occurs in the JDBC call.
     */
    public void dropConstraints(Connection conn)
    throws SQLException
    {
        assertIsInitialized();

        boolean drop_using_constraint = dba.supportsOption(DatastoreAdapter.ALTER_TABLE_DROP_CONSTRAINT_SYNTAX);
        boolean drop_using_foreign_key = dba.supportsOption(DatastoreAdapter.ALTER_TABLE_DROP_FOREIGN_KEY_CONSTRAINT);
        if (!drop_using_constraint && !drop_using_foreign_key)
        {
            return;
        }

        // There's no need to drop indices; we assume they'll go away quietly when the table is dropped.
        Set<String> fkNames = new HashSet<>();
        StoreSchemaHandler handler = storeMgr.getSchemaHandler();
        RDBMSTableFKInfo fkInfo = (RDBMSTableFKInfo)handler.getSchemaData(conn, RDBMSSchemaHandler.TYPE_FKS, new Object[] {this});
        Iterator iter = fkInfo.getChildren().iterator();
        while (iter.hasNext())
        {
            // JDBC drivers can return null names for foreign keys, so we then skip the DROP CONSTRAINT.
            ForeignKeyInfo fki = (ForeignKeyInfo)iter.next();
            String fkName = (String)fki.getProperty("fk_name");
            if (fkName != null)
            {
                fkNames.add(fkName);
            }
        }
        int numFKs = fkNames.size();
        if (numFKs > 0)
        {
            if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("058102", "" + numFKs, this));
            }
            iter = fkNames.iterator();
            IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
            Statement stmt = conn.createStatement();
            try
            {
                while (iter.hasNext())
                {
                    String constraintName = (String) iter.next();
                    String stmtText = null;
                    if (drop_using_constraint)
                    {
                        stmtText = "ALTER TABLE " + toString() + " DROP CONSTRAINT " + idFactory.getIdentifierInAdapterCase(constraintName);
                    }
                    else
                    {
                        stmtText = "ALTER TABLE " + toString() + " DROP FOREIGN KEY " + idFactory.getIdentifierInAdapterCase(constraintName);
                    }

                    executeDdlStatement(stmt, stmtText);
                }
            }
            finally
            {
                stmt.close();
            }
        }
    }

    /**
     * Accessor for the expected foreign keys for this table in the datastore.
     * Currently only checks the columns for referenced tables (i.e relationships) and returns those.
     * @param clr The ClassLoaderResolver
     * @return List of foreign keys.
     */
    public List<ForeignKey> getExpectedForeignKeys(ClassLoaderResolver clr)
    {
        assertIsInitialized();

        // The following Set is to avoid the duplicate usage of columns that have already been used in conjunction with another column
        Set<Column> colsInFKs = new HashSet<>();
        List<ForeignKey> foreignKeys = new ArrayList<>();
        Iterator i = columns.iterator();
        while (i.hasNext())
        {
            Column col = (Column) i.next();
            if (!colsInFKs.contains(col))
            {
                try
                {
                    DatastoreClass referencedTable = storeMgr.getDatastoreClass(col.getStoredJavaType(), clr);
                    if (referencedTable != null)
                    {
                        for (int j = 0; j < col.getJavaTypeMapping().getNumberOfColumnMappings(); j++)
                        {
                            colsInFKs.add(col.getJavaTypeMapping().getColumnMapping(j).getColumn());
                        }
                        ForeignKey fk = new ForeignKey(col.getJavaTypeMapping(), dba, referencedTable, true);
                        foreignKeys.add(fk);
                    }
                }
                catch (NoTableManagedException e)
                {
                    //expected when no table exists
                }
            }
        }
        return foreignKeys;
    }

    /**
     * Accessor for the expected candidate keys for this table in the datastore.
     * Currently returns an empty list.
     * @return List of candidate keys.
     */
    protected List<CandidateKey> getExpectedCandidateKeys()
    {
        assertIsInitialized();
        return new ArrayList<>();
    }

    /**
     * Accessor for the indices for this table in the datastore.
     * @param clr The ClassLoaderResolver
     * @return Set of indices expected.
     */
    protected Set<Index> getExpectedIndices(ClassLoaderResolver clr)
    {
        assertIsInitialized();

        /*
         * For each foreign key, add to the list an index made up of the "from"
         * column(s) of the key, *unless* those columns also happen to be 
         * equal to the primary key (then they are indexed anyway).
         * Ensure that we have separate indices for foreign key columns 
         * if the primary key is the combination of foreign keys, e.g. in join tables.
         * This greatly decreases deadlock probability e.g. on Oracle.
         */
        Set<Index> indices = new HashSet<>();
        PrimaryKey pk = getPrimaryKey();
        List<ForeignKey> expectedFKs = getExpectedForeignKeys(clr);
        for (ForeignKey fk : expectedFKs)
        {
            if (!pk.getColumnList().equals(fk.getColumnList()))
            {
                indices.add(new Index(fk));
            }
        }

        return indices;
    }

    /**
     * Accessor for the primary keys for this table in the datastore.
     * @param conn The JDBC Connection
     * @return Map of primary keys
     * @throws SQLException Thrown when an error occurs in the JDBC call.
     */
    private Map<DatastoreIdentifier, PrimaryKey> getExistingPrimaryKeys(Connection conn)
    throws SQLException
    {
        Map<DatastoreIdentifier, PrimaryKey> primaryKeysByName = new HashMap<>();
        if (tableExistsInDatastore(conn))
        {
            StoreSchemaHandler handler = storeMgr.getSchemaHandler();
            RDBMSTablePKInfo tablePkInfo = (RDBMSTablePKInfo)handler.getSchemaData(conn, RDBMSSchemaHandler.TYPE_PKS, new Object[] {this});
            IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
            Iterator pkColsIter = tablePkInfo.getChildren().iterator();
            while (pkColsIter.hasNext())
            {
                PrimaryKeyInfo pkInfo = (PrimaryKeyInfo)pkColsIter.next();
                String pkName = (String)pkInfo.getProperty("pk_name");
                DatastoreIdentifier pkIdentifier;

                if (pkName == null)
                {
                    pkIdentifier = idFactory.newPrimaryKeyIdentifier(this);
                }
                else
                {
                    pkIdentifier = idFactory.newIdentifier(IdentifierType.COLUMN, pkName);
                }
    
                PrimaryKey pk = primaryKeysByName.get(pkIdentifier);
                if (pk == null)
                {
                    pk = new PrimaryKey(this);
                    pk.setName(pkIdentifier.getName());
                    primaryKeysByName.put(pkIdentifier, pk);
                }
    
                int keySeq = (((Short)pkInfo.getProperty("key_seq")).shortValue()) - 1;
                String colName = (String)pkInfo.getProperty("column_name");
                DatastoreIdentifier colIdentifier = idFactory.newIdentifier(IdentifierType.COLUMN, colName);
                Column col = columnsByIdentifier.get(colIdentifier);
    
                if (col == null)
                {
                    throw new UnexpectedColumnException(this.toString(), colIdentifier.getName(), this.getSchemaName(), this.getCatalogName());
                }
                pk.setColumn(keySeq, col);
            }
        }
        return primaryKeysByName;
    }

    /**
     * Accessor for the foreign keys for this table.
     * @param conn The JDBC Connection
     * @return Map of foreign keys
     * @throws SQLException Thrown when an error occurs in the JDBC call.
     */
    private Map<DatastoreIdentifier, ForeignKey> getExistingForeignKeys(Connection conn)
    throws SQLException
    {
        Map<DatastoreIdentifier, ForeignKey> foreignKeysByName = new HashMap<>();
        if (tableExistsInDatastore(conn))
        {
            StoreSchemaHandler handler = storeMgr.getSchemaHandler();
            IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
            RDBMSTableFKInfo tableFkInfo = (RDBMSTableFKInfo)handler.getSchemaData(conn, RDBMSSchemaHandler.TYPE_FKS, new Object[] {this});
            Iterator fksIter = tableFkInfo.getChildren().iterator();
            while (fksIter.hasNext())
            {
                ForeignKeyInfo fkInfo = (ForeignKeyInfo)fksIter.next();
                DatastoreIdentifier fkIdentifier;
                String fkName = (String)fkInfo.getProperty("fk_name");
                if (fkName == null)
                {
                    fkIdentifier = idFactory.newForeignKeyIdentifier(this, foreignKeysByName.size());
                }
                else
                {
                    fkIdentifier = idFactory.newIdentifier(IdentifierType.FOREIGN_KEY, fkName);
                }
    
                short deferrability = ((Short)fkInfo.getProperty("deferrability")).shortValue();
                boolean initiallyDeferred = deferrability == DatabaseMetaData.importedKeyInitiallyDeferred;
                ForeignKey fk = foreignKeysByName.get(fkIdentifier);
                if (fk == null)
                {
                    fk = new ForeignKey(dba, initiallyDeferred);
                    fk.setName(fkIdentifier.getName());
                    foreignKeysByName.put(fkIdentifier, fk);
                }

                // Find the referenced table from the provided name
                String pkTableName = (String)fkInfo.getProperty("pk_table_name");
                DatastoreIdentifier refTableId = idFactory.newTableIdentifier(pkTableName);
                DatastoreClass refTable = storeMgr.getDatastoreClass(refTableId);
                if (refTable == null)
                {
                    // Try with same catalog/schema as this table since some JDBC don't provide this info
                    if (getSchemaName() != null)
                    {
                        refTableId.setSchemaName(getSchemaName());
                    }
                    if (getCatalogName() != null)
                    {
                        refTableId.setCatalogName(getCatalogName());
                    }
                    refTable = storeMgr.getDatastoreClass(refTableId);
                }

                if (refTable != null)
                {
                    String fkColumnName = (String)fkInfo.getProperty("fk_column_name");
                    String pkColumnName = (String)fkInfo.getProperty("pk_column_name");
                    DatastoreIdentifier colName = idFactory.newIdentifier(IdentifierType.COLUMN, fkColumnName);
                    DatastoreIdentifier refColName = idFactory.newIdentifier(IdentifierType.COLUMN, pkColumnName);
                    Column col = columnsByIdentifier.get(colName);
                    Column refCol = refTable.getColumn(refColName);
                    if (col != null && refCol != null)
                    {
                        fk.addColumn(col, refCol);
                    }
                    else
                    {
                        //TODO throw exception?
                    }
                }
                else
                {
                    NucleusLogger.DATASTORE_SCHEMA.warn("Retrieved ForeignKey from datastore for table=" + toString() + " referencing table " + pkTableName + 
                        " but not found internally. Is there some catalog/schema or quoting causing problems?");
                }
            }
        }
        return foreignKeysByName;
    }

    /**
     * Accessor for the candidate keys for this table.
     * @param conn The JDBC Connection
     * @return Map of candidate keys
     * @throws SQLException Thrown when an error occurs in the JDBC call.
     */
    private Map<DatastoreIdentifier, CandidateKey> getExistingCandidateKeys(Connection conn)
    throws SQLException
    {
        Map<DatastoreIdentifier, CandidateKey> candidateKeysByName = new HashMap<>();

        if (tableExistsInDatastore(conn))
        {
            StoreSchemaHandler handler = storeMgr.getSchemaHandler();
            RDBMSTableIndexInfo tableIndexInfo = (RDBMSTableIndexInfo)handler.getSchemaData(conn, RDBMSSchemaHandler.TYPE_INDICES, new Object[] {this});
            IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
            Iterator indexIter = tableIndexInfo.getChildren().iterator();
            while (indexIter.hasNext())
            {
                IndexInfo indexInfo = (IndexInfo)indexIter.next();
                boolean isUnique = !((Boolean)indexInfo.getProperty("non_unique")).booleanValue();
                if (isUnique)
                {
                    // No idea of why this was being used, so commented out (H2 v2 fails if enabled)
//                    short idxType = ((Short)indexInfo.getProperty("type")).shortValue();
//                    if (idxType == DatabaseMetaData.tableIndexStatistic)
//                    {
//                        // Ignore
//                        continue;
//                    }

                    // Only utilise unique indexes    
                    String keyName = (String)indexInfo.getProperty("index_name");
                    DatastoreIdentifier idxName = idFactory.newIdentifier(IdentifierType.CANDIDATE_KEY, keyName);
                    CandidateKey key = candidateKeysByName.get(idxName);
                    if (key == null)
                    {
                        key = new CandidateKey(this, null);
                        key.setName(keyName);
                        candidateKeysByName.put(idxName, key);
                    }
    
                    // Set the column
                    int colSeq = ((Short)indexInfo.getProperty("ordinal_position")).shortValue() - 1;
                    DatastoreIdentifier colName = idFactory.newIdentifier(IdentifierType.COLUMN, (String)indexInfo.getProperty("column_name"));
                    Column col = columnsByIdentifier.get(colName);
                    if (col != null)
                    {
                        key.setColumn(colSeq, col);
                    }
                }
            }
        }
        return candidateKeysByName;
    }

    
    /**
     * Accessor for indices on the actual table.
     * @param conn The JDBC Connection
     * @return Map of indices (keyed by the index name)
     * @throws SQLException Thrown when an error occurs in the JDBC call.
     */
    private Map<DatastoreIdentifier, Index> getExistingIndices(Connection conn)
    throws SQLException
    {
        Map<DatastoreIdentifier, Index> indicesByName = new HashMap<>();

        if (tableExistsInDatastore(conn))
        {
            StoreSchemaHandler handler = storeMgr.getSchemaHandler();
            RDBMSTableIndexInfo tableIndexInfo = (RDBMSTableIndexInfo)handler.getSchemaData(conn, RDBMSSchemaHandler.TYPE_INDICES, new Object[] {this});
            IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
            Iterator indexIter = tableIndexInfo.getChildren().iterator();
            while (indexIter.hasNext())
            {
                IndexInfo indexInfo = (IndexInfo)indexIter.next();

                if (!dba.supportsOption(DatastoreAdapter.INCLUDE_TABLE_INDEX_STATISTICS))
                {
                    short idxType = ((Short)indexInfo.getProperty("type")).shortValue();
                    if (idxType == DatabaseMetaData.tableIndexStatistic)
                    {
                        // Ignore
                        continue;
                    }
                }
 
                String indexName = (String)indexInfo.getProperty("index_name");
                DatastoreIdentifier indexIdentifier = idFactory.newIdentifier(IdentifierType.CANDIDATE_KEY, indexName);
                Index idx = indicesByName.get(indexIdentifier);
                if (idx == null)
                {
                    boolean isUnique = !((Boolean)indexInfo.getProperty("non_unique")).booleanValue();
                    idx = new Index(this, isUnique, null);
                    idx.setName(indexName);
                    indicesByName.put(indexIdentifier, idx);
                }
    
                // Set the column
                int colSeq = ((Short)indexInfo.getProperty("ordinal_position")).shortValue() - 1;
                DatastoreIdentifier colName = idFactory.newIdentifier(IdentifierType.COLUMN, (String)indexInfo.getProperty("column_name"));
                Column col = columnsByIdentifier.get(colName);
                if (col != null)
                {
                    idx.setColumn(colSeq, col);
                }
            }
        }
        return indicesByName;
    }

    /**
     * Accessor for the SQL CREATE statements for this table.
     * @param props Properties for controlling the table creation
     * @return List of statements.
     */
    protected List<String> getSQLCreateStatements(Properties props)
    {
        assertIsInitialized();

        Column[] cols = null;

        // Pass 1 : populate positions defined in metadata as vendor extension "index"
        Iterator<org.datanucleus.store.schema.table.Column> iter = columns.iterator();
        while (iter.hasNext())
        {
            Column col = (Column)iter.next();
            ColumnMetaData colmd = col.getColumnMetaData();
            Integer colPos = (colmd != null ? colmd.getPosition() : null);
            if (colPos != null)
            {
                int index = colPos.intValue();
                if (index < columns.size() && index >= 0)
                {
                    if (cols == null)
                    {
                        cols = new Column[columns.size()];
                    }
                    if (cols[index] != null)
                    {
                        throw new NucleusUserException("Column index " + index + " has been specified multiple times : " + cols[index] + " and " + col);
                    }
                    cols[index] = col;
                }
            }
        }

        // Pass 2 : fill in spaces for columns with undefined positions
        if (cols != null)
        {
            iter = columns.iterator();
            while (iter.hasNext())
            {
                Column col = (Column)iter.next();
                ColumnMetaData colmd = col.getColumnMetaData();
                Integer colPos = (colmd != null ? colmd.getPosition() : null);
                if (colPos == null)
                {
                    // No index set for this column, so assign to next free position
                    for (int i=0;i<cols.length;i++)
                    {
                        if (cols[i] == null)
                        {
                            cols[i] = col;
                        }
                    }
                }
            }
        }
        else
        {
            cols = columns.toArray(new Column[columns.size()]);
        }

        List<String> stmts = new ArrayList<>();
        stmts.add(dba.getCreateTableStatement(this, cols, props, storeMgr.getIdentifierFactory()));

        PrimaryKey pk = getPrimaryKey();
        if (pk.getNumberOfColumns() > 0)
        {
            // Some databases define the primary key on the create table
            // statement so we don't have a Statement for the primary key here.
            String pkStmt = dba.getAddPrimaryKeyStatement(pk, storeMgr.getIdentifierFactory());
            if (pkStmt != null)
            {
                stmts.add(pkStmt);
            }
        }

        return stmts;
    }

    /**
     * Get SQL statements to add expected Foreign Keys that are not yet at the table.
     * If the returned Map is empty, the current FK setup is correct.
     * @param actualForeignKeysByName Actual Map of foreign keys
     * @param clr The ClassLoaderResolver
     * @return a Map with the SQL statements
     */
    protected Map<String, String> getSQLAddFKStatements(Map actualForeignKeysByName, ClassLoaderResolver clr)
    {
        assertIsInitialized();

        Map<String, String> stmtsByFKName = new HashMap<>();
        List<ForeignKey> expectedForeignKeys = getExpectedForeignKeys(clr);
        int n = 1;
        IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
        for (ForeignKey fk : expectedForeignKeys)
        {
            if (!actualForeignKeysByName.containsValue(fk))
            {
                // If no name assigned, make one up
                if (fk.getName() == null)
                {
                    // Use the ForeignKeyIdentifier to generate the name
                    DatastoreIdentifier fkName;
                    do
                    {
                        fkName = idFactory.newForeignKeyIdentifier(this, n++);
                    }
                    while (actualForeignKeysByName.containsKey(fkName));
                    fk.setName(fkName.getName());
                }
                String stmtText = dba.getAddForeignKeyStatement(fk, idFactory);
                if (stmtText != null)
                {
                    stmtsByFKName.put(fk.getName(), stmtText);
                }
            }
        }

        return stmtsByFKName;
    }

    /**
     * Get SQL statements to add expected Candidate Keys that are not yet on the
     * table. If the returned Map is empty, the current Candidate Key setup is correct.
     * @param actualCandidateKeysByName Actual Map of candidate keys
     * @return a Map with the SQL statements
     */
    protected Map<String, String> getSQLAddCandidateKeyStatements(Map actualCandidateKeysByName)
    {
        assertIsInitialized();

        Map<String, String> stmtsByCKName = new HashMap<>();
        List<CandidateKey> expectedCandidateKeys = getExpectedCandidateKeys();
        int n = 1;
        IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
        for (CandidateKey ck : expectedCandidateKeys)
        {
            if (!actualCandidateKeysByName.containsValue(ck))
            {
                // If no name assigned, make one up
                if (ck.getName() == null)
                {
                    // Use the CandidateKeyIdentifier to generate the name
                    DatastoreIdentifier ckName;
                    do
                    {
                        ckName = idFactory.newCandidateKeyIdentifier(this, n++);
                    }
                    while (actualCandidateKeysByName.containsKey(ckName));
                    ck.setName(ckName.getName());
                }
                String stmtText = dba.getAddCandidateKeyStatement(ck, idFactory);
                if (stmtText != null)
                {
                    stmtsByCKName.put(ck.getName(), stmtText);
                }
            }
        }

        return stmtsByCKName;
    }

    /**
     * Utility to check if an index is necessary.
     * @param requiredIdx The index
     * @param actualIndices The actual indexes
     * @return Whether the index is needed (i.e not present in the actual indexes)
     */
    private boolean isIndexReallyNeeded(Index requiredIdx, Collection actualIndices)
    {
        Iterator i = actualIndices.iterator();
        if (requiredIdx.getName() != null)
        {
            // Compare the index name since it is defined
            IdentifierFactory idFactory = requiredIdx.getTable().getStoreManager().getIdentifierFactory();
            String reqdName = idFactory.getIdentifierInAdapterCase(requiredIdx.getName()); // Allow for user input in incorrect case
            while (i.hasNext())
            {
                Index actualIdx = (Index) i.next();
                String actualName = idFactory.getIdentifierInAdapterCase(actualIdx.getName()); // Allow for DB returning no quotes
                if (actualName.equals(reqdName) && actualIdx.getTable().getIdentifier().toString().equals(requiredIdx.getTable().getIdentifier().toString()))
                {
                    // There already is an index of that name for the same table in the actual list so not needed
                    return false;
                }
            }
        }
        else
        {
            // Compare against the index table and columns since we have no index name yet
            while (i.hasNext())
            {
                Index actualIdx = (Index) i.next();
                if (actualIdx.toString().equals(requiredIdx.toString()) &&
                    actualIdx.getTable().getIdentifier().toString().equals(requiredIdx.getTable().getIdentifier().toString()))
                {
                    // There already is an index of that name for the same table in the actual list so not needed
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Accessor for the CREATE INDEX statements for this table.
     * @param actualIndicesByName Map of actual indexes
     * @param clr The ClassLoaderResolver
     * @return Map of statements
     */
    protected Map<String, String> getSQLCreateIndexStatements(Map actualIndicesByName, ClassLoaderResolver clr)
    {
        assertIsInitialized();
        Map<String, String> stmtsByIdxName = new HashMap<>();
        Set<Index> expectedIndices = getExpectedIndices(clr);

        int n = 1;
        IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
        for (Index idx : expectedIndices)
        {
            if (isIndexReallyNeeded(idx, actualIndicesByName.values()))
            {
                // If no name assigned, make one up
                if (idx.getName() == null)
                {
                    // Use IndexIdentifier to generate the name.
                    DatastoreIdentifier idxName;
                    do
                    {
                        idxName = idFactory.newIndexIdentifier(this, idx.getUnique(), n++);
                        idx.setName(idxName.getName());
                    }
                    while (actualIndicesByName.containsKey(idxName));
                }

                String stmtText = dba.getCreateIndexStatement(idx, idFactory);
                stmtsByIdxName.put(idx.getName(), stmtText);
            }
        }
        return stmtsByIdxName;
    }

    /**
     * Accessor for the DROP statements for this table.
     * @return List of statements
     */
    protected List<String> getSQLDropStatements()
    {
        assertIsInitialized();

        List<String> stmts = new ArrayList<>();
        stmts.add(dba.getDropTableStatement(this));
        return stmts;
    }

    /**
     * Convenience logging method to output the mapping information for an element, key, value field.
     * @param memberName Name of the member
     * @param mapping The mapping
     */
    protected void logMapping(String memberName, JavaTypeMapping mapping)
    {
        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            // Provide field->column mapping debug message
            StringBuilder columnsStr = new StringBuilder();
            for (int i=0;i<mapping.getNumberOfColumnMappings();i++)
            {
                if (i > 0)
                {
                    columnsStr.append(",");
                }
                columnsStr.append(mapping.getColumnMapping(i).getColumn());
            }
            if (mapping.getNumberOfColumnMappings() == 0)
            {
                columnsStr.append("[none]");
            }
            StringBuilder columnMappingTypes = new StringBuilder();
            for (int i=0;i<mapping.getNumberOfColumnMappings();i++)
            {
                if (i > 0)
                {
                    columnMappingTypes.append(',');
                }
                columnMappingTypes.append(mapping.getColumnMapping(i).getClass().getName());
            }
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("057010", memberName, columnsStr.toString(), mapping.getClass().getName(), columnMappingTypes.toString()));
        }
    }
}