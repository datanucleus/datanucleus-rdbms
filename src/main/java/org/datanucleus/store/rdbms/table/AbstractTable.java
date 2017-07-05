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
2003 Andy Jefferson - added localiser
2003 Andy Jefferson - added output of logging when an exception is thrown
2004 Andy Jefferson - allowed updateable catalogName, schemaName
2004 Andy Jefferson - rewritten to remove levels of inheritance
2004 Andy Jefferson - added capability to add columns after initialisation.
2006 Andy Jefferson - removed catalog/schema flags and use tableIdentifier instead
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.table;

import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.store.rdbms.exceptions.DuplicateColumnException;
import org.datanucleus.store.rdbms.JDBCUtils;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.schema.RDBMSSchemaHandler;
import org.datanucleus.store.rdbms.schema.RDBMSSchemaInfo;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Abstract implementation of a table in the datastore.
 * The table exists in various states.
 * After initialisation it can be created in the datastore by calling <B>create</B>. 
 * At any point after initialisation it can be modified, but only by addition of columns.
 * The table can be dropped from the datastore by calling <B>drop</B>.
 */
public abstract class AbstractTable implements Table
{
    /** Table object has just been created. */
    static final protected int TABLE_STATE_NEW = 0;

    /** Table object is created and PK initialised. */
    static final protected int TABLE_STATE_PK_INITIALIZED = 1;

    /** Table object has been initialised. */
    static final protected int TABLE_STATE_INITIALIZED = 2;

    /** Table object has been initialized but has had structural modifications since. */
    static final protected int TABLE_STATE_INITIALIZED_MODIFIED = 3;

    /** Table object has been validated. */
    static final protected int TABLE_STATE_VALIDATED = 4;

    /** Manager for this table. */
    protected final RDBMSStoreManager storeMgr;

    /** Database Adapter being used. */
    protected final DatastoreAdapter dba;

    /** Identifier name for the table. Includes the catalog/schema internally (if defined by the user). */
    protected final DatastoreIdentifier identifier;

    /** State of the table */
    protected int state = TABLE_STATE_NEW;

    /** Columns for this table. */
    protected List<org.datanucleus.store.schema.table.Column> columns = new ArrayList<>();

    /** Index to the columns, keyed by name identifier. TODO Key this by the column name, not its identifier. */
    protected Map<DatastoreIdentifier, Column> columnsByIdentifier = new HashMap<>();

    /** Fully qualified name of this table. */
    private String fullyQualifiedName;

    //----------------------- convenience fields to improve performance ----------------------//
    /** compute hashCode in advance to improve performance **/
    private final int hashCode;
    
    /**
     * Constructor taking the table name and the RDBMSManager managing this table.
     * @param identifier Name of the table
     * @param storeMgr The RDBMS Manager
     */
    public AbstractTable(DatastoreIdentifier identifier, RDBMSStoreManager storeMgr)
    {
        this.storeMgr = storeMgr;
        this.dba = storeMgr.getDatastoreAdapter();
        this.identifier = identifier;
        this.hashCode = identifier.hashCode() ^ storeMgr.hashCode();
    }

    /**
     * Accessor for whether the table is initialised.
     * @return Whether it is initialised
     */
    public boolean isInitialized()
    {
        // All of the states from initialised onwards imply that it has (at some time) been initialised
        return state >= TABLE_STATE_INITIALIZED;
    }

    /**
     * Accessor for whether the primary key of the table is initialised.
     * @return Whether the primary key of the table is initialised
     */
    public boolean isPKInitialized()
    {
        return state >= TABLE_STATE_PK_INITIALIZED;
    }

    /**
     * Accessor for whether the table is validated.
     * @return Whether it is validated.
     */
    public boolean isValidated()
    {
        return state == TABLE_STATE_VALIDATED;
    }
    
    /**
     * Accessor for whether the table has been modified since initialisation.
     * @return Whether it is modified since initialisation.
     */
    public boolean isInitializedModified()
    {
        return state == TABLE_STATE_INITIALIZED_MODIFIED;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Table#getStoreManager()
     */
    @Override
    public RDBMSStoreManager getStoreManager()
    {
        return storeMgr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Table#getName()
     */
    @Override
    public String getName()
    {
        return identifier.toString();
    }

    /**
     * Accessor for the Catalog Name.
     * This will be part of the fully qualified name IF the user has specified
     * the catalog in the MetaData, OR if they have specified the catalog in the PMF.
     * @return Catalog Name
     **/
    public String getCatalogName()
    {
        return identifier.getCatalogName();
    }

    /**
     * Accessor for the Schema Name.
     * This will be part of the fully qualified name IF the user has specified
     * the schema in the MetaData, OR if they have specified the schema in the PMF.
     * @return Schema Name
     **/
    public String getSchemaName()
    {
        return identifier.getSchemaName();
    }

    /**
     * Accessor for the SQL identifier (the table name).
     * @return The name
     **/
    public DatastoreIdentifier getIdentifier()
    {
        return identifier;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Table#getClassMetaData()
     */
    @Override
    public AbstractClassMetaData getClassMetaData()
    {
        // Will be overridden by tables relating to classes
        return null;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Table#getNumberOfColumns()
     */
    @Override
    public int getNumberOfColumns()
    {
        return columns.size();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Table#getColumns()
     */
    @Override
    public List<org.datanucleus.store.schema.table.Column> getColumns()
    {
        return columns;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Table#getColumnForPosition(int)
     */
    @Override
    public org.datanucleus.store.schema.table.Column getColumnForPosition(int pos)
    {
        throw new UnsupportedOperationException("Not supported on this table");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Table#getSurrogateColumn(org.datanucleus.store.schema.table.SurrogateColumnType)
     */
    @Override
    public Column getSurrogateColumn(SurrogateColumnType colType)
    {
        throw new UnsupportedOperationException("Not supported on this table");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Table#getColumnForName(java.lang.String)
     */
    @Override
    public org.datanucleus.store.schema.table.Column getColumnForName(String name)
    {
        throw new UnsupportedOperationException("Not supported on this table");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Table#getMemberColumnMappingForMember(org.datanucleus.metadata.AbstractMemberMetaData)
     */
    @Override
    public MemberColumnMapping getMemberColumnMappingForMember(AbstractMemberMetaData mmd)
    {
        throw new UnsupportedOperationException("Not supported on this table");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Table#getMemberColumnMappingForEmbeddedMember(java.util.List)
     */
    @Override
    public MemberColumnMapping getMemberColumnMappingForEmbeddedMember(List<AbstractMemberMetaData> mmds)
    {
        throw new UnsupportedOperationException("Not supported on this table");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.Table#getMemberColumnMappings()
     */
    @Override
    public Set<MemberColumnMapping> getMemberColumnMappings()
    {
        throw new UnsupportedOperationException("Not supported on this table");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.table.Table#getSurrogateMapping(org.datanucleus.store.schema.table.SurrogateColumnType, boolean)
     */
    @Override
    public JavaTypeMapping getSurrogateMapping(SurrogateColumnType colType, boolean allowSuperclasses)
    {
        if (colType == SurrogateColumnType.DISCRIMINATOR)
        {
            return null;
        }
        else if (colType == SurrogateColumnType.MULTITENANCY)
        {
            return null;
        }
        else if (colType == SurrogateColumnType.VERSION)
        {
            return null;
        }
        else if (colType == SurrogateColumnType.DATASTORE_ID)
        {
            return null;
        }
        else if (colType == SurrogateColumnType.SOFTDELETE)
        {
            return null;
        }
        // TODO Support other types
        return null;
    }

    /**
     * Accessor for Version MetaData
     * @return Returns the Version MetaData.
     */
    public VersionMetaData getVersionMetaData()
    {
        return null;
    }  

    /**
     * Accessor for Discriminator MetaData
     * @return Returns the Discriminator MetaData.
     */
    public DiscriminatorMetaData getDiscriminatorMetaData()
    {
        return null;
    }

    /**
     * Creates a new column in the table.
     * Will add the new Column and return it. If the new column clashes in name with an existing column of the
     * required name will throw a DuplicateColumnNameException except when :-
     * <ul>
     * <li>The 2 columns are for same named fields in the class or its subclasses with the subclass(es) using 
     * "superclass-table" inheritance strategy. One of the columns has to come from a subclass - cant have
     * both from the same class.</li>
     * </ul>
     * @param storedJavaType the java type of the datastore field
     * @param name the SQL identifier for the column to be added
     * @param mapping the mapping for the column to be added
     * @param colmd ColumnMetaData for the column to be added to the table
     * @return the new Column
     * @throws DuplicateColumnException if a column already exists with same name and not a supported situation.
     */
    public synchronized Column addColumn(String storedJavaType, DatastoreIdentifier name, JavaTypeMapping mapping, ColumnMetaData colmd)
    {
        // TODO If already initialized and this is called we should check if exists in current representation
        // then check if exists in datastore, and then create it if necessary

        boolean duplicateName = false;
        if (hasColumnName(name))
        {
            duplicateName = true;
        }

        // Create the column
        Column col = new ColumnImpl(this, storedJavaType, name, colmd);

        if (duplicateName && colmd != null)
        {
            // Verify if a duplicate column is valid. A duplicate column name is (currently) valid when :-
            // 1. subclasses defining the duplicated column are using "super class table" strategy
            //
            // Find the MetaData for the existing column
            Column existingCol = columnsByIdentifier.get(name);
            MetaData md = existingCol.getColumnMetaData().getParent();
            while (!(md instanceof AbstractClassMetaData))
            {
                if (md == null)
                {
                    // ColumnMetaData for existing column has no parent class somehow!
                    throw new NucleusUserException(Localiser.msg("057043", name.getName(), getDatastoreIdentifierFullyQualified()));
                }
                md = md.getParent();
            }

            // Find the MetaData for the column to be added
            MetaData dupMd = colmd.getParent();
            while (!(dupMd instanceof AbstractClassMetaData))
            {
                dupMd = dupMd.getParent();
                if (dupMd == null)
                {
                    // ColumnMetaData for required column has no parent class somehow!
                    throw new NucleusUserException(Localiser.msg("057044", name.getName(), getDatastoreIdentifierFullyQualified(), colmd.toString()));
                }
            }

            boolean reuseColumns = storeMgr.getBooleanProperty(RDBMSPropertyNames.PROPERTY_RDBMS_ALLOW_COLUMN_REUSE);
            if (!reuseColumns)
            {
                if (((AbstractClassMetaData)md).getFullClassName().equals(((AbstractClassMetaData)dupMd).getFullClassName()))
                {
                    // compare the current column defining class and the duplicated column defining class. if the same class, 
                    // we raise an exception when within one class it is defined a column twice
                    // in some cases it could still be possible to have these duplicated columns, but does not make too
                    // much sense in most of the cases. (this whole block of duplicated column check, could be optional, like a pmf property)
                    throw new DuplicateColumnException(this.toString(), existingCol, col);
                }

                // Make sure the field JavaTypeMappings are compatible
                if (mapping != null &&
                    !mapping.getClass().isAssignableFrom(existingCol.getJavaTypeMapping().getClass()) &&
                    !existingCol.getJavaTypeMapping().getClass().isAssignableFrom(mapping.getClass())) 
                {
                    // the mapping class must be the same (not really required, but to avoid user mistakes)
                    throw new DuplicateColumnException(this.toString(), existingCol, col);
                }
            }
            else
            {
                if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                {
                    if (mapping != null && mapping.getMemberMetaData() != null)
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug("Column " + existingCol + 
                            " has already been defined but needing to reuse it for " + mapping.getMemberMetaData().getFullFieldName());
                    }
                    else
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug("Column " + existingCol + " has already been defined but needing to reuse it");
                    }
                }
            }

            // Make sure the field java types are compatible
            Class fieldStoredJavaTypeClass = null;
            Class existingColStoredJavaTypeClass = null;
            try
            {
                ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
                fieldStoredJavaTypeClass = clr.classForName(storedJavaType);
                existingColStoredJavaTypeClass = clr.classForName(col.getStoredJavaType());
            }
            catch (RuntimeException cnfe)
            {
                // Do nothing
            }

            if (fieldStoredJavaTypeClass != null && existingColStoredJavaTypeClass != null &&
                !fieldStoredJavaTypeClass.isAssignableFrom(existingColStoredJavaTypeClass) &&
                !existingColStoredJavaTypeClass.isAssignableFrom(fieldStoredJavaTypeClass))
            {
                // the stored java type must be the same (not really required, but to avoid user mistakes)
                throw new DuplicateColumnException(this.toString(), existingCol, col);
            }
        }

        if (!duplicateName)
        {
            // Only add to our internal list when it is not a dup (since it would try to create a table with the same col twice)
            addColumnInternal(col);
        }

        if (isInitialized())
        {
            // Set state to modified
            state = TABLE_STATE_INITIALIZED_MODIFIED;
        }

        return col;
    }

	/**
	 * Checks if there is a column for the identifier
	 * @param identifier the identifier of the column
	 * @return true if the column exists for the identifier
	 */
	public boolean hasColumn(DatastoreIdentifier identifier)
	{
        return (hasColumnName(identifier));
	}

	/**
	 * Accessor for the Datastore field with the specified identifier.
	 * Returns null if has no column of this name.
	 * @param identifier The name of the column
	 * @return The column
	 */
	public Column getColumn(DatastoreIdentifier identifier)
	{
	    return columnsByIdentifier.get(identifier);
	}

    /**
     * Method to create this table.
     * @param conn Connection to the datastore.
     * @return true if the table was created
     * @throws SQLException Thrown if an error occurs creating the table.
     */
    public boolean create(Connection conn)
    throws SQLException
    {
        assertIsInitialized();

        if (storeMgr.getSchemaHandler().isAutoCreateDatabase())
        {
            if (identifier.getSchemaName() != null || identifier.getCatalogName() != null)
            {
                // Make sure the specified catalog/schema exists
                RDBMSSchemaInfo info = (RDBMSSchemaInfo)storeMgr.getSchemaHandler().getSchemaData(conn, RDBMSSchemaHandler.TYPE_SCHEMA, new Object[] {getSchemaName(), getCatalogName()});
                NucleusLogger.DATASTORE_SCHEMA.debug("Check of existence of catalog=" + identifier.getCatalogName() + " schema=" + identifier.getSchemaName() + " returned " + (info != null));
                if (info == null)
                {
                    storeMgr.getSchemaHandler().createDatabase(identifier.getCatalogName(), identifier.getSchemaName(), null, conn);
                }
            }
        }

        List<String> createStmts = getSQLCreateStatements(null);
        executeDdlStatementList(createStmts, conn);
        return !createStmts.isEmpty();
    }

    /**
     * Method to drop this table.
     * @param conn Connection to the datastore.
     * @throws SQLException Thrown if an error occurs dropping the table.
     */
    public void drop(Connection conn)
    throws SQLException
    {
        assertIsInitialized();

        executeDdlStatementList(getSQLDropStatements(), conn);
    }

    /** Cache what we learned in a call to exists() */
    protected Boolean existsInDatastore = null;
    
    /**
     * Method to check the existence of the table/view, optionally auto creating it
     * where required. If it doesn't exist and auto creation isn't specified this
     * throws a MissingTableException.
     * @param conn The JDBC Connection
     * @param auto_create Whether to auto create the table if not existing
     * @return Whether the table was added
     * @throws SQLException Thrown when an error occurs in the JDBC calls
     */
    public boolean exists(Connection conn, boolean auto_create) 
    throws SQLException
    {
        assertIsInitialized();

        // Get column info for table to check for existence
        // Note that unless the schema is specified here with Oracle then it can often come back 
        // with crap like "SYNONYM" as the table type (10.2.0.4 driver).
        String type = ((RDBMSSchemaHandler)storeMgr.getSchemaHandler()).getTableType(conn, this);
        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            if (type == null)
            {
                NucleusLogger.DATASTORE_SCHEMA.debug("Check of existence of " + this + " returned no table");
            }
            else
            {
                NucleusLogger.DATASTORE_SCHEMA.debug("Check of existence of " + this + " returned table type of " + type);
            }
        }
        if (type == null || (allowDDLOutput() && storeMgr.getDdlWriter() != null && storeMgr.getCompleteDDL()))
        {
            // Table is missing, or we're running SchemaTool with a DDL file
            if (!auto_create)
            {
                existsInDatastore = Boolean.FALSE;
                throw new MissingTableException(getCatalogName(),getSchemaName(),this.toString());
            }

            boolean created = create(conn);

            // TODO Do we need this check on tableType ?
            String tableType = ((RDBMSSchemaHandler)storeMgr.getSchemaHandler()).getTableType(conn, this);
            if (storeMgr.getDdlWriter() == null || (tableType != null))
            {
                // table either already existed or we really just created it
                existsInDatastore = Boolean.TRUE;
            }

            state = TABLE_STATE_VALIDATED;
            return created;
        }

        // table already existed
        existsInDatastore = Boolean.TRUE;

        return false;
    } 

    /**
     * Equality operator.
     * @param obj The object to compare against
     * @return Whether the objects are equal
     */
    public final boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (!(obj instanceof AbstractTable))
        {
            return false;
        }

        AbstractTable t = (AbstractTable)obj;
        return getClass().equals(t.getClass()) && identifier.equals(t.identifier) && storeMgr.equals(t.storeMgr);
    }

    /**
     * Accessor for the hash code of this table.
     * @return The hash code.
     */
    public final int hashCode()
    {
        return hashCode;
    }

    /**
     * Method to return a string version of this table. This name is the
     * fully-qualified name of the table,including catalog/schema names, where
     * these are appropriate. They are included where the user has either specified
     * the catalog/schema for the PMF, or in the MetaData. They are also only included
     * where the datastore adapter supports their use.
     * @return String name of the table (catalog.schema.table)
     */
    public final String toString()
    {
        if (fullyQualifiedName != null)
        {
            return fullyQualifiedName;
        }

        fullyQualifiedName = identifier.getFullyQualifiedName(false);

        return fullyQualifiedName;
    }

    /**
     * Method that operates like toString except it returns a fully-qualified name that will always
     * be fully-qualified even when the user hasnt specified the catalog/schema in PMF or MetaData.
     * That is, it will add on any auto-calculated catalog/schema for the datastore.
     * Note that this will never include any quoting strings required for insert/select etc.
     * @return The fully qualified name
     */
    public DatastoreIdentifier getDatastoreIdentifierFullyQualified()
    {
        String catalog = identifier.getCatalogName();
        if (catalog != null)
        {
            // Remove any identifier quotes
            catalog = catalog.replace(dba.getIdentifierQuoteString(), "");
        }

        String schema = identifier.getSchemaName();
        if (schema != null)
        {
            // Remove any identifier quotes
            schema = schema.replace(dba.getIdentifierQuoteString(), "");
        }

        String table = identifier.getName();
        table = table.replace(dba.getIdentifierQuoteString(), "");

        DatastoreIdentifier di = storeMgr.getIdentifierFactory().newTableIdentifier(table);
        di.setCatalogName(catalog);
        di.setSchemaName(schema);
        return di;
    }

    // -------------------------------- Internal Implementation ---------------------------
    
    /**
     * Utility method to add a column to the internal representation
     * @param col The column
     */
    protected synchronized void addColumnInternal(Column col)
    {
        DatastoreIdentifier colName = col.getIdentifier();

        columns.add(col);
        columnsByIdentifier.put(colName, col);

        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("057034", col));
        }
    }
    
    /**
     * Utility to return if a column of this name exists.
     * @param colName The column name
     * @return Whether the column of this name exists
     */
    protected boolean hasColumnName(DatastoreIdentifier colName)
    {
        return columnsByIdentifier.get(colName) != null;
    }

    /**
     * Accessor for the SQL create statements.
     * @param props Properties controlling the table creation
     * @return The SQL Create statements
     */
    protected abstract List<String> getSQLCreateStatements(Properties props);

    /**
     * Accessor for the SQL drop statements.
     * @return The SQL Drop statements
     */
    protected abstract List<String> getSQLDropStatements();

    protected void assertIsPKUninitialized()
    {
        if (isPKInitialized())
        {
            throw new IllegalStateException(Localiser.msg("057000",this));
        }
    }

    protected void assertIsUninitialized()
    {
        if (isInitialized())
        {
            throw new IllegalStateException(Localiser.msg("057000",this));
        }
    }

    protected void assertIsInitialized()
    {
        if (!isInitialized())
        {
            throw new IllegalStateException(Localiser.msg("057001",this));
        }
    }

    protected void assertIsInitializedModified()
    {
        if (!isInitializedModified())
        {
            throw new IllegalStateException(Localiser.msg("RDBMS.Table.UnmodifiedError",this));
        }
    }
    
    protected void assertIsPKInitialized()
    {
        if (!isPKInitialized())
        {
            throw new IllegalStateException(Localiser.msg("057001",this));
        }
    }

    protected void assertIsValidated()
    {
        if (!isValidated())
        {
            throw new IllegalStateException(Localiser.msg("057002",this));
        }
    }
    
    /**
     * Determine whether we or our concrete class allow DDL to be written into a file instead of
     * sending it to the DB. Defaults to true.
     * @return Whether it allows DDL outputting
     */
    protected boolean allowDDLOutput()
    {
        return true;
    }

    /**
     * Method to perform the required SQL statements.
     * @param stmts A List of statements
     * @param conn The Connection to the datastore
     * @throws SQLException Any exceptions thrown by the statements 
     **/
    protected void executeDdlStatementList(List<String> stmts, Connection conn)
    throws SQLException
    {
        Statement stmt = conn.createStatement();

        String stmtText=null;
        try
        {
            Iterator<String> i = stmts.iterator();
            while (i.hasNext())
            {
                stmtText = i.next();
                executeDdlStatement(stmt, stmtText);
            }
        }
        catch (SQLException sqe)
        {
            NucleusLogger.DATASTORE.error(Localiser.msg("057028",stmtText,sqe));
            throw sqe;
        }
        finally
        {
            stmt.close();
        }
    }

    /**
     * Execute a single DDL SQL statement with appropriate logging. 
     * If ddlWriter is set, do not actually execute the SQL but write it to that Writer.
     * @param stmt The JDBC Statement object to execute on
     * @param stmtText The actual SQL statement text
     * @throws SQLException Thrown if an error occurs
     */
    protected void executeDdlStatement(Statement stmt, String stmtText) 
    throws SQLException
    {
        Writer ddlWriter = storeMgr.getDdlWriter();
        if (ddlWriter != null && allowDDLOutput())
        {
            try
            {
                // make sure we write the same statement only once
                if (!storeMgr.hasWrittenDdlStatement(stmtText)) 
                {
                    // we shall write the SQL to a file instead of issuing to DB
                    ddlWriter.write(stmtText + ";\n\n");
                    storeMgr.addWrittenDdlStatement(stmtText);
                }
            }
            catch (IOException e)
            {
                NucleusLogger.DATASTORE_SCHEMA.error("error writing DDL into file for table " + toString() + " and statement=" + stmtText, e);
            }
        }
        else
        {
            if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_SCHEMA.debug(stmtText);
            }
            long startTime = System.currentTimeMillis();
            stmt.execute(stmtText);
            if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("045000",(System.currentTimeMillis() - startTime)));
            }
        }

        JDBCUtils.logWarnings(stmt);
    }

    /**
     * Determine whether our table exists in the datastore (without modifying datastore).
     * Result is cached
     * @param conn The Connection
     * @return Whether the table exists in the datastore
     * @throws SQLException Thrown if an error occurs
     */
    protected boolean tableExistsInDatastore(Connection conn) throws SQLException
    {
        if (existsInDatastore == null)
        {
            // exists() has not been called yet
            try
            {
                exists(conn, false);
            }
            catch (MissingTableException mte)
            {
            }
            // existsInDatastore will be set now
        }
        return existsInDatastore.booleanValue();
    }
}