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
2003 Andy Jefferson - commented and javadocs
2003 Andy Jefferson - added localiser
2003 Andy Jefferson - added sequence methods
2004 Erik Bengtson - added auto increment
2004 Erik Bengtson - added query operators, sql expressions
2004 Andy Jefferson - removed IndexMapping/OptimisticMapping
2008 Andy Jefferson - option Strings
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.identifier.IdentifierType;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.mapping.RDBMSMappingManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.schema.ForeignKeyInfo;
import org.datanucleus.store.rdbms.schema.RDBMSColumnInfo;
import org.datanucleus.store.rdbms.schema.RDBMSTypesInfo;
import org.datanucleus.store.rdbms.schema.SQLTypeInfo;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SQLText;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.table.TableImpl;
import org.datanucleus.store.rdbms.table.ViewImpl;
import org.datanucleus.store.schema.StoreSchemaHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Provides methods for adapting SQL language elements to a specific vendor's
 * database.  A database adapter is primarily used to map generic JDBC data
 * types and SQL identifiers to specific types/identifiers suitable for the
 * database in use.
 *
 * <p>Each database adapter corresponds to a particular combination of database,
 * database version, driver, and driver version, as provided by the driver's
 * own metadata.  Database adapters cannot be constructed directly, but must be
 * obtained using the {@link org.datanucleus.store.rdbms.adapter.DatastoreAdapterFactory} class.</p>
 *
 * @see DatastoreAdapterFactory
 * @see java.sql.DatabaseMetaData
 */
public class BaseDatastoreAdapter implements DatastoreAdapter
{
    protected Map<Integer, String> supportedJdbcTypesById = new HashMap();
    protected Map<Integer, String> unsupportedJdbcTypesById = new HashMap();
    /** The set of reserved keywords for this datastore. */
    protected final HashSet<String> reservedKeywords = new HashSet();

    /** The product name of the underlying datastore. */
    protected String datastoreProductName;

    /** The version number of the underlying datastore as a string. */
    protected String datastoreProductVersion;

    /** The major version number of the underlying datastore. */
    protected int datastoreMajorVersion;

    /** The minor version number of the underlying datastore. */
    protected int datastoreMinorVersion;

    /** The revision version number of the underlying datastore. */
    protected int datastoreRevisionVersion = 0;

    /** The String used to quote identifiers. */
    protected String identifierQuoteString;

    /** Supported option names. */
    protected Collection<String> supportedOptions = new HashSet();

    /** the JDBC driver name **/
    protected String driverName;

    /** the JDBC driver version **/
    protected String driverVersion;

    /** The major version number of the underlying driver. */
    protected int driverMajorVersion;

    /** The minor version number of the underlying driver. */
    protected int driverMinorVersion;

    /** The maximum length to be used for a table name. */
    protected int maxTableNameLength;

    /** The maximum length to be used for a table constraint name. */
    protected int maxConstraintNameLength;

    /** The maximum length to be used for an index name. */
    protected int maxIndexNameLength;

    /** The maximum length to be used for a column name. */
    protected int maxColumnNameLength;

    /** The String used to separate catalog and table name. */
    protected String catalogSeparator;

    /** Optional properties controlling the configuration. */
    protected Map<String, Object> properties = null;

    /**
     * Constructs a database adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    protected BaseDatastoreAdapter(DatabaseMetaData metadata)
    {
        super();

        // Add the supported and unsupported JDBC types for lookups
        supportedJdbcTypesById.put(Integer.valueOf(Types.BIGINT), "BIGINT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.BIT), "BIT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.BLOB), "BLOB");
        supportedJdbcTypesById.put(Integer.valueOf(Types.BOOLEAN), "BOOLEAN");
        supportedJdbcTypesById.put(Integer.valueOf(Types.CHAR), "CHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.CLOB), "CLOB");
        supportedJdbcTypesById.put(Integer.valueOf(Types.DATALINK), "DATALINK");
        supportedJdbcTypesById.put(Integer.valueOf(Types.DATE), "DATE");
        supportedJdbcTypesById.put(Integer.valueOf(Types.DECIMAL), "DECIMAL");
        supportedJdbcTypesById.put(Integer.valueOf(Types.DOUBLE), "DOUBLE");
        supportedJdbcTypesById.put(Integer.valueOf(Types.FLOAT), "FLOAT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.INTEGER), "INTEGER");
        supportedJdbcTypesById.put(Integer.valueOf(Types.LONGVARBINARY), "LONGVARBINARY");
        supportedJdbcTypesById.put(Integer.valueOf(Types.LONGVARCHAR), "LONGVARCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.NUMERIC), "NUMERIC");
        supportedJdbcTypesById.put(Integer.valueOf(Types.REAL), "REAL");
        supportedJdbcTypesById.put(Integer.valueOf(Types.SMALLINT), "SMALLINT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.TIME), "TIME");
        supportedJdbcTypesById.put(Integer.valueOf(Types.TIMESTAMP), "TIMESTAMP");
        supportedJdbcTypesById.put(Integer.valueOf(Types.TINYINT), "TINYINT");
        supportedJdbcTypesById.put(Integer.valueOf(Types.VARBINARY), "VARBINARY");
        supportedJdbcTypesById.put(Integer.valueOf(Types.VARCHAR), "VARCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.NVARCHAR), "NVARCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.NCHAR), "NCHAR");
        supportedJdbcTypesById.put(Integer.valueOf(Types.NCLOB), "NCLOB");
        supportedJdbcTypesById.put(Integer.valueOf(Types.OTHER), "OTHER");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.ARRAY), "ARRAY");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.BINARY), "BINARY");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.DISTINCT), "DISTINCT");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.JAVA_OBJECT), "JAVA_OBJECT");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.NULL), "NULL");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.REF), "REF");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.STRUCT), "STRUCT");
        unsupportedJdbcTypesById.put(Integer.valueOf(Types.SQLXML), "SQLXML");

        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(SQLConstants.SQL92_RESERVED_WORDS));
        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(SQLConstants.SQL99_RESERVED_WORDS));
        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(SQLConstants.SQL2003_RESERVED_WORDS));
        reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(SQLConstants.NONRESERVED_WORDS));

        try
        {
            try
            {
                String sqlKeywordsString = metadata.getSQLKeywords();
                reservedKeywords.addAll(StringUtils.convertCommaSeparatedStringToSet(sqlKeywordsString));
            }
            catch (SQLFeatureNotSupportedException fnse)
            {
                // Inept excuse for a JDBC driver
            }

            driverMinorVersion = metadata.getDriverMinorVersion();
            driverMajorVersion = metadata.getDriverMajorVersion();
            driverName = metadata.getDriverName();
            driverVersion = metadata.getDriverVersion();
            datastoreProductName = metadata.getDatabaseProductName();
            datastoreProductVersion = metadata.getDatabaseProductVersion();

            // Try to convert the "version" string into a W.X.Y.Z version string
            StringBuilder strippedProductVersion=new StringBuilder();
            char previousChar = ' ';
            for (int i=0; i<datastoreProductVersion.length(); ++i)
            {
                char c = datastoreProductVersion.charAt(i);
                if (Character.isDigit(c) || c == '.')
                {
                    // Only update the stripped version when we have "X."
                    if (previousChar != ' ')
                    {
                        if (strippedProductVersion.length() == 0)
                        {
                            strippedProductVersion.append(previousChar);
                        }
                        strippedProductVersion.append(c);
                    }
                    previousChar = c;
                }
                else
                {
                    previousChar = ' ';
                }
            }

            datastoreMajorVersion = metadata.getDatabaseMajorVersion();
            datastoreMinorVersion = metadata.getDatabaseMinorVersion();
            try
            {
                boolean noDBVersion = false;
                if (datastoreMajorVersion <= 0 && datastoreMinorVersion <= 0)
                {
                    // Check for the crap that they package with Websphere returning major/minor as 0.0
                    noDBVersion = true;
                }

                // Retrieve the Revision version if it is accessible
                StringTokenizer parts = new StringTokenizer(strippedProductVersion.toString(), ".");
                if (parts.hasMoreTokens())
                {
                    // Major version
                    if (noDBVersion)
                    {
                        try
                        {
                            datastoreMajorVersion = Integer.parseInt(parts.nextToken());
                        }
                        catch (Exception e)
                        {
                            datastoreMajorVersion = -1; //unknown
                        }
                    }
                    else
                    {
                        // already have it, so ignore this
                        parts.nextToken();
                    }
                }
                if (parts.hasMoreTokens())
                {
                    // Minor Version
                    if (noDBVersion)
                    {
                        try
                        {
                            datastoreMinorVersion = Integer.parseInt(parts.nextToken());
                        }
                        catch (Exception e)
                        {
                            datastoreMajorVersion = -1; //unknown
                        }
                    }
                    else
                    {
                        // already have it, so ignore this
                        parts.nextToken();
                    }
                }
                if (parts.hasMoreTokens())
                {
                    // Revision Version
                    try
                    {
                        datastoreRevisionVersion = Integer.parseInt(parts.nextToken());
                    }
                    catch (Exception e)
                    {
                        datastoreRevisionVersion = -1; //unknown
                    }
                }
            }
            catch (Throwable t)
            {
                /*
                 * The driver doesn't support JDBC 3.  Try to parse major and
                 * minor version numbers out of the product version string.
                 * We do this by stripping out everything but digits and periods
                 * and hoping we get something that looks like <major>.<minor>.
                 */
                StringTokenizer parts = new StringTokenizer(strippedProductVersion.toString(), ".");
                if (parts.hasMoreTokens())
                {
                    try
                    {
                        datastoreMajorVersion = Integer.parseInt(parts.nextToken());
                    }
                    catch (Exception e)
                    {
                        datastoreMajorVersion = -1; //unknown
                    }
                }
                if (parts.hasMoreTokens())
                {
                    try
                    {
                        datastoreMinorVersion = Integer.parseInt(parts.nextToken());
                    }
                    catch (Exception e)
                    {
                        datastoreMajorVersion = -1; //unknown
                    }
                }
                if (parts.hasMoreTokens())
                {
                    try
                    {
                        datastoreRevisionVersion = Integer.parseInt(parts.nextToken());
                    }
                    catch (Exception e)
                    {
                        datastoreRevisionVersion = -1; //unknown
                    }
                }
            }

            // Extract attributes of the Database adapter
            maxTableNameLength = metadata.getMaxTableNameLength();
            maxConstraintNameLength = metadata.getMaxTableNameLength();
            maxIndexNameLength = metadata.getMaxTableNameLength();
            maxColumnNameLength = metadata.getMaxColumnNameLength();
            if (metadata.supportsCatalogsInTableDefinitions())
            {
                supportedOptions.add(CATALOGS_IN_TABLE_DEFINITIONS);
            }
            if (metadata.supportsSchemasInTableDefinitions())
            {
                supportedOptions.add(SCHEMAS_IN_TABLE_DEFINITIONS);
            }
            if (metadata.supportsBatchUpdates())
            {
                supportedOptions.add(STATEMENT_BATCHING);
            }

            // Save the identifier cases available
            if (metadata.storesLowerCaseIdentifiers())
            {
                supportedOptions.add(IDENTIFIERS_LOWERCASE);
            }
            if (metadata.storesMixedCaseIdentifiers())
            {
                supportedOptions.add(IDENTIFIERS_MIXEDCASE);
            }
            if (metadata.storesUpperCaseIdentifiers())
            {
                supportedOptions.add(IDENTIFIERS_UPPERCASE);
            }
            if (metadata.storesLowerCaseQuotedIdentifiers())
            {
                supportedOptions.add(IDENTIFIERS_LOWERCASE_QUOTED);
            }
            if (metadata.storesMixedCaseQuotedIdentifiers())
            {
                supportedOptions.add(IDENTIFIERS_MIXEDCASE_QUOTED);
            }
            if (metadata.storesUpperCaseQuotedIdentifiers())
            {
                supportedOptions.add(IDENTIFIERS_UPPERCASE_QUOTED);
            }
            if (metadata.supportsMixedCaseIdentifiers())
            {
                supportedOptions.add(IDENTIFIERS_MIXEDCASE_SENSITIVE);
            }
            if (metadata.supportsMixedCaseQuotedIdentifiers())
            {
                supportedOptions.add(IDENTIFIERS_MIXEDCASE_QUOTED_SENSITIVE);
            }
            supportedOptions.add(HOLD_CURSORS_OVER_COMMIT); // TODO Could use metadata.supportResultSetHoldability but some JDBC drivers give unexpected results

            // Retrieve the catalog separator string (default = ".")
            catalogSeparator = metadata.getCatalogSeparator();
            catalogSeparator =
                ((catalogSeparator == null) || (catalogSeparator.trim().length() < 1)) ? "." : catalogSeparator;

            // Retrieve the identifier quote string (default = "")
            identifierQuoteString = metadata.getIdentifierQuoteString();
            identifierQuoteString =
                ((null == identifierQuoteString) || (identifierQuoteString.trim().length() < 1)) ?
                "\"" : identifierQuoteString;
        }
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("051004"), e);
        }

        supportedOptions.add(RESULTSET_TYPE_FORWARD_ONLY);
        supportedOptions.add(RESULTSET_TYPE_SCROLL_SENSITIVE);
        supportedOptions.add(RESULTSET_TYPE_SCROLL_INSENSITIVE);

        supportedOptions.add(RIGHT_OUTER_JOIN);
        supportedOptions.add(SOME_ANY_ALL_SUBQUERY_EXPRESSIONS);

        supportedOptions.add(UPDATE_STATEMENT_ALLOW_TABLE_ALIAS_IN_SET_CLAUSE);
        supportedOptions.add(UPDATE_DELETE_STATEMENT_ALLOW_TABLE_ALIAS_IN_WHERE_CLAUSE);

        supportedOptions.add(VIEWS);
        supportedOptions.add(DATETIME_STORES_MILLISECS);
        supportedOptions.add(ESCAPE_EXPRESSION_IN_LIKE_PREDICATE);
        supportedOptions.add(UNION_SYNTAX);
        supportedOptions.add(EXISTS_SYNTAX);
        supportedOptions.add(ALTER_TABLE_DROP_CONSTRAINT_SYNTAX);
        supportedOptions.add(DEFERRED_CONSTRAINTS);

        supportedOptions.add(DISTINCT_WITH_SELECT_FOR_UPDATE);
        supportedOptions.add(GROUPING_WITH_SELECT_FOR_UPDATE);
        supportedOptions.add(HAVING_WITH_SELECT_FOR_UPDATE);
        supportedOptions.add(ORDERING_WITH_SELECT_FOR_UPDATE);
        supportedOptions.add(MULTITABLES_WITH_SELECT_FOR_UPDATE);

        supportedOptions.add(PERSIST_OF_UNASSIGNED_CHAR);
        // TODO If the datastore supports CHECK in CREATE, we should turn off CHECK in END statement, and vice versa.
        supportedOptions.add(CHECK_IN_CREATE_STATEMENTS);
        supportedOptions.add(GET_GENERATED_KEYS_STATEMENT);
        supportedOptions.add(BOOLEAN_COMPARISON);
        supportedOptions.add(NULLS_IN_CANDIDATE_KEYS);
        supportedOptions.add(NULLS_KEYWORD_IN_COLUMN_OPTIONS);
        supportedOptions.add(DEFAULT_KEYWORD_IN_COLUMN_OPTIONS);
        supportedOptions.add(DEFAULT_KEYWORD_WITH_NOT_NULL_IN_COLUMN_OPTIONS);
        supportedOptions.add(DEFAULT_BEFORE_NULL_IN_COLUMN_OPTIONS);
        supportedOptions.add(ANSI_JOIN_SYNTAX);
        supportedOptions.add(ANSI_CROSSJOIN_SYNTAX);
        supportedOptions.add(AUTO_INCREMENT_KEYS_NULL_SPECIFICATION);
        supportedOptions.add(AUTO_INCREMENT_COLUMN_TYPE_SPECIFICATION);
        supportedOptions.add(INCLUDE_ORDERBY_COLS_IN_SELECT);
        supportedOptions.add(ACCESS_PARENTQUERY_IN_SUBQUERY_JOINED);
        supportedOptions.add(SUBQUERY_IN_HAVING);

        supportedOptions.add(VALUE_GENERATION_UUID_STRING);

        supportedOptions.add(FK_DELETE_ACTION_CASCADE);
        supportedOptions.add(FK_DELETE_ACTION_RESTRICT);
        supportedOptions.add(FK_DELETE_ACTION_DEFAULT);
        supportedOptions.add(FK_DELETE_ACTION_NULL);
        supportedOptions.add(FK_UPDATE_ACTION_CASCADE);
        supportedOptions.add(FK_UPDATE_ACTION_RESTRICT);
        supportedOptions.add(FK_UPDATE_ACTION_DEFAULT);
        supportedOptions.add(FK_UPDATE_ACTION_NULL);

        supportedOptions.add(TX_ISOLATION_READ_COMMITTED);
        supportedOptions.add(TX_ISOLATION_READ_UNCOMMITTED);
        supportedOptions.add(TX_ISOLATION_REPEATABLE_READ);
        supportedOptions.add(TX_ISOLATION_SERIALIZABLE);
    }

    /**
     * Creates the auxiliary functions/procedures in the schema 
     * @param conn the connection to the datastore
     */
    public void initialiseDatastore(Object conn)
    {
    }

    /**
     * Initialise the types for this datastore.
     * @param handler SchemaHandler that we initialise the types for
     * @param mconn Managed connection to use
     */
    public void initialiseTypes(StoreSchemaHandler handler, ManagedConnection mconn)
    {
        // Initialise the mappings available. Load all possible, and remove unsupported for this datastore
        RDBMSStoreManager storeMgr = (RDBMSStoreManager)handler.getStoreManager();
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        PluginManager pluginMgr = storeMgr.getNucleusContext().getPluginManager();
        MappingManager mapMgr = storeMgr.getMappingManager();
        mapMgr.loadDatastoreMapping(pluginMgr, clr, getVendorID());

        // Load the types from plugin(s)
        handler.getSchemaData(mconn.getConnection(), "types", null);
    }

    public String getNameForJDBCType(int jdbcType)
    {
        String typeName = supportedJdbcTypesById.get(Integer.valueOf(jdbcType));
        if (typeName == null)
        {
            typeName = unsupportedJdbcTypesById.get(Integer.valueOf(jdbcType));
        }
        return typeName;
    }

    public int getJDBCTypeForName(String typeName)
    {
        if (typeName == null)
        {
            return 0;
        }

        Set<Map.Entry<Integer, String>> entries = supportedJdbcTypesById.entrySet();
        Iterator<Map.Entry<Integer, String>> entryIter = entries.iterator();
        while (entryIter.hasNext())
        {
            Map.Entry<Integer, String> entry = entryIter.next();
            if (typeName.equalsIgnoreCase(entry.getValue()))
            {
                return entry.getKey().intValue();
            }
        }
        return 0;
    }

    /**
     * Set any properties controlling how the adapter is configured.
     * @param props The properties
     */
    public void setProperties(Map<String, Object> props)
    {
        if (props != null)
        {
            properties = new HashMap<String, Object>();
        }
        properties.putAll(props);
    }

    /**
     * Accessor for a property. Null imples not defined
     * @param name Name of the property
     * @return Its value
     */
    public Object getValueForProperty(String name)
    {
        return (properties != null ? properties.get(name) : null);
    }

    /**
     * Remove all mappings from the mapping manager that don't have a datastore type initialised.
     * @param handler Schema handler
     * @param mconn Managed connection to use
     */
    public void removeUnsupportedMappings(StoreSchemaHandler handler, ManagedConnection mconn)
    {
        RDBMSStoreManager storeMgr = (RDBMSStoreManager)handler.getStoreManager();
        RDBMSMappingManager mapMgr = (RDBMSMappingManager)storeMgr.getMappingManager();
        RDBMSTypesInfo types = (RDBMSTypesInfo)handler.getSchemaData(mconn.getConnection(), "types", null);

        Iterator<Map.Entry<Integer, String>> entryIter = supportedJdbcTypesById.entrySet().iterator();
        while (entryIter.hasNext())
        {
            Map.Entry<Integer, String> entry = entryIter.next();
            int jdbcType = entry.getKey();
            if (types.getChild("" + jdbcType) == null)
            {
                // JDBC type not supported by adapter so deregister the mapping
                // Means that we don't need to add "excludes" definitions to plugin.xml
                mapMgr.deregisterDatastoreMappingsForJDBCType(entry.getValue());
            }
        }
        entryIter = unsupportedJdbcTypesById.entrySet().iterator();
        while (entryIter.hasNext())
        {
            Map.Entry<Integer, String> entry = entryIter.next();
            int jdbcType = entry.getKey();
            if (types.getChild("" + jdbcType) == null)
            {
                // JDBC type not supported by adapter so deregister the mapping
                // Means that we don't need to add "excludes" definitions to plugin.xml
                mapMgr.deregisterDatastoreMappingsForJDBCType(entry.getValue());
            }
        }
    }

    /**
     * Accessor for the SQLType info for the specified JDBC type and the SQL type name.
     * @param handler Schema handler
     * @param mconn Connection
     * @param jdbcTypeNumber JDBC type
     * @return The SQL type info
     */
    protected Collection<SQLTypeInfo> getSQLTypeInfoForJdbcType(StoreSchemaHandler handler, ManagedConnection mconn,
            short jdbcTypeNumber)
    {
        RDBMSTypesInfo types = (RDBMSTypesInfo)handler.getSchemaData(mconn.getConnection(), "types", null);

        String key = "" + jdbcTypeNumber;
        org.datanucleus.store.rdbms.schema.JDBCTypeInfo jdbcType =
            (org.datanucleus.store.rdbms.schema.JDBCTypeInfo)types.getChild(key);
        if (jdbcType == null)
        {
            return null;
        }
        return jdbcType.getChildren().values();
    }

    /**
     * Convenience method for use by adapters to add their own fake JDBC/SQL types in where the 
     * JDBC driver doesn't provide some type.
     * @param handler the schema handler managing the types
     * @param mconn Connection to use
     * @param jdbcTypeNumber The JDBC type
     * @param sqlType The type info to use
     * @param addIfNotPresent whether to add only if JDBC type not present
     */
    protected void addSQLTypeForJDBCType(StoreSchemaHandler handler, ManagedConnection mconn,
            short jdbcTypeNumber, SQLTypeInfo sqlType, boolean addIfNotPresent)
    {
        RDBMSTypesInfo types = (RDBMSTypesInfo)handler.getSchemaData(mconn.getConnection(), "types", null);
        String key = "" + jdbcTypeNumber;
        org.datanucleus.store.rdbms.schema.JDBCTypeInfo jdbcType =
            (org.datanucleus.store.rdbms.schema.JDBCTypeInfo)types.getChild(key);
        if (jdbcType != null && !addIfNotPresent)
        {
            // Already have this JDBC type so ignore
            return;
        }
        else if (jdbcType == null)
        {
            // New JDBC type
            jdbcType = new org.datanucleus.store.rdbms.schema.JDBCTypeInfo(jdbcTypeNumber);
            types.addChild(jdbcType);
            jdbcType.addChild(sqlType);
        }
        else
        {
            // Existing JDBC type so add SQL type
            jdbcType.addChild(sqlType);
        }
    }

    /**
     * Accessor for whether this database adapter supports the specified transaction isolation.
     * @param level The isolation level (as defined by Connection enums).
     * @return Whether it is supported.
     */
    public boolean supportsTransactionIsolation(int level)
    {
        if ((level == Connection.TRANSACTION_NONE && supportsOption(DatastoreAdapter.TX_ISOLATION_NONE)) ||
            (level == Connection.TRANSACTION_READ_COMMITTED && supportsOption(DatastoreAdapter.TX_ISOLATION_READ_COMMITTED)) ||
            (level == Connection.TRANSACTION_READ_UNCOMMITTED && supportsOption(DatastoreAdapter.TX_ISOLATION_READ_UNCOMMITTED)) ||
            (level == Connection.TRANSACTION_REPEATABLE_READ && supportsOption(DatastoreAdapter.TX_ISOLATION_REPEATABLE_READ)) ||
            (level == Connection.TRANSACTION_SERIALIZABLE && supportsOption(DatastoreAdapter.TX_ISOLATION_SERIALIZABLE)))
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for the options that are supported by this datastore adapter and the underlying datastore.
     * @return The options (Collection&lt;String&gt;)
     */
    public Collection<String> getSupportedOptions()
    {
        return supportedOptions;
    }

    /**
     * Accessor for whether the supplied option is supported.
     * @param option The option
     * @return Whether supported.
     */
    public boolean supportsOption(String option)
    {
        return supportedOptions.contains(option);
    }

    /**
     * Accessor for a MappingManager suitable for use with this datastore adapter.
     * @param storeMgr The StoreManager
     * @return the MappingManager
     */
    public MappingManager getMappingManager(RDBMSStoreManager storeMgr)
    {
        return new RDBMSMappingManager(storeMgr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.DatastoreAdapter#getAdapterTime(java.sql.Timestamp)
     */
    public long getAdapterTime(Timestamp time)
    {
        long timestamp = getTime(time.getTime(), time.getNanos());
        int ms = getMiliseconds(time.getNanos());

        return timestamp + ms;
    }

    protected long getTime(long time, long nanos)
    {
        if (nanos < 0)
        {
            return (((time / 1000) - 1) * 1000);
        }
        return (time / 1000) * 1000;
    }

    protected int getMiliseconds(long nanos)
    {
        return (int) (nanos / 1000000);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatastoreAdapter#getDatastoreProductName()
     */
    public String getDatastoreProductName()
    {
        return datastoreProductName;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatastoreAdapter#getDatastoreProductVersion()
     */
    public String getDatastoreProductVersion()
    {
        return datastoreProductVersion;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatastoreAdapter#getDatastoreDriverName()
     */
    public String getDatastoreDriverName()
    {
        return driverName;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatastoreAdapter#getDatastoreDriverVersion()
     */
    public String getDatastoreDriverVersion()
    {
        return driverVersion;
    }

    /**
     * Whether the datastore will support setting the query fetch size to the supplied value.
     * @param size The value to set to
     * @return Whether it is supported.
     */
    public boolean supportsQueryFetchSize(int size)
    {
        // Default to supported all possible values
        return true;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.DatastoreAdapter#getVendorID()
     */
    public String getVendorID()
    {
        return null;
    }

    /**
     * Tests if a given string is a SQL keyword.
     * <p>
     * The list of key words tested against is defined to contain all SQL/92 keywords, plus any additional 
     * key words reported by the JDBC driver for this adapter via <code>DatabaseMetaData.getSQLKeywords()</code>.
     * <p>
     * In general, use of a SQL key word as an identifier should be avoided.
     * SQL/92 key words are divided into reserved and non-reserved words. If a reserved word is used as
     * an identifier it must be quoted with double quotes. Strictly speaking, the same is not true of 
     * non-reserved words. However, as C.J. Date writes in <u>A Guide To The SQL Standard </u>:
     * <blockquote>The rule by which it is determined within the standard that one key word needs to be 
     * reserved while another need not is not clear to this writer. In practice, it is probably wise to 
     * treat all key words as reserved.</blockquote>
     * @param word The word to test.
     * @return <code>true</code> if <var>word </var> is a SQL key word for this DBMS. 
     *     The comparison is case-insensitive.
     * @see SQLConstants
     */
    public boolean isReservedKeyword(String word)
    {
        return reservedKeywords.contains(word.toUpperCase());
    }

    /**
     * Accessor for an identifier quote string.
     * @return Identifier quote string.
     **/
    public String getIdentifierQuoteString()
    {
        return identifierQuoteString;
    }

    /**
     * Accessor for the JDBC driver major version
     * @return The driver major version
     */
    public int getDriverMajorVersion()
    {
        return driverMajorVersion;
    }

    /**
     * Accessor for the JDBC driver minor version
     * @return The driver minor version
     */
    public int getDriverMinorVersion()
    {
        return driverMinorVersion;
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
            return maxTableNameLength;
        }
        else if (identifierType == IdentifierType.COLUMN)
        {
            return maxColumnNameLength;
        }
        else if (identifierType == IdentifierType.CANDIDATE_KEY)
        {
            return maxConstraintNameLength;
        }
        else if (identifierType == IdentifierType.FOREIGN_KEY)
        {
            return maxConstraintNameLength;
        }
        else if (identifierType == IdentifierType.INDEX)
        {
            return maxIndexNameLength;
        }
        else if (identifierType == IdentifierType.PRIMARY_KEY)
        {
            return maxConstraintNameLength;
        }
        else if (identifierType == IdentifierType.SEQUENCE)
        {
            return maxTableNameLength;
        }
        else
        {
            return -1;
        }
    }

    /**
     * Accessor for the maximum foreign keys by table permitted for this datastore.
     * @return Max number of FKs for a table
     **/
    public int getMaxForeignKeys()
    {
        // TODO This is arbitrary. Should be relative to the RDBMS in use
        return 9999;
    }

    /**
     * Accessor for the maximum indexes by schema permitted for this datastore.
     * @return Max number of indexes for a table
     **/
    public int getMaxIndexes()
    {
        // TODO This is arbitrary. Should be relative to the RDBMS in use
        return 9999;
    }

    /**
     * Iterator for the reserved words constructed from the method
     * DataBaseMetaData.getSQLKeywords + standard SQL reserved words
     * @return an Iterator with a set of reserved words
     */
    public Iterator iteratorReservedWords()
    {
        return reservedKeywords.iterator();
    }

    public RDBMSColumnInfo newRDBMSColumnInfo(ResultSet rs)
    {
        return new RDBMSColumnInfo(rs);
    }

    public SQLTypeInfo newSQLTypeInfo(ResultSet rs)
    {
        return new SQLTypeInfo(rs);
    }

    /**
     * Method to return ForeignKeyInfo for the current row of the ResultSet which will have been
     * obtained from a call to DatabaseMetaData.getImportedKeys() or DatabaseMetaData.getExportedKeys().
     * @param rs The result set returned from DatabaseMetaData.get??portedKeys()
     * @return The foreign key info 
     */
    public ForeignKeyInfo newFKInfo(ResultSet rs)
    {
        return new ForeignKeyInfo(rs);
    }

    /**
     * Returns the precision value to be used when creating string columns of "unlimited" length.
     * Usually, if this value is needed it is provided in.  However, for some types in some databases 
     * the value must be computed.
     * @param typeInfo the typeInfo object for which the precision value is needed.
     * @return the precision value to be used when creating the column, or -1 if no value should be used.
     */
    public int getUnlimitedLengthPrecisionValue(SQLTypeInfo typeInfo)
    {
        if (typeInfo.getCreateParams() != null && typeInfo.getCreateParams().length() > 0)
        {
            return typeInfo.getPrecision();
        }

        return -1;
    }

    /**
     * Method to return whether the specified JDBC type is valid for use in a PrimaryKey.
     * @param datatype The JDBC type.
     * @return Whether it is valid for use in the PK
     */
    public boolean isValidPrimaryKeyType(JdbcType datatype)
    {
        // This is temporary since some RDBMS allow indexing of Blob/Clob/LongVarBinary
        // TODO Transfer to individual adapters
        if (datatype == JdbcType.BLOB || datatype == JdbcType.CLOB || datatype == JdbcType.LONGVARBINARY)
        {
            return false;
        }
        return true;
    }

    /**
     * Some databases, Oracle, treats an empty string (0 length) equals null
     * @return returns a surrogate to replace the empty string in the database
     * otherwise it would be treated as null
     */
    public String getSurrogateForEmptyStrings()
    {
        return null;
    }

    /**
     * Accessor for the transaction isolation level to use during schema creation.
     * @return The transaction isolation level for schema generation process
     */
    public int getTransactionIsolationForSchemaCreation()
    {
        return Connection.TRANSACTION_SERIALIZABLE;
    }

    /**
     * Accessor for the "required" transaction isolation level if it has to be a certain value
     * for this adapter.
     * @return Transaction isolation level (-1 implies no restriction)
     */
    public int getRequiredTransactionIsolationLevel()
    {
        return -1;
    }

    /**
     * Accessor for the Catalog Name for this datastore.
     * @param conn Connection to the datastore
     * @return The catalog name
     * @throws SQLException Thrown if error occurs in determining the catalog name.
     **/
    public String getCatalogName(Connection conn)
    throws SQLException
    {
        throw new UnsupportedOperationException(Localiser.msg("051015",datastoreProductName,datastoreProductVersion));
    }

    /**
     * Accessor for the Schema Name for this datastore.
     * @param conn Connection to the datastore
     * @return The schema name
     * @throws SQLException Thrown if error occurs in determining the schema name.
     **/
    public String getSchemaName(Connection conn)
    throws SQLException
    {
        throw new UnsupportedOperationException(Localiser.msg("051016",datastoreProductName,datastoreProductVersion));
    }

    /**
     * Accessor for the catalog separator.
     * @return Catalog separator string.
     **/
    public String getCatalogSeparator()
    {
        return catalogSeparator;
    }

    /**
     * The option to specify in "SELECT ... FROM TABLE ... WITH (option)" to lock instances
     * Null if not supported.
     * @return The option to specify with "SELECT ... FROM TABLE ... WITH (option)"
     **/
    public String getSelectWithLockOption()
    {
        return null;
    }

    /**
     * Method returning the text to append to the end of the SELECT to perform the equivalent
     * of "SELECT ... FOR UPDATE" (on some RDBMS). This method means that we can have different
     * text with some datastores (e.g Derby).
     * @return The "FOR UPDATE" text
     */
    public String getSelectForUpdateText()
    {
        return "FOR UPDATE";
    }

    /**
     * The function to creates a unique value of type uniqueidentifier.
     * @return The function. e.g. "SELECT NEWID()"
     **/
    public String getSelectNewUUIDStmt()
    {
        return null;
    }
    
    /**
     * The function to creates a unique value of type uniqueidentifier.
     * @return The function. e.g. "NEWID()"
     **/
    public String getNewUUIDFunction()
    {
        return null;
    }

    /**
     * Convenience method to allow adaption of an ordering string before applying it.
     * This is useful where the datastore accepts some conversion adapter around the ordering column
     * for example.
     * @param storeMgr StoreManager
     * @param orderString The basic ordering string
     * @param sqlExpr The sql expression being represented here
     * @return The adapted ordering string
     */
    public String getOrderString(StoreManager storeMgr, String orderString, SQLExpression sqlExpr)
    {
        return orderString;
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
        return true;
    }

    // ---------------------------- AutoIncrement Support ----------------------

    /**
     * Accessor for the autoincrementing sql statement for this datastore.
     * @param table Name of the table that the autoincrement is for
     * @param columnName Name of the column that the autoincrement is for
     * @return The statement for getting the latest autoincremented key
     **/
    public String getAutoIncrementStmt(Table table, String columnName)
    {
        throw new UnsupportedOperationException(Localiser.msg("051019"));
    }

    /**
     * Accessor for the autoincrementing keyword for generating DDLs.
     * (CREATE TABLEs...).
     * @return The keyword for a column using autoincrement
     **/
    public String getAutoIncrementKeyword()
    {
        throw new UnsupportedOperationException(Localiser.msg("051019"));
    }

    @Override
    public Class getAutoIncrementJavaTypeForType(Class type)
    {
        // Most datastores have no restrictions (maybe we should limit to Long, Integer, Short?)
        return type;
    }

    /**
     * Verifies if the given <code>typeName</code> is auto incremented by the datastore.
     * @param typeName the datastore type name
     * @return true when the <code>typeName</code> has values auto incremented by the datastore
     **/
    public boolean isIdentityFieldDataType(String typeName)
    {
        throw new UnsupportedOperationException(Localiser.msg("051019"));
    }

    /**
     * Method to return the INSERT statement to use when inserting into a table that has no
     * columns specified. This is the case when we have a single column in the table and that column
     * is autoincrement/identity (and so is assigned automatically in the datastore).
     * @param table The table
     * @return The statement for the INSERT
     */
    public String getInsertStatementForNoColumns(Table table)
    {
        return "INSERT INTO " + table.toString() + " () VALUES ()";
    }

    // ---------------------------- Sequence Support ---------------------------

    public boolean sequenceExists(Connection conn, String catalogName, String schemaName, String seqName)
    {
        // Override this with database-specific mechanism for checking if a sequence exists (still not part of standard JDBC after 15 yrs!)
        return true;
    }

    /**
     * Accessor for the sequence create statement for this datastore.
     * @param sequence_name Name of the sequence 
     * @param min Minimum value for the sequence
     * @param max Maximum value for the sequence
     * @param start Start value for the sequence
     * @param increment Increment value for the sequence
     * @param cache_size Cache size for the sequence
     * @return The statement for getting the next id from the sequence
     */
    public String getSequenceCreateStmt(String sequence_name,
                                        Integer min,Integer max,
                                        Integer start,Integer increment,
                                        Integer cache_size)
    {
        throw new UnsupportedOperationException(Localiser.msg("051020"));
    }

    /**
     * Accessor for the sequence statement to get the next id for this 
     * datastore.
     * @param sequence_name Name of the sequence 
     * @return The statement for getting the next id for the sequence
     **/
    public String getSequenceNextStmt(String sequence_name)
    {
        throw new UnsupportedOperationException(Localiser.msg("051020"));
    }

    /**
     * Provide the existing indexes in the database for the table.
     * This is implemented if and only if the datastore has its own way of getting indexes.
     * Otherwise we will use DatabaseMetaData.getIndexInfo().
     * The implementation here returns null.
     * @param conn the JDBC connection
     * @param catalog the catalog name
     * @param schema the schema name
     * @param table the table name
     * @return a ResultSet with the format @see DatabaseMetaData#getIndexInfo(java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
     * @throws SQLException if an error occurs
     */
    public ResultSet getExistingIndexes(Connection conn, String catalog, String schema, String table) 
    throws SQLException
    {
        return null;
    }

    /**
     * Returns the appropriate SQL to create the given table having the given
     * columns. No column constraints or key definitions should be included.
     * It should return something like:
     * <pre>
     * CREATE TABLE FOO ( BAR VARCHAR(30), BAZ INTEGER )
     * </pre>
     *
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
        createStmt.append("CREATE TABLE ").append(table.toString())
                  .append(getContinuationString())
                  .append("(")
                  .append(getContinuationString());
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
            if (pk != null && pk.size() > 0)
            {
                boolean includePk = true;
                if (supportsOption(AUTO_INCREMENT_PK_IN_CREATE_TABLE_COLUMN_DEF))
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
                    NucleusLogger.GENERAL.debug(">> TODO Add FK in CREATE TABLE as " + fk);
                    // TODO Add the FK. Make sure that the other table exists
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
	            if (columns[i].getConstraints() != null)
	            {
	                checkConstraintStmt.append(",").append(getContinuationString());
	                checkConstraintStmt.append(indent).append(columns[i].getConstraints());
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
     * Returns the appropriate SQL to add a primary key to its table.
     * It should return something like:
     * <pre>
     * ALTER TABLE FOO ADD CONSTRAINT FOO_PK PRIMARY KEY (BAR)
     * ALTER TABLE FOO ADD PRIMARY KEY (BAR)
     * </pre>
     *
     * @param pk An object describing the primary key.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        if (pk.getName() != null)
        {
            String identifier = factory.getIdentifierInAdapterCase(pk.getName());
            return "ALTER TABLE " + pk.getTable().toString() + " ADD CONSTRAINT " + identifier + ' ' + pk;
        }

        return "ALTER TABLE " + pk.getTable().toString() + " ADD " + pk;
    }

    /**
     * Returns the appropriate SQL to add a candidate key to its table.
     * It should return something like:
     * <pre>
     * ALTER TABLE FOO ADD CONSTRAINT FOO_CK UNIQUE (BAZ)
     * ALTER TABLE FOO ADD UNIQUE (BAZ)
     * </pre>
     *
     * @param ck An object describing the candidate key.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    public String getAddCandidateKeyStatement(CandidateKey ck, IdentifierFactory factory)
    {
        if (ck.getName() != null)
        {
            String identifier = factory.getIdentifierInAdapterCase(ck.getName());
            return "ALTER TABLE " + ck.getTable().toString() + " ADD CONSTRAINT " + identifier + ' ' + ck;
        }

        return "ALTER TABLE " + ck.getTable().toString() + " ADD " + ck;
    }

    /**
     * Returns the appropriate SQL to add a foreign key to its table.
     * It should return something like:
     * <pre>
     * ALTER TABLE FOO ADD CONSTRAINT FOO_FK1 FOREIGN KEY (BAR, BAZ) REFERENCES ABC (COL1, COL2)
     * ALTER TABLE FOO ADD FOREIGN KEY (BAR, BAZ) REFERENCES ABC (COL1, COL2)
     * </pre>
     * @param fk An object describing the foreign key.
     * @param factory Identifier factory
     * @return  The text of the SQL statement.
     */
    public String getAddForeignKeyStatement(ForeignKey fk, IdentifierFactory factory)
    {
        if (fk.getName() != null)
        {
            String identifier = factory.getIdentifierInAdapterCase(fk.getName());
            return "ALTER TABLE " + fk.getTable().toString() + " ADD CONSTRAINT " + identifier + ' ' + fk;
        }

        return "ALTER TABLE " + fk.getTable().toString() + " ADD " + fk;
    }

    /**
     * Accessor for the SQL statement to add a column to a table.
     * @param table The table
     * @param col The column
     * @return The SQL necessary to add the column
     */
    public String getAddColumnStatement(Table table, Column col)
    {
        return "ALTER TABLE " + table.toString() + " ADD " + col.getSQLDefinition();
    }

    /**
     * Returns the appropriate DDL to create an index.
     * It should return something like:
     * <pre>
     * CREATE INDEX FOO_N1 ON FOO (BAR,BAZ) [Extended Settings]
     * CREATE UNIQUE INDEX FOO_U1 ON FOO (BAR,BAZ) [Extended Settings]
     * </pre>
     * @param idx An object describing the index.
     * @param factory Identifier factory
     * @return The text of the SQL statement.
     */
    public String getCreateIndexStatement(Index idx, IdentifierFactory factory)
    {
        DatastoreIdentifier indexIdentifier = factory.newTableIdentifier(idx.getName());
        return 
           "CREATE " + (idx.getUnique() ? "UNIQUE " : "") + "INDEX " + indexIdentifier.getFullyQualifiedName(true) + 
           " ON " + idx.getTable().toString() + ' ' +
           idx + (idx.getExtendedIndexSettings() == null ? "" : " " + idx.getExtendedIndexSettings());
    }

    /**
     * Creates a CHECK constraint definition based on the given values
     * e.g. <pre>CHECK ("COLUMN" IN ('VAL1','VAL2') OR "COLUMN" IS NULL)</pre>
     * @param identifier Column identifier
     * @param values Valid values
     * @param nullable whether the datastore identifier is null
     * @return The check constraint
     */
    public String getCheckConstraintForValues(DatastoreIdentifier identifier, Object[] values, boolean nullable)
    {
        StringBuilder constraints = new StringBuilder("CHECK (");
        constraints.append(identifier);
        constraints.append(" IN (");
        for (int i=0;i<values.length;i++)
        {
            if (i > 0)
            {
                constraints.append(",");
            }
            if (values[i] instanceof String)
            {
                constraints.append("'").append(values[i]).append("'");
            }
            else
            {
                constraints.append(values[i]);
            }
        }
        constraints.append(")");
        if (nullable)
        {
            constraints.append(" OR " + identifier + " IS NULL");
        }
        constraints.append(")");
        return constraints.toString();
    }

    public String getCreateDatabaseStatement(String catalogName, String schemaName)
    {
        return "CREATE SCHEMA " + schemaName;
    }

    public String getDropDatabaseStatement(String catalogName, String schemaName)
    {
        return "DROP SCHEMA " + schemaName;
    }

    /**
     * Returns the appropriate SQL to drop the given table.
     * It should return something like:
     * <pre>
     * DROP TABLE FOO CASCADE
     * </pre>
     *
     * @param table The table to drop.
     * @return The text of the SQL statement.
     */
    public String getDropTableStatement(Table table)
    {
        return "DROP TABLE " + table.toString() + " CASCADE";
    }

    /**
     * Returns the appropriate SQL to drop the given view.
     * It should return something like:
     * <pre>
     * DROP VIEW FOO
     * </pre>
     *
     * @param view The view to drop.
     * @return The text of the SQL statement.
     */
    public String getDropViewStatement(ViewImpl view)
    {
        return "DROP VIEW " + view.toString();
    }

    /**
     * Method to return the basic SQL for a DELETE TABLE statement.
     * Returns the String as <code>DELETE FROM tbl t1</code>. Doesn't include any where clause.
     * @param tbl The SQLTable to delete
     * @return The delete table string
     */
    public String getDeleteTableStatement(SQLTable tbl)
    {
        return "DELETE FROM " + tbl.toString();
    }

    /**
     * Method to return the SQLText for an UPDATE TABLE statement.
     * Returns the SQLText for <code>UPDATE tbl t1 SET x1 = val1, x2 = val2</code>.
     * Override if the datastore doesn't support that standard syntax.
     * @param tbl The primary table
     * @param setSQL The SQLText for the SET component
     * @return SQLText for the update statement
     */
    public SQLText getUpdateTableStatement(SQLTable tbl, SQLText setSQL)
    {
        SQLText sql = new SQLText("UPDATE ");
        sql.append(tbl.toString()); // "MYTBL T1"
        sql.append(" ").append(setSQL);
        return sql;
    }

    /**
     * Method to return the SQL to append to the end of the SELECT statement to handle
     * restriction of ranges using the LIMIT keyword. Defaults to an empty string (not supported).
     * SELECT param ... WHERE {LIMIT}
     * @param offset The offset to return from
     * @param count The number of items to return
     * @return The SQL to append to allow for ranges using LIMIT.
     */
    public String getRangeByLimitEndOfStatementClause(long offset, long count)
    {
        return "";
    }

    /**
     * Method to return the column name to use when handling ranges via
     * a rownumber on the select using the original method (DB2). Defaults to an empty string (not supported).
     * @return The row number column.
     */
    public String getRangeByRowNumberColumn()
    {
        return "";
    }

    /**
     * Method to return the column name to use when handling ranges via
     * a rownumber on the select using the second method (Oracle). Defaults to an empty string (not supported).
     * @return The row number column.
     */
    public String getRangeByRowNumberColumn2()
    {
        return "";
    }

    /**
     * Accessor for table and column information for a catalog/schema in this datastore.
     * @param conn Connection to use
     * @param catalog The catalog (null if none)
     * @param schema The schema (null if none)
     * @param tableNamePattern The table name pattern (null if all)
     * @param columnNamePattern The column name pattern (null if all)
     * @return ResultSet containing the table/column information
     * @throws SQLException Thrown if an error occurs
     */
    public ResultSet getColumns(Connection conn, String catalog, String schema, String tableNamePattern, String columnNamePattern)
    throws SQLException
    {
        DatabaseMetaData dmd = conn.getMetaData();
        return dmd.getColumns(catalog, schema, tableNamePattern, columnNamePattern);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("================ DatabaseAdapter ==================");
        sb.append("\n");
        sb.append("Adapter : " + this.getClass().getName());
        sb.append("\n");
        sb.append("Datastore : name=\"" + datastoreProductName + "\" version=\"" + datastoreProductVersion + 
            "\" (major=" + datastoreMajorVersion + ", minor=" + datastoreMinorVersion + ", revision=" + datastoreRevisionVersion + ")");
        sb.append("\n");
        sb.append("Driver : name=\"" + driverName + "\" version=\"" + driverVersion + 
            "\" (major=" + driverMajorVersion + ", minor=" + driverMinorVersion + ")");
        sb.append("\n");
        sb.append("===================================================");
        return sb.toString();
    }

    /**
     * Accessor for a statement that will return the statement to use to get the datastore date.
     * @return SQL statement to get the datastore date
     */
    public String getDatastoreDateStatement()
    {
        return "SELECT CURRENT_TIMESTAMP";
    }

    /**
     * The pattern string for representing one character that is expanded in word searches.
     * Most of databases will use the underscore character.
     * @return the pattern string.
     **/
    public String getPatternExpressionAnyCharacter()
    {
        return "_";
    }
    
    /**
     * The pattern string for representing zero or more characters that is expanded in word searches.
     * Most of databases will use the percent sign character.
     * @return the pattern string.
     **/
    public String getPatternExpressionZeroMoreCharacters()
    {
        return "%";
    }
    
    /**
     * The character for escaping characters in pattern expressions.
     * @return the character.
     **/
    public String getEscapePatternExpression()
    {
        return "ESCAPE '\\'";
    }
    
    /**
     * The character for escaping characters in pattern expressions.
     * @return the character.
     **/
    public String getEscapeCharacter()
    {
        return "\\";
    }

    /**
     * Continuation string to use where the SQL statement goes over more than 1
     * line. Some JDBC adapters (e.g DB2) don't do conversion.
     * @return Continuation string.
     **/
    public String getContinuationString()
    {
        return "\n";
    }

    /**
     * Accessor for the function to use for converting to numeric.
     * @return The numeric conversion function for this datastore.
     */
    public String getNumericConversionFunction()
    {
        return "ASCII";
    }

    /**
     * An operator in a string expression that concatenates two or more
     * character or binary strings, columns, or a combination of strings and
     * column names into one expression (a string operator).
     * 
     * @return the operator SQL String
     */
    public String getOperatorConcat()
    {
        return "||";
    }

    /**
     * return whether this exception represents a cancelled statement.
     * @param sqle the exception
     * @return whether it is a cancel
     */
    public boolean isStatementCancel(SQLException sqle)
    {
        return false;
    }

    /**
     * return whether this exception represents a timed out statement.
     * @param sqle the exception
     * @return whether it is a timeout
     */
    public boolean isStatementTimeout(SQLException sqle)
    {
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.DatastoreAdapter#validToIndexMapping(org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping)
     */
    @Override
    public boolean validToIndexMapping(JavaTypeMapping mapping)
    {
        return true;
    }
}