/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms;

/**
 * Utility providing convenience naming of RDBMS persistence properties.
 */
public class RDBMSPropertyNames
{
    public static final String PROPERTY_RDBMS_LEGACY_NATIVE_VALUE_STRATEGY = "datanucleus.rdbms.useLegacyNativeValueStrategy";
    public static final String PROPERTY_RDBMS_DYNAMIC_SCHEMA_UPDATES = "datanucleus.rdbms.dynamicSchemaUpdates";
    public static final String PROPERTY_RDBMS_TABLE_COLUMN_ORDER = "datanucleus.rdbms.tableColumnOrder";

    public static final String PROPERTY_RDBMS_CLASS_ADDER_MAX_RETRIES = "datanucleus.rdbms.classAdditionMaxRetries";
    public static final String PROPERTY_RDBMS_ORACLE_NLS_SORT_ORDER = "datanucleus.rdbms.oracleNlsSortOrder";
    public static final String PROPERTY_RDBMS_DISCRIM_PER_SUBCLASS_TABLE = "datanucleus.rdbms.discriminatorPerSubclassTable";
    public static final String PROPERTY_RDBMS_CONSTRAINT_CREATE_MODE = "datanucleus.rdbms.constraintCreateMode";
    public static final String PROPERTY_RDBMS_UNIQUE_CONSTRAINTS_MAP_INVERSE = "datanucleus.rdbms.uniqueConstraints.mapInverse";
    public static final String PROPERTY_RDBMS_INIT_COLUMN_INFO = "datanucleus.rdbms.initializeColumnInfo";
    public static final String PROPERTY_RDBMS_STRING_DEFAULT_LENGTH = "datanucleus.rdbms.stringDefaultLength";
    public static final String PROPERTY_RDBMS_STRING_LENGTH_EXCEEDED_ACTION = "datanucleus.rdbms.stringLengthExceededAction";
    public static final String PROPERTY_RDBMS_COLUMN_DEFAULT_WHEN_NULL = "datanucleus.rdbms.useColumnDefaultWhenNull";
    public static final String PROPERTY_RDBMS_PERSIST_EMPTY_STRING_AS_NULL = "datanucleus.rdbms.persistEmptyStringAsNull";
    public static final String PROPERTY_RDBMS_CHECK_EXISTS_TABLES_VIEWS = "datanucleus.rdbms.checkExistTablesOrViews";
    public static final String PROPERTY_RDBMS_SCHEMA_TABLE_NAME = "datanucleus.rdbms.schemaTable.tableName";
    public static final String PROPERTY_RDBMS_CONNECTION_PROVIDER_NAME = "datanucleus.rdbms.connectionProviderName";
    public static final String PROPERTY_RDBMS_CONNECTION_PROVIDER_FAIL_ON_ERROR = "datanucleus.rdbms.connectionProviderFailOnError";
    public static final String PROPERTY_RDBMS_DATASTORE_ADAPTER_CLASS_NAME = "datanucleus.rdbms.datastoreAdapterClassName";
    public static final String PROPERTY_RDBMS_OMIT_DATABASEMETADATA_GETCOLUMNS = "datanucleus.rdbms.omitDatabaseMetaDataGetColumns";
    public static final String PROPERTY_RDBMS_ALLOW_COLUMN_REUSE = "datanucleus.rdbms.allowColumnReuse";
    public static final String PROPERTY_RDBMS_DEFAULT_SQL_TYPE = "datanucleus.rdbms.useDefaultSqlType";

    public static final String PROPERTY_RDBMS_INFORMIX_USE_SERIAL_FOR_IDENTITY = "datanucleus.rdbms.adapter.informixUseSerialForIdentity";

    public static final String PROPERTY_RDBMS_QUERY_MULTIVALUED_FETCH = "datanucleus.rdbms.query.multivaluedFetch";
    public static final String PROPERTY_RDBMS_QUERY_FETCH_DIRECTION = "datanucleus.rdbms.query.fetchDirection";
    public static final String PROPERTY_RDBMS_QUERY_RESULT_SET_TYPE = "datanucleus.rdbms.query.resultSetType";
    public static final String PROPERTY_RDBMS_QUERY_RESULT_SET_CONCURRENCY = "datanucleus.rdbms.query.resultSetConcurrency";
    public static final String PROPERTY_RDBMS_FETCH_UNLOADED_AUTO = "datanucleus.rdbms.fetchUnloadedAutomatically";

    public static final String PROPERTY_RDBMS_SQL_TABLE_NAMING_STRATEGY = "datanucleus.rdbms.sqlTableNamingStrategy";
    public static final String PROPERTY_RDBMS_STATEMENT_LOGGING = "datanucleus.rdbms.statementLogging";
    public static final String PROPERTY_RDBMS_STATEMENT_BATCH_LIMIT = "datanucleus.rdbms.statementBatchLimit";

    // TODO Likely these should move to core plugin
    public static final String PROPERTY_CONNECTION_POOL_MAX_CONNECTIONS = "datanucleus.connectionPool.maxConnections";
    public static final String PROPERTY_CONNECTION_POOL_MAX_STATEMENTS = "datanucleus.connectionPool.maxStatements";
    public static final String PROPERTY_CONNECTION_POOL_MAX_POOL_SIZE = "datanucleus.connectionPool.maxPoolSize";
    public static final String PROPERTY_CONNECTION_POOL_MIN_POOL_SIZE = "datanucleus.connectionPool.minPoolSize";
    public static final String PROPERTY_CONNECTION_POOL_INIT_POOL_SIZE = "datanucleus.connectionPool.initialPoolSize";
    public static final String PROPERTY_CONNECTION_POOL_MAX_IDLE = "datanucleus.connectionPool.maxIdle";
    public static final String PROPERTY_CONNECTION_POOL_MIN_IDLE = "datanucleus.connectionPool.minIdle";
    public static final String PROPERTY_CONNECTION_POOL_MAX_ACTIVE = "datanucleus.connectionPool.maxActive";
    public static final String PROPERTY_CONNECTION_POOL_MAX_WAIT = "datanucleus.connectionPool.maxWait";
    public static final String PROPERTY_CONNECTION_POOL_TEST_SQL = "datanucleus.connectionPool.testSQL";
    public static final String PROPERTY_CONNECTION_POOL_TIME_BETWEEN_EVICTOR_RUNS_MILLIS = "datanucleus.connectionPool.timeBetweenEvictionRunsMillis";
    public static final String PROPERTY_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS = "datanucleus.connectionPool.minEvictableIdleTimeMillis";
    public static final String PROPERTY_CONNECTION_POOL_DRIVER_PROPS = "datanucleus.connectionPool.driverProps";
    public static final String PROPERTY_CONNECTION_POOL_LEAK_DETECTION_THRESHOLD = "datanucleus.connectionPool.leakThreshold";
    public static final String PROPERTY_CONNECTION_POOL_MAX_LIFETIME = "datanucleus.connectionPool.maxLifetime";

    // TODO These are HikariCP specific, so maybe ought to be named as such
    public static final String PROPERTY_CONNECTION_POOL_AUTO_COMMIT = "datanucleus.connectionPool.autoCommit";
    public static final String PROPERTY_CONNECTION_POOL_CONNECTION_WAIT_TIMEOUT = "datanucleus.connectionPool.connectionWaitTimeout";
    public static final String PROPERTY_CONNECTION_POOL_NAME = "datanucleus.connectionPool.name";
    public static final String PROPERTY_CONNECTION_POOL_ALLOW_POOL_SUPSENSION= "datanucleus.connectionPool.allowPoolSuspension";
    public static final String PROPERTY_CONNECTION_POOL_READ_ONLY = "datanucleus.connectionPool.readOnly";
    public static final String PROPERTY_CONNECTION_POOL_VALIDATION_TIMEOUT = "datanucleus.connectionPool.validationTimeout";
    public static final String PROPERTY_CONNECTION_POOL_TRANSACTION_ISOLATION = "datanucleus.connectionPool.transactionIsolation";
    public static final String PROPERTY_CONNECTION_POOL_CATALOG = "datanucleus.connectionPool.catalog";
}