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
    private RDBMSPropertyNames(){}

    public static final String PROPERTY_RDBMS_LEGACY_NATIVE_VALUE_STRATEGY = "datanucleus.rdbms.useLegacyNativeValueStrategy".toLowerCase();
    public static final String PROPERTY_RDBMS_DYNAMIC_SCHEMA_UPDATES = "datanucleus.rdbms.dynamicSchemaUpdates".toLowerCase();
    public static final String PROPERTY_RDBMS_TABLE_COLUMN_ORDER = "datanucleus.rdbms.tableColumnOrder".toLowerCase();

    public static final String PROPERTY_RDBMS_CLASS_ADDER_MAX_RETRIES = "datanucleus.rdbms.classAdditionMaxRetries".toLowerCase();
    public static final String PROPERTY_RDBMS_DISCRIM_PER_SUBCLASS_TABLE = "datanucleus.rdbms.discriminatorPerSubclassTable".toLowerCase();
    public static final String PROPERTY_RDBMS_CONSTRAINT_CREATE_MODE = "datanucleus.rdbms.constraintCreateMode".toLowerCase();
    public static final String PROPERTY_RDBMS_UNIQUE_CONSTRAINTS_MAP_INVERSE = "datanucleus.rdbms.uniqueConstraints.mapInverse".toLowerCase();
    public static final String PROPERTY_RDBMS_INIT_COLUMN_INFO = "datanucleus.rdbms.initializeColumnInfo".toLowerCase();
    public static final String PROPERTY_RDBMS_STRING_DEFAULT_LENGTH = "datanucleus.rdbms.stringDefaultLength".toLowerCase();
    public static final String PROPERTY_RDBMS_STRING_LENGTH_EXCEEDED_ACTION = "datanucleus.rdbms.stringLengthExceededAction".toLowerCase();
    public static final String PROPERTY_RDBMS_COLUMN_DEFAULT_WHEN_NULL = "datanucleus.rdbms.useColumnDefaultWhenNull".toLowerCase();
    public static final String PROPERTY_RDBMS_PERSIST_EMPTY_STRING_AS_NULL = "datanucleus.rdbms.persistEmptyStringAsNull".toLowerCase();
    public static final String PROPERTY_RDBMS_CHECK_EXISTS_TABLES_VIEWS = "datanucleus.rdbms.checkExistTablesOrViews".toLowerCase();
    public static final String PROPERTY_RDBMS_SCHEMA_TABLE_NAME = "datanucleus.rdbms.schemaTable.tableName".toLowerCase();
    public static final String PROPERTY_RDBMS_DATASTORE_ADAPTER_CLASS_NAME = "datanucleus.rdbms.datastoreAdapterClassName".toLowerCase();
    public static final String PROPERTY_RDBMS_OMIT_DATABASEMETADATA_GETCOLUMNS = "datanucleus.rdbms.omitDatabaseMetaDataGetColumns".toLowerCase();
    public static final String PROPERTY_RDBMS_OMIT_VALUE_GENERATION_GETCOLUMNS = "datanucleus.rdbms.omitValueGenerationGetColumns".toLowerCase();
    public static final String PROPERTY_RDBMS_REFRESH_ALL_TABLES_ON_REFRESH_COLUMNS = "datanucleus.rdbms.refreshAllTablesOnRefreshColumns".toLowerCase();
    public static final String PROPERTY_RDBMS_ALLOW_COLUMN_REUSE = "datanucleus.rdbms.allowColumnReuse".toLowerCase();
    public static final String PROPERTY_RDBMS_DEFAULT_SQL_TYPE = "datanucleus.rdbms.useDefaultSqlType".toLowerCase();
    
    public static final String PROPERTY_RDBMS_CLONE_CALENDAR_FOR_DATE_TIMEZONE = "datanucleus.rdbms.cloneCalendarForDateTimezone".toLowerCase();

    public static final String PROPERTY_RDBMS_QUERY_MULTIVALUED_FETCH = "datanucleus.rdbms.query.multivaluedFetch".toLowerCase();
    public static final String PROPERTY_RDBMS_QUERY_FETCH_DIRECTION = "datanucleus.rdbms.query.fetchDirection".toLowerCase();
    public static final String PROPERTY_RDBMS_QUERY_RESULT_SET_TYPE = "datanucleus.rdbms.query.resultSetType".toLowerCase();
    public static final String PROPERTY_RDBMS_QUERY_RESULT_SET_CONCURRENCY = "datanucleus.rdbms.query.resultSetConcurrency".toLowerCase();

    public static final String PROPERTY_RDBMS_FETCH_UNLOADED_BASIC_AUTO = "datanucleus.rdbms.autoFetchUnloadedBasicFields".toLowerCase();
    public static final String PROPERTY_RDBMS_FETCH_UNLOADED_FK_AUTO = "datanucleus.rdbms.autoFetchUnloadedFKs".toLowerCase();

    public static final String PROPERTY_RDBMS_SQL_TABLE_NAMING_STRATEGY = "datanucleus.rdbms.sqlTableNamingStrategy".toLowerCase();
    public static final String PROPERTY_RDBMS_STATEMENT_LOGGING = "datanucleus.rdbms.statementLogging".toLowerCase();
    public static final String PROPERTY_RDBMS_STATEMENT_BATCH_LIMIT = "datanucleus.rdbms.statementBatchLimit".toLowerCase();

    // TODO Likely these should move to core plugin
    public static final String PROPERTY_CONNECTION_POOL_MAX_CONNECTIONS = "datanucleus.connectionPool.maxConnections".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_MAX_STATEMENTS = "datanucleus.connectionPool.maxStatements".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_MAX_POOL_SIZE = "datanucleus.connectionPool.maxPoolSize".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_MIN_POOL_SIZE = "datanucleus.connectionPool.minPoolSize".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_INIT_POOL_SIZE = "datanucleus.connectionPool.initialPoolSize".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_MAX_IDLE = "datanucleus.connectionPool.maxIdle".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_MIN_IDLE = "datanucleus.connectionPool.minIdle".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_MAX_ACTIVE = "datanucleus.connectionPool.maxActive".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_MAX_WAIT = "datanucleus.connectionPool.maxWait".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_TEST_SQL = "datanucleus.connectionPool.testSQL".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_TIME_BETWEEN_EVICTOR_RUNS_MILLIS = "datanucleus.connectionPool.timeBetweenEvictionRunsMillis".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS = "datanucleus.connectionPool.minEvictableIdleTimeMillis".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_DRIVER_PROPS = "datanucleus.connectionPool.driverProps".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_LEAK_DETECTION_THRESHOLD = "datanucleus.connectionPool.leakThreshold".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_MAX_LIFETIME = "datanucleus.connectionPool.maxLifetime".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_IDLE_TIMEOUT = "datanucleus.connectionPool.idleTimeout".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_VALIDATION_TIMEOUT = "datanucleus.connectionPool.validationTimeout".toLowerCase();

    // TODO These are HikariCP specific, so maybe ought to be named as such
    public static final String PROPERTY_CONNECTION_POOL_AUTO_COMMIT = "datanucleus.connectionPool.autoCommit".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_CONNECTION_WAIT_TIMEOUT = "datanucleus.connectionPool.connectionWaitTimeout".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_NAME = "datanucleus.connectionPool.name".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_ALLOW_POOL_SUPSENSION= "datanucleus.connectionPool.allowPoolSuspension".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_READ_ONLY = "datanucleus.connectionPool.readOnly".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_TRANSACTION_ISOLATION = "datanucleus.connectionPool.transactionIsolation".toLowerCase();
    public static final String PROPERTY_CONNECTION_POOL_CATALOG = "datanucleus.connectionPool.catalog".toLowerCase();

    // Oracle specific
    public static final String PROPERTY_RDBMS_ORACLE_NLS_SORT_ORDER = "datanucleus.rdbms.oracle.nlsSortOrder".toLowerCase();

    // Informix specific
    public static final String PROPERTY_RDBMS_INFORMIX_USE_SERIAL_FOR_IDENTITY = "datanucleus.rdbms.informix.useSerialForIdentity".toLowerCase();

    // MySQL specific
    public static final String PROPERTY_RDBMS_MYSQL_ENGINETYPE = "datanucleus.rdbms.mysql.engineType".toLowerCase();
    public static final String PROPERTY_RDBMS_MYSQL_COLLATION = "datanucleus.rdbms.mysql.collation";
    public static final String PROPERTY_RDBMS_MYSQL_CHARACTERSET = "datanucleus.rdbms.mysql.characterSet".toLowerCase();
}