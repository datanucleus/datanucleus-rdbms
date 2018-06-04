/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.sql.expression;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.plugin.ConfigurationElement;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.method.SQLMethod;
import org.datanucleus.store.rdbms.sql.operation.SQLOperation;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Factory for creating SQL expressions/literals.
 * These are typically called when we are building up an SQL statement and we want to impose conditions
 * using the fields of a class, and values for the field.
 */
public class SQLExpressionFactory
{
    private static final Class[] EXPR_CREATION_ARG_TYPES = new Class[] {SQLStatement.class, SQLTable.class, JavaTypeMapping.class};
    private static final Class[] LIT_CREATION_ARG_TYPES = new Class[] {SQLStatement.class, JavaTypeMapping.class, Object.class, String.class};

    RDBMSStoreManager storeMgr;

    ClassLoaderResolver clr;

    /** Cache of expression class, keyed by the mapping class name. */
    Map<String, Class> expressionClassByMappingName = new ConcurrentHashMap<>();

    /** Cache of literal class, keyed by the mapping class name. */
    Map<String, Class> literalClassByMappingName = new ConcurrentHashMap<>();

    /** Keys of SQLMethods that are supported. */
    Set<MethodKey> pluginSqlMethodsKeysSupported = new HashSet<>();

    /** Cache of SQLMethod instances, keyed by their class+method[+datastore] name. */
    Map<MethodKey, SQLMethod> sqlMethodsByKey = new HashMap<>();

    /** Keys of SQLOperations that are supported. */
    Set<String> pluginSqlOperationKeysSupported = new HashSet<>();

    /** Cache of SQLOperation instances, keyed by their name. */
    Map<String, SQLOperation> sqlOperationsByName = new HashMap<>();

    /** Map of JavaTypeMapping for use in query expressions, keyed by the type being represented. */
    Map<Class, JavaTypeMapping> mappingByClass = new HashMap<>();

    private class MethodKey
    {
        String clsName;
        String methodName;
        String datastoreName;

        public int hashCode()
        {
            return (clsName + methodName + datastoreName).hashCode();
        }
        public boolean equals(Object other)
        {
            if (other == null || !(other instanceof MethodKey))
            {
                return false;
            }
            MethodKey otherKey = (MethodKey)other;
            return (otherKey.clsName.equals(clsName) && otherKey.methodName.equals(methodName) && otherKey.datastoreName.equals(datastoreName));
        }
    }

    /**
     * Constructor for an SQLExpressionFactory.
     * Also loads up the defined SQL methods [extension-point: "org.datanucleus.store.rdbms.sql_method"] and caches the keys.
     * Also loads up the defined SQL operations [extension-point: "org.datanucleus.store.rdbms.sql_operation"] and caches the keys.
     * @param storeMgr RDBMS Manager
     */
    public SQLExpressionFactory(RDBMSStoreManager storeMgr)
    {
        this.storeMgr = storeMgr;
        this.clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);

        PluginManager pluginMgr = storeMgr.getNucleusContext().getPluginManager();

        // Load up keys of all SQLMethods supported via the plugin mechanism
        ConfigurationElement[] methodElems = pluginMgr.getConfigurationElementsForExtension("org.datanucleus.store.rdbms.sql_method", null, null);
        if (methodElems != null)
        {
            for (int i=0;i<methodElems.length;i++)
            {
                String datastoreName = methodElems[i].getAttribute("datastore");
                if (StringUtils.isWhitespace(datastoreName) || storeMgr.getDatastoreAdapter().getVendorID().equals(datastoreName))
                {
                    String className = methodElems[i].getAttribute("class");
                    String methodName = methodElems[i].getAttribute("method").trim();
                    MethodKey key = getSQLMethodKey(datastoreName, className, methodName);
                    pluginSqlMethodsKeysSupported.add(key);
                }
            }
        }

        // Load up keys of all SQLOperations supported via the plugin mechanism
        ConfigurationElement[] operationElems = pluginMgr.getConfigurationElementsForExtension("org.datanucleus.store.rdbms.sql_operation", null, null);
        if (operationElems != null)
        {
            for (int i=0;i<operationElems.length;i++)
            {
                String datastoreName = operationElems[i].getAttribute("datastore");
                if (StringUtils.isWhitespace(datastoreName) || storeMgr.getDatastoreAdapter().getVendorID().equals(datastoreName))
                {
                    String name = operationElems[i].getAttribute("name").trim();
                    String key = getSQLOperationKey(datastoreName, name);
                    pluginSqlOperationKeysSupported.add(key);
                }
            }
        }

        // Load up built-in expression class names for mapping
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.BigDecimalMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.NumericExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.BigIntegerMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.NumericExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.BooleanMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.BooleanExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ByteMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ByteExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.CharacterMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.CharacterExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.DoubleMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.NumericExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.FloatMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.NumericExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.IntegerMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.NumericExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.LongMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.NumericExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ShortMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.NumericExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.EnumMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.EnumExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.NumberMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.NumericExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.StringMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.StringExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OracleStringLobMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.StringExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OptionalMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.OptionalExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.UUIDMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.StringExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.DateMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TemporalExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.SqlDateMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TemporalExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.SqlTimeMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TemporalExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.SqlTimestampMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TemporalExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.GregorianCalendarMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TemporalExpression.class);

        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ArrayExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OracleArrayMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ArrayExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.CollectionExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OracleCollectionMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.CollectionExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.SingleCollectionMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.SingleCollectionExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.MapMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.MapExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OracleMapMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.MapExpression.class);

        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.DiscriminatorMapping.DiscriminatorLongMapping.class.getName(), 
            org.datanucleus.store.rdbms.sql.expression.NumericExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.DiscriminatorMapping.DiscriminatorStringMapping.class.getName(), 
            org.datanucleus.store.rdbms.sql.expression.StringExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.VersionMapping.VersionLongMapping.class.getName(), 
            org.datanucleus.store.rdbms.sql.expression.NumericExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.VersionMapping.VersionTimestampMapping.class.getName(), 
            org.datanucleus.store.rdbms.sql.expression.TemporalExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OrderIndexMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.NumericExpression.class);

        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.DatastoreIdMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.PersistableMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.PersistableIdMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ReferenceIdMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ObjectMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ReferenceMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.InterfaceMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.EmbeddedPCMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.EmbeddedElementPCMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.EmbeddedKeyPCMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.EmbeddedValuePCMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectExpression.class);

        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.TypeConverterMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TypeConverterExpression.class);
        expressionClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.TypeConverterMultiMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TypeConverterMultiExpression.class);

        // Load up built-in literal class names for mapping
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.BigDecimalMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.FloatingPointLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.BigIntegerMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.IntegerLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.BooleanMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.BooleanLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ByteMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ByteLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.CharacterMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.CharacterLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.DoubleMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.FloatingPointLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.FloatMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.FloatingPointLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.IntegerMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.IntegerLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.LongMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.IntegerLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ShortMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.IntegerLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.EnumMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.EnumLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.NumberMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.FloatingPointLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.StringMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.StringLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OracleStringLobMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.StringLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OptionalMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.OptionalLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.UUIDMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.StringLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.DateMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TemporalLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.SqlDateMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TemporalLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.SqlTimeMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TemporalLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.SqlTimestampMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TemporalLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.GregorianCalendarMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TemporalLiteral.class);

        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ArrayMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ArrayLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OracleArrayMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ArrayLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.CollectionMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.CollectionLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OracleCollectionMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.CollectionLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.SingleCollectionMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.SingleCollectionLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.MapMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.MapLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OracleMapMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.MapLiteral.class);

        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.DiscriminatorMapping.DiscriminatorLongMapping.class.getName(), 
            org.datanucleus.store.rdbms.sql.expression.IntegerLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.DiscriminatorMapping.DiscriminatorStringMapping.class.getName(), 
            org.datanucleus.store.rdbms.sql.expression.StringLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.VersionMapping.VersionLongMapping.class.getName(), 
            org.datanucleus.store.rdbms.sql.expression.IntegerLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.VersionMapping.VersionTimestampMapping.class.getName(), 
            org.datanucleus.store.rdbms.sql.expression.TemporalLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.OrderIndexMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.IntegerLiteral.class);

        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.NullMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.NullLiteral.class);

        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.DatastoreIdMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.PersistableMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.PersistableIdMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ReferenceIdMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ObjectMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.ReferenceMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.InterfaceMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.EmbeddedPCMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.EmbeddedElementPCMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.EmbeddedKeyPCMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.EmbeddedValuePCMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.ObjectLiteral.class);

        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.TypeConverterMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TypeConverterLiteral.class);
        literalClassByMappingName.put(org.datanucleus.store.rdbms.mapping.java.TypeConverterMultiMapping.class.getName(), org.datanucleus.store.rdbms.sql.expression.TypeConverterMultiLiteral.class);
    }

    /**
     * Factory for an expression representing a mapping on a table.
     * @param stmt The statement
     * @param sqlTbl The table
     * @param mapping The mapping
     * @return The expression
     */
    public SQLExpression newExpression(SQLStatement stmt, SQLTable sqlTbl, JavaTypeMapping mapping)
    {
        return newExpression(stmt, sqlTbl, mapping, null);
    }

    /**
     * Factory for an expression representing a mapping on a table.
     * @param stmt The statement
     * @param sqlTbl The table
     * @param mapping The mapping
     * @param parentMapping Optional parent mapping of this mapping (e.g when handling an implementation of an interface)
     * @return The expression
     */
    public SQLExpression newExpression(SQLStatement stmt, SQLTable sqlTbl, JavaTypeMapping mapping, JavaTypeMapping parentMapping)
    {
        // Use "new SQLExpression(SQLStatement, SQLTable, JavaTypeMapping)"
        SQLTable exprSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(stmt, sqlTbl, parentMapping == null ? mapping : parentMapping);
        Object[] args = new Object[] {stmt, exprSqlTbl, mapping};

        Class expressionClass = expressionClassByMappingName.get(mapping.getClass().getName());
        if (expressionClass != null)
        {
            // Use built-in expression class
            return (SQLExpression)ClassUtils.newInstance(expressionClass, EXPR_CREATION_ARG_TYPES, new Object[] {stmt, exprSqlTbl, mapping});
        }

        try
        {
            // Fallback to the plugin mechanism
            SQLExpression sqlExpr = (SQLExpression)storeMgr.getNucleusContext().getPluginManager().createExecutableExtension("org.datanucleus.store.rdbms.sql_expression", 
                "mapping-class", mapping.getClass().getName(), "expression-class", EXPR_CREATION_ARG_TYPES, args);
            if (sqlExpr == null)
            {
                throw new NucleusException(Localiser.msg("060004", mapping.getClass().getName()));
            }

            expressionClassByMappingName.put(mapping.getClass().getName(), sqlExpr.getClass());
            return sqlExpr;
        }
        catch (Exception e)
        {
            String msg = Localiser.msg("060005", mapping.getClass().getName());
            NucleusLogger.QUERY.error(msg, e);
            throw new NucleusException(msg, e);
        }
    }

    /**
     * Factory for a literal representing a value.
     * To create a NullLiteral pass in a null mapping.
     * @param stmt The statement
     * @param mapping The mapping
     * @param value The value
     * @return The literal
     */
    public SQLExpression newLiteral(SQLStatement stmt, JavaTypeMapping mapping, Object value)
    {
        // Use "new SQLLiteral(SQLStatement, JavaTypeMapping, Object, String)"
        Object[] args = new Object[] {stmt, mapping, value, null};

        if (mapping != null)
        {
            Class literalClass = literalClassByMappingName.get(mapping.getClass().getName());
            if (literalClass != null)
            {
                // Use cached literal class
                return (SQLExpression)ClassUtils.newInstance(literalClass, LIT_CREATION_ARG_TYPES, args);
            }
        }

        try
        {
            if (mapping == null)
            {
                return (SQLExpression)ClassUtils.newInstance(NullLiteral.class, LIT_CREATION_ARG_TYPES, args);
            }

            Class literalClass = literalClassByMappingName.get(mapping.getClass().getName());
            if (literalClass != null)
            {
                // Use built-in literal class
                return (SQLExpression)ClassUtils.newInstance(literalClass, LIT_CREATION_ARG_TYPES, args);
            }

            // Fallback to the plugin mechanism
            SQLExpression sqlExpr = (SQLExpression) storeMgr.getNucleusContext().getPluginManager().createExecutableExtension("org.datanucleus.store.rdbms.sql_expression",
                "mapping-class", mapping.getClass().getName(), "literal-class", LIT_CREATION_ARG_TYPES, args);
            if (sqlExpr == null)
            {
                throw new NucleusException(Localiser.msg("060006", mapping.getClass().getName()));
            }

            literalClassByMappingName.put(mapping.getClass().getName(), sqlExpr.getClass());
            return sqlExpr;
        }
        catch (Exception e)
        {
            String mappingName = (mapping != null) ? mapping.getClass().getName() : null;
            NucleusLogger.QUERY.error("Exception creating SQLLiteral for mapping " + mappingName, e);
            throw new NucleusException(Localiser.msg("060007", mappingName));
        }
    }

    /**
     * Factory for a literal as an input parameter.
     * If the mapping (type of parameter) is not known at this point then put in null and it will return a ParameterLiteral.
     * @param stmt The statement
     * @param mapping The mapping
     * @param value Value of the literal (if known)
     * @param paramName The parameter name
     * @return The literal
     */
    public SQLExpression newLiteralParameter(SQLStatement stmt, JavaTypeMapping mapping, Object value, String paramName)
    {
        // Use "new SQLLiteral(SQLStatement, JavaTypeMapping, Object, String)"
        Object[] args = new Object[] {stmt, mapping, value, paramName};

        try
        {
            if (mapping == null)
            {
                return (SQLExpression)ClassUtils.newInstance(ParameterLiteral.class, LIT_CREATION_ARG_TYPES, args);
            }

            Class literalClass = literalClassByMappingName.get(mapping.getClass().getName());
            if (literalClass != null)
            {
                // Use built-in literal class
                return (SQLExpression)ClassUtils.newInstance(literalClass, LIT_CREATION_ARG_TYPES, args);
            }

            // Fallback to the plugin mechanism
            SQLExpression sqlExpr = (SQLExpression) storeMgr.getNucleusContext().getPluginManager().createExecutableExtension("org.datanucleus.store.rdbms.sql_expression",
                "mapping-class", mapping.getClass().getName(), "literal-class", LIT_CREATION_ARG_TYPES, args);
            if (sqlExpr == null)
            {
                throw new NucleusException(Localiser.msg("060006", mapping.getClass().getName()));
            }
            return sqlExpr;
        }
        catch (Exception e)
        {
            String mappingName = (mapping != null) ? mapping.getClass().getName() : null;
            NucleusLogger.QUERY.error("Exception creating SQLLiteral for mapping " + mappingName, e);
            throw new NucleusException(Localiser.msg("060007", mappingName));
        }
    }

    /**
     * Accessor for the result of an SQLMethod call on the supplied expression with the supplied args.
     * Throws a NucleusException is the method is not supported.
     * @param stmt SQLStatement that this relates to
     * @param className Class we are invoking the method on
     * @param methodName Name of the method
     * @param expr The expression we invoke the method on
     * @param args Any arguments to the method call
     * @return The result
     */
    public SQLExpression invokeMethod(SQLStatement stmt, String className, String methodName, SQLExpression expr, List args)
    {
        SQLMethod method = getMethod(className, methodName, args);
        if (method != null)
        {
            return method.getExpression(stmt, expr, args);
        }
        return null;
    }

    public boolean isMethodRegistered(String className, String methodName)
    {
        if (storeMgr.getDatastoreAdapter().getSQLMethodClass(className, methodName, clr) != null)
        {
            // Built-in SQLMethod available
            return true;
        }

        if (pluginSqlMethodsKeysSupported.contains(getSQLMethodKey(storeMgr.getDatastoreAdapter().getVendorID(), className, methodName)))
        {
            // Plugin mechanism has datastore-dependent SQLMethod
            return true;
        }

        if (pluginSqlMethodsKeysSupported.contains(getSQLMethodKey(null, className, methodName)))
        {
            // Plugin mechanism has datastore-independent SQLMethod
            return true;
        }

        return false;
    }

    /**
     * Method to allow a user to register an SQLMethod at runtime without utilising the plugin mechanism.
     * Will throw a NucleusUserException if this class+method already has an SQLMethod defined.
     * @param className Class name (or null if "static")
     * @param methodName Name of the method/function
     * @param method The SQLMethod to invoke when this method is encountered
     */
    public void registerMethod(String className, String methodName, SQLMethod method)
    {
        if (isMethodRegistered(className, methodName))
        {
            throw new NucleusUserException("SQLMethod already defined for class=" + className + " method=" + methodName);
        }

        // Try to find datastore-dependent evaluator for class+method
        MethodKey methodKey = getSQLMethodKey(storeMgr.getDatastoreAdapter().getVendorID(), className, methodName);

        pluginSqlMethodsKeysSupported.add(methodKey);
        sqlMethodsByKey.put(methodKey, method);
    }

    /**
     * Accessor for the method defined by the class/method names and supplied args.
     * Throws a NucleusException is the method is not supported.
     * Note that if the class name passed in is not for a listed class with that method defined then will check all remaining defined methods for a superclass.
     * @param className Class we are invoking the method on
     * @param methodName Name of the method
     * @param args Any arguments to the method call (ignored currently) TODO Check the arguments
     * @return The method
     */
    protected SQLMethod getMethod(String className, String methodName, List args)
    {
        String datastoreId = storeMgr.getDatastoreAdapter().getVendorID();

        // Try to find datastore-dependent evaluator for class+method
        MethodKey methodKey1 = getSQLMethodKey(datastoreId, className, methodName);
        MethodKey methodKey2 = null;
        SQLMethod method = sqlMethodsByKey.get(methodKey1);
        if (method == null)
        {
            // Try to find datastore-independent evaluator for class+method
            methodKey2 = getSQLMethodKey(null, className, methodName);
            method = sqlMethodsByKey.get(methodKey2);
        }
        if (method != null)
        {
            return method;
        }

        // No existing instance, so check the built-in SQLMethods from DatastoreAdapter
        Class sqlMethodCls = storeMgr.getDatastoreAdapter().getSQLMethodClass(className, methodName, clr);
        if (sqlMethodCls != null)
        {
            // Built-in SQLMethod found, so instantiate it, cache it and return it
            try
            {
                method = (SQLMethod) sqlMethodCls.newInstance();
                MethodKey key = getSQLMethodKey(datastoreId, className, methodName);
                sqlMethodsByKey.put(key, method);
                return method;
            }
            catch (Exception e)
            {
                throw new NucleusException("Error creating SQLMethod of type " + sqlMethodCls.getName() + " for class=" + className + " method=" + methodName);
            }
        }

        // Check the plugin mechanism
        // 1). Try datastore-dependent key
        boolean datastoreDependent = true;
        if (!pluginSqlMethodsKeysSupported.contains(methodKey1))
        {
            // 2). No datastore-dependent method, so try a datastore-independent key
            datastoreDependent = false;
            if (!pluginSqlMethodsKeysSupported.contains(methodKey2))
            {
                // Not listed as supported for this particular class+method, so maybe is for a superclass
                boolean unsupported = true;
                if (!StringUtils.isWhitespace(className))
                {
                    Class cls = clr.classForName(className);

                    // Try datastore-dependent
                    for (MethodKey methodKey : pluginSqlMethodsKeysSupported)
                    {
                        if (methodKey.methodName.equals(methodName) && methodKey.datastoreName.equals(datastoreId))
                        {
                            Class methodCls = null;
                            try
                            {
                                methodCls = clr.classForName(methodKey.clsName);
                            }
                            catch (ClassNotResolvedException cnre)
                            {
                                // Maybe generic array support?
                            }
                            if (methodCls != null && methodCls.isAssignableFrom(cls))
                            {
                                // This one is usable here, for superclass
                                method = sqlMethodsByKey.get(methodKey);
                                if (method != null)
                                {
                                    MethodKey superMethodKey = new MethodKey();
                                    superMethodKey.clsName = className;
                                    superMethodKey.methodName = methodKey.methodName;
                                    superMethodKey.datastoreName = methodKey.datastoreName;
                                    sqlMethodsByKey.put(superMethodKey, method); // Cache the same method under this class also
                                    return method;
                                }

                                className = methodKey.clsName;
                                datastoreId = methodKey.datastoreName;
                                datastoreDependent = true;
                                unsupported = false;
                                break;
                            }
                        }
                    }
                    if (unsupported)
                    {
                        // Try datastore-independent
                        for (MethodKey methodKey : pluginSqlMethodsKeysSupported)
                        {
                            if (methodKey.methodName.equals(methodName) && methodKey.datastoreName.equals("ALL"))
                            {
                                Class methodCls = null;
                                try
                                {
                                    methodCls = clr.classForName(methodKey.clsName);
                                }
                                catch (ClassNotResolvedException cnre)
                                {
                                    // Maybe generic array support?
                                }
                                if (methodCls != null && methodCls.isAssignableFrom(cls))
                                {
                                    // This one is usable here, for superclass
                                    method = sqlMethodsByKey.get(methodKey);
                                    if (method != null)
                                    {
                                        MethodKey superMethodKey = new MethodKey();
                                        superMethodKey.clsName = className;
                                        superMethodKey.methodName = methodKey.methodName;
                                        superMethodKey.datastoreName = methodKey.datastoreName;
                                        sqlMethodsByKey.put(superMethodKey, method); // Cache the same method under this class also
                                        return method;
                                    }

                                    className = methodKey.clsName;
                                    datastoreId = methodKey.datastoreName;
                                    datastoreDependent = false;
                                    unsupported = false;
                                    break;
                                }
                            }
                        }
                    }
                }

                if (unsupported)
                {
                    if (className != null)
                    {
                        throw new NucleusUserException(Localiser.msg("060008", methodName, className));
                    }
                    throw new NucleusUserException(Localiser.msg("060009", methodName));
                }
            }
        }

        // Fallback to plugin lookup of class+method[+datastore]
        PluginManager pluginMgr = storeMgr.getNucleusContext().getPluginManager();
        String[] attrNames = (datastoreDependent ? new String[] {"class", "method", "datastore"} : new String[] {"class", "method"});
        String[] attrValues = (datastoreDependent ? new String[] {className, methodName, datastoreId} : new String[] {className, methodName});
        try
        {
            method = (SQLMethod)pluginMgr.createExecutableExtension("org.datanucleus.store.rdbms.sql_method", attrNames, attrValues, "evaluator", new Class[]{}, new Object[]{});

            // Register the method
            sqlMethodsByKey.put(getSQLMethodKey(datastoreDependent ? datastoreId : null, className, methodName), method);

            return method;
        }
        catch (Exception e)
        {
            throw new NucleusUserException(Localiser.msg("060011", "class=" + className + " method=" + methodName), e);
        }
    }

    /**
     * Convenience method to return the key for the SQL method.
     * Returns a string like <pre>{datastore}#{class}.{method}</pre> if the class is defined, and
     * <pre>{datastore}#{method}</pre> if the class is not defined (function).
     * @param datastoreName Vendor id of the RDBMS datastore
     * @param className Name of the class that we are invoking on (null if static).
     * @param methodName Method to be invoked
     * @return Key for the SQLMethod
     */
    private MethodKey getSQLMethodKey(String datastoreName, String className, String methodName)
    {
        MethodKey key = new MethodKey();
        key.clsName = (className != null ? className.trim() : "");
        key.methodName = methodName;
        key.datastoreName = (datastoreName != null ? datastoreName.trim() : "ALL");
        return key;
    }

    /**
     * Accessor for the result of an SQLOperation call on the supplied expression with the supplied args.
     * Throws a NucleusException is the method is not supported.
     * @param name Operation to be invoked
     * @param expr The first expression to perform the operation on
     * @param expr2 The second expression to perform the operation on
     * @return The result
     * @throws UnsupportedOperationException if the operation is not specified
     */
    public SQLExpression invokeOperation(String name, SQLExpression expr, SQLExpression expr2)
    {
        // Check for instantiated plugin SQLOperation
        DatastoreAdapter dba = storeMgr.getDatastoreAdapter();
        SQLOperation operation = sqlOperationsByName.get(name);
        if (operation != null)
        {
            return operation.getExpression(expr, expr2);
        }

        // Check for built-in SQLOperation class definition
        Class sqlOpClass = dba.getSQLOperationClass(name);
        if (sqlOpClass != null)
        {
            try
            {
                // Instantiate it
                operation = (SQLOperation) sqlOpClass.newInstance();
                sqlOperationsByName.put(name, operation);
                return operation.getExpression(expr, expr2);
            }
            catch (Exception e)
            {
                throw new NucleusException("Error creating SQLOperation of type " + sqlOpClass.getName() + " for operation " + name);
            }
        }

        // Check for plugin definition of this operation for this datastore

        // 1). Try datastore-dependent key
        String datastoreId = dba.getVendorID();
        String key = getSQLOperationKey(datastoreId, name);
        boolean datastoreDependent = true;
        if (!pluginSqlOperationKeysSupported.contains(key))
        {
            // 2). No datastore-dependent method, so try a datastore-independent key
            key = getSQLOperationKey(null, name);
            datastoreDependent = false;
            if (!pluginSqlOperationKeysSupported.contains(key))
            {
                throw new UnsupportedOperationException("Operation " + name + " on datastore=" + datastoreId + " not supported");
            }
        }

        PluginManager pluginMgr = storeMgr.getNucleusContext().getPluginManager();
        String[] attrNames = (datastoreDependent ? new String[] {"name", "datastore"} : new String[] {"name"});
        String[] attrValues = (datastoreDependent ? new String[] {name, datastoreId} : new String[] {name});
        try
        {
            operation = (SQLOperation)pluginMgr.createExecutableExtension("org.datanucleus.store.rdbms.sql_operation", attrNames, attrValues, "evaluator", null, null);
            synchronized (operation)
            {
                sqlOperationsByName.put(key, operation);
                return operation.getExpression(expr, expr2);
            }
        }
        catch (Exception e)
        {
            throw new NucleusUserException(Localiser.msg("060011", "operation=" + name), e);
        }
    }

    /**
     * Convenience method to return the key for the SQL operation.
     * Returns a string like <pre>{datastore}#{operation}</pre>.
     * @param datastoreName Vendor id of the RDBMS datastore
     * @param name Operation to be invoked
     * @return Key for the SQLOperation
     */
    private String getSQLOperationKey(String datastoreName, String name)
    {
        return (datastoreName != null ? datastoreName.trim() : "ALL") + "#" + name;
    }

    /**
     * Accessor for a mapping to use in a query expression.
     * @param cls The class that the mapping should represent.
     * @param useCached Whether to use any cached mapping (if available)
     * @return The mapping
     */
    public JavaTypeMapping getMappingForType(Class cls, boolean useCached)
    {
        JavaTypeMapping mapping = null;
        if (useCached)
        {
            mapping = mappingByClass.get(cls);
            if (mapping != null)
            {
                return mapping;
            }
        }
        mapping = storeMgr.getMappingManager().getMappingWithColumnMapping(cls, false, false, clr);
        mappingByClass.put(cls, mapping);
        return mapping;
    }

    public JavaTypeMapping getMappingForType(Class cls)
    {
        return getMappingForType(cls, true);
    }
}