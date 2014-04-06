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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.plugin.ConfigurationElement;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
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
    /** Localiser for messages */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.rdbms.Localisation", RDBMSStoreManager.class.getClassLoader());

    RDBMSStoreManager storeMgr;

    ClassLoaderResolver clr;

    private final Class[] EXPR_CREATION_ARG_TYPES = 
        new Class[] {SQLStatement.class, SQLTable.class, JavaTypeMapping.class};

    private final Class[] LIT_CREATION_ARG_TYPES = 
        new Class[] {SQLStatement.class, JavaTypeMapping.class, Object.class, String.class};

    /** Cache of expression class, keyed by the mapping class name. */
    Map<String, Class> expressionClassByMappingName = new HashMap();

    /** Cache of literal class, keyed by the mapping class name. */
    Map<String, Class> literalClassByMappingName = new HashMap();

    /** Names of methods that are supported. */
    Set<MethodKey> methodNamesSupported = new HashSet<MethodKey>();

    /** Cache of already created SQLMethod instances, keyed by their class+method[+datastore] name. */
    Map<MethodKey, SQLMethod> methodByClassMethodName = new HashMap<MethodKey, SQLMethod>();

    /** Names of operations that are supported. */
    Set<String> operationNamesSupported = new HashSet<String>();

    /** Cache of already created SQLOperation instances, keyed by their name. */
    Map<String, SQLOperation> operationByOperationName = new HashMap<String, SQLOperation>();

    /** Map of JavaTypeMapping for use in query expressions, keyed by the type being represented. */
    Map<Class, JavaTypeMapping> mappingByClass = new HashMap<Class, JavaTypeMapping>();

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
            return (otherKey.clsName.equals(clsName) && otherKey.methodName.equals(methodName) &&
                otherKey.datastoreName.equals(datastoreName));
        }
    }

    /**
     * Constructor for an SQLExpressionFactory.
     * Also loads up the defined SQL methods
     * [extension-point: "org.datanucleus.store.rdbms.sql_method"] and caches them.
     * Also loads up the defined SQL operations
     * [extension-point: "org.datanucleus.store.rdbms.sql_operation"] and caches them.
     * @param storeMgr RDBMS Manager
     */
    public SQLExpressionFactory(RDBMSStoreManager storeMgr)
    {
        this.storeMgr = storeMgr;
        this.clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);

        PluginManager pluginMgr = storeMgr.getNucleusContext().getPluginManager();
        ConfigurationElement[] methodElems = 
            pluginMgr.getConfigurationElementsForExtension("org.datanucleus.store.rdbms.sql_method", null, null);
        if (methodElems != null)
        {
            for (int i=0;i<methodElems.length;i++)
            {
                String datastoreName = methodElems[i].getAttribute("datastore");
                String className = methodElems[i].getAttribute("class");
                String methodName = methodElems[i].getAttribute("method").trim();
                MethodKey key = getSQLMethodKey(datastoreName, className, methodName);
                methodNamesSupported.add(key);
            }
        }

        ConfigurationElement[] operationElems = 
            pluginMgr.getConfigurationElementsForExtension("org.datanucleus.store.rdbms.sql_operation", null, null);
        if (operationElems != null)
        {
            for (int i=0;i<operationElems.length;i++)
            {
                String datastoreName = operationElems[i].getAttribute("datastore");
                String name = operationElems[i].getAttribute("name").trim();
                String key = getSQLOperationKey(datastoreName, name);
                operationNamesSupported.add(key);
            }
        }
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
     * @param parentMapping Optional parent mapping of this mapping (e.g when handling impl of an interface)
     * @return The expression
     */
    public SQLExpression newExpression(SQLStatement stmt, SQLTable sqlTbl, JavaTypeMapping mapping,
            JavaTypeMapping parentMapping)
    {
        SQLTable exprSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(stmt, sqlTbl, 
            parentMapping == null ? mapping : parentMapping);
        Object[] args = new Object[] {stmt, exprSqlTbl, mapping};

        Class expressionClass = expressionClassByMappingName.get(mapping.getClass().getName());
        if (expressionClass != null)
        {
            // Use cached expression class
            return (SQLExpression)ClassUtils.newInstance(expressionClass, EXPR_CREATION_ARG_TYPES, 
                new Object[] {stmt, exprSqlTbl, mapping});
        }

        try
        {
            // Use "new SQLExpression(SQLStatement, SQLTable, JavaTypeMapping)"
            SQLExpression sqlExpr = (SQLExpression)storeMgr.getNucleusContext().getPluginManager().createExecutableExtension(
                "org.datanucleus.store.rdbms.sql_expression", "mapping-class", mapping.getClass().getName(), 
                "expression-class", EXPR_CREATION_ARG_TYPES, args);
            if (sqlExpr == null)
            {
                throw new NucleusException(LOCALISER.msg("060004", mapping.getClass().getName()));
            }

            expressionClassByMappingName.put(mapping.getClass().getName(), sqlExpr.getClass());
            return sqlExpr;
        }
        catch (Exception e)
        {
            String msg = LOCALISER.msg("060005", mapping.getClass().getName());
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
            // Use "new SQLLiteral(SQLStatement, JavaTypeMapping, Object, String)"
            if (mapping == null)
            {
                return (SQLExpression)ClassUtils.newInstance(NullLiteral.class, LIT_CREATION_ARG_TYPES, args);
            }
            else
            {
                SQLExpression sqlExpr = (SQLExpression) storeMgr.getNucleusContext().getPluginManager().createExecutableExtension(
                    "org.datanucleus.store.rdbms.sql_expression", "mapping-class", mapping.getClass().getName(), 
                    "literal-class", LIT_CREATION_ARG_TYPES, args);
                if (sqlExpr == null)
                {
                    throw new NucleusException(LOCALISER.msg("060006", mapping.getClass().getName()));
                }

                literalClassByMappingName.put(mapping.getClass().getName(), sqlExpr.getClass());
                return sqlExpr;
            }
        }
        catch (Exception e)
        {
            NucleusLogger.QUERY.error("Exception creating SQLLiteral for mapping " + mapping.getClass().getName(), e);
            throw new NucleusException(LOCALISER.msg("060007", mapping.getClass().getName()));
        }
    }

    /**
     * Factory for a literal as an input parameter.
     * If the mapping (type of parameter) is not known at this point then put in null and it
     * will return a ParameterLiteral.
     * @param stmt The statement
     * @param mapping The mapping
     * @param value Value of the literal (if known)
     * @param paramName The parameter name
     * @return The literal
     */
    public SQLExpression newLiteralParameter(SQLStatement stmt, JavaTypeMapping mapping, Object value, String paramName)
    {
        try
        {
            // Use "new SQLLiteral(SQLStatement, JavaTypeMapping, Object, String)"
            Object[] args = new Object[] {stmt, mapping, value, paramName};
            if (mapping == null)
            {
                return (SQLExpression)ClassUtils.newInstance(ParameterLiteral.class, LIT_CREATION_ARG_TYPES, args);
            }
            else
            {
                SQLExpression sqlExpr = (SQLExpression) storeMgr.getNucleusContext().getPluginManager().createExecutableExtension(
                    "org.datanucleus.store.rdbms.sql_expression", "mapping-class", mapping.getClass().getName(), 
                    "literal-class", LIT_CREATION_ARG_TYPES, args);
                if (sqlExpr == null)
                {
                    throw new NucleusException(LOCALISER.msg("060006", mapping.getClass().getName()));
                }
                return sqlExpr;
            }
        }
        catch (Exception e)
        {
            NucleusLogger.QUERY.error("Exception creating SQLLiteral for mapping " + mapping.getClass().getName(), e);
            throw new NucleusException(LOCALISER.msg("060007", mapping.getClass().getName()));
        }
    }

    /**
     * Accessor for the result of a method call on the supplied expression with the supplied args.
     * Throws a NucleusException is the method is not supported.
     * Note that if the class name passed in is not for a listed class with that method defined then
     * will check all remaining defined methods for a superclass. TODO Make more efficient lookups
     * @param stmt SQLStatement that this relates to
     * @param className Class we are invoking the method on
     * @param methodName Name of the method
     * @param expr The expression we invoke the method on
     * @param args Any arguments to the method call
     * @return The result
     */
    public SQLExpression invokeMethod(SQLStatement stmt, String className, String methodName, SQLExpression expr, List args)
    {
        String datastoreId = storeMgr.getDatastoreAdapter().getVendorID();

        // Try to find datastore-dependent evaluator for class+method
        MethodKey methodKey1 = getSQLMethodKey(datastoreId, className, methodName);
        MethodKey methodKey2 = null;
        SQLMethod method = methodByClassMethodName.get(methodKey1);
        if (method == null)
        {
            // Try to find datastore-independent evaluator for class+method
            methodKey2 = getSQLMethodKey(null, className, methodName);
            method = methodByClassMethodName.get(methodKey2);
        }
        if (method != null)
        {
            // Reuse method, setting statement for this usage
            synchronized (method) // Only permit sole usage at any time
            {
                method.setStatement(stmt);
                return method.getExpression(expr, args);
            }
        }

        // No cached instance of the SQLMethod so find and instantiate it
        // 1). Try datastore-dependent key
        boolean datastoreDependent = true;
        if (!methodNamesSupported.contains(methodKey1))
        {
            // 2). No datastore-dependent method, so try a datastore-independent key
            datastoreDependent = false;
            if (!methodNamesSupported.contains(methodKey2))
            {
                // Not listed as supported for this particular class+method, so maybe is for a superclass
                boolean unsupported = true;
                if (!StringUtils.isWhitespace(className))
                {
                    Class cls = clr.classForName(className);

                    // Try datastore-dependent
                    for (MethodKey methodKey : methodNamesSupported)
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
                                method = methodByClassMethodName.get(methodKey);
                                if (method != null)
                                {
                                    // Reuse method, setting statement for this usage
                                    synchronized (method) // Only permit sole usage at any time
                                    {
                                        method.setStatement(stmt);
                                        return method.getExpression(expr, args);
                                    }
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
                        for (MethodKey methodKey : methodNamesSupported)
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
                                    method = methodByClassMethodName.get(methodKey);
                                    if (method != null)
                                    {
                                        // Reuse method, setting statement for this usage
                                        synchronized (method) // Only permit sole usage at any time
                                        {
                                            method.setStatement(stmt);
                                            return method.getExpression(expr, args);
                                        }
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
                        throw new NucleusUserException(LOCALISER.msg("060008", methodName, className));
                    }
                    else
                    {
                        throw new NucleusUserException(LOCALISER.msg("060009", methodName));
                    }
                }
            }
        }

        PluginManager pluginMgr = storeMgr.getNucleusContext().getPluginManager();
        String[] attrNames = 
            (datastoreDependent ? new String[] {"class", "method", "datastore"} : new String[] {"class", "method"});
        String[] attrValues =
            (datastoreDependent ? new String[] {className, methodName, datastoreId} : new String[] {className, methodName});
        try
        {
            // Use SQLMethod().getExpression(SQLExpression, args)
            SQLMethod evaluator = (SQLMethod)pluginMgr.createExecutableExtension("org.datanucleus.store.rdbms.sql_method", 
                attrNames, attrValues, "evaluator", new Class[]{}, new Object[]{});
            evaluator.setStatement(stmt);
            MethodKey key = getSQLMethodKey(datastoreDependent ? datastoreId : null, className, methodName);
            synchronized (evaluator)
            {
                methodByClassMethodName.put(key, evaluator);
                return evaluator.getExpression(expr, args);
            }
        }
        catch (Exception e)
        {
            throw new NucleusUserException(LOCALISER.msg("060011", "class=" + className + " method=" + methodName), e);
        }
    }

    /**
     * Accessor for the result of an operation call on the supplied expression with the supplied args.
     * Throws a NucleusException is the method is not supported.
     * @param name Operation to be invoked
     * @param expr The first expression to perform the operation on
     * @param expr2 The second expression to perform the operation on
     * @return The result
     * @throws UnsupportedOperationException if the operation is not specified
     */
    public SQLExpression invokeOperation(String name, SQLExpression expr, SQLExpression expr2)
    {
        // Try to find an instance of the SQLOperation
        SQLOperation operation = operationByOperationName.get(name);
        if (operation != null)
        {
            return operation.getExpression(expr, expr2);
        }

        // No cached instance of the SQLMethod so find and instantiate it
        // 1). Try datastore-dependent key
        String datastoreId = storeMgr.getDatastoreAdapter().getVendorID();
        String key = getSQLOperationKey(datastoreId, name);
        boolean datastoreDependent = true;
        if (!operationNamesSupported.contains(key))
        {
            // 2). No datastore-dependent method, so try a datastore-independent key
            key = getSQLOperationKey(null, name);
            datastoreDependent = false;
            if (!operationNamesSupported.contains(key))
            {
                throw new UnsupportedOperationException("Operation " + name + " datastore=" + datastoreId + " not supported");
            }
        }

        PluginManager pluginMgr = storeMgr.getNucleusContext().getPluginManager();
        String[] attrNames = 
            (datastoreDependent ? new String[] {"name", "datastore"} : new String[] {"name"});
        String[] attrValues =
            (datastoreDependent ? new String[] {name, datastoreId} : new String[] {name});
        try
        {
            // Use SQLOperation().getExpression(SQLExpression, SQLExpression)
            operation = (SQLOperation)pluginMgr.createExecutableExtension("org.datanucleus.store.rdbms.sql_operation", 
                attrNames, attrValues, "evaluator", null, null);
            operation.setExpressionFactory(this);
            synchronized (operation)
            {
                operationByOperationName.put(key, operation);
                return operation.getExpression(expr, expr2);
            }
        }
        catch (Exception e)
        {
            throw new NucleusUserException(LOCALISER.msg("060011", "operation=" + name), e);
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
        mapping = storeMgr.getMappingManager().getMappingWithDatastoreMapping(cls, false, false, clr);
        mappingByClass.put(cls, mapping);
        return mapping;
    }
}