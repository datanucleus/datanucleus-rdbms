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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.fieldmanager.SingleValueFieldManager;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.datastore.DatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.SingleCollectionMapping;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Utility methods for working with SQL expressions.
 */
public class ExpressionUtils
{
    /**
     * Method to return a numeric expression for the supplied SQL expression.
     * Makes use of the RDBMS function to convert to a numeric.
     * @param expr The expression
     * @return The numeric expression for this SQL expression
     */
    public static NumericExpression getNumericExpression(SQLExpression expr)
    {
        RDBMSStoreManager storeMgr = expr.getSQLStatement().getRDBMSManager();
        SQLExpressionFactory factory = storeMgr.getSQLExpressionFactory();
        DatastoreAdapter dba = expr.getSQLStatement().getDatastoreAdapter();
        if (expr instanceof CharacterLiteral)
        {
            char c = ((Character)((CharacterLiteral)expr).getValue()).charValue();
            BigInteger value = new BigInteger("" + (int)c);
            return (NumericExpression)factory.newLiteral(expr.getSQLStatement(), storeMgr.getMappingManager().getMapping(value.getClass()), value);
        }
        else if (expr instanceof SQLLiteral)
        {
            BigInteger value = new BigInteger((String)((SQLLiteral)expr).getValue());
            return (NumericExpression)factory.newLiteral(expr.getSQLStatement(), storeMgr.getMappingManager().getMapping(value.getClass()), value);
        }

        ArrayList args = new ArrayList();
        args.add(expr);
        return new NumericExpression(expr.getSQLStatement(), expr.getJavaTypeMapping(), dba.getNumericConversionFunction(), args);
    }

    /**
     * Convenience accessor for a literal for the number 1.
     * @param stmt The SQL statement
     * @return The literal
     */
    public static SQLExpression getLiteralForOne(SQLStatement stmt)
    {
        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        JavaTypeMapping mapping = storeMgr.getMappingManager().getMapping(BigInteger.class);
        return storeMgr.getSQLExpressionFactory().newLiteral(stmt, mapping, BigInteger.ONE);
    }

    /**
     * The pattern string for representing one character.
     * Most of databases will use the underscore character.
     * @param patternExpr The expression that represents one character for a matcher/parser in the database
     * @return the pattern string.
     */
    public static SQLExpression getEscapedPatternExpression(SQLExpression patternExpr)
    {
        if (patternExpr instanceof StringLiteral)
        {
            String value = (String) ((StringLiteral)patternExpr).getValue();
            SQLExpressionFactory exprFactory = patternExpr.getSQLStatement().getSQLExpressionFactory();
            JavaTypeMapping m = exprFactory.getMappingForType(String.class, false);
            if (value != null)
            {
                value = value.replace("\\","\\\\").replace("%","\\%").replace("_","\\_");
            }
            return exprFactory.newLiteral(patternExpr.getSQLStatement(), m, value);
        }

        return patternExpr;
    }

    /**
     * Convenience method to populate PK mappings/values allowing for recursion where a PK field is itself
     * a PCMapping, that itself has PK mappings, which in turn may include PCMappings.
     * The pkMappings/pkFieldValues arrays are already created and we populate from "position".
     * @param pkMappings The array of pk mappings to be populated
     * @param pkFieldValues The array of pk field values to be populated
     * @param position The current position needing populating
     * @param pcMapping The PC mapping we are processing
     * @param cmd ClassMetaData for the owning class with this PCMapping field
     * @param mmd Field metadata for the field that this PCMapping represents
     * @param fieldValue The value for the PCMapping field in the owning object
     * @param storeMgr Store Manager
     * @param clr ClassLoader resolver
     * @return The current position (after our processing)
     */
    public static int populatePrimaryKeyMappingsValuesForPCMapping(JavaTypeMapping[] pkMappings, Object[] pkFieldValues, int position, 
            PersistableMapping pcMapping, AbstractClassMetaData cmd, AbstractMemberMetaData mmd, Object fieldValue, RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        ExecutionContext ec = storeMgr.getApiAdapter().getExecutionContext(fieldValue);
        JavaTypeMapping[] subMappings = pcMapping.getJavaTypeMapping();
        if (subMappings.length == 0)
        {
            // Embedded PC has no PK so must be embedded-only so use mapping from owner table
            DatastoreClass table = storeMgr.getDatastoreClass(cmd.getFullClassName(), clr);
            JavaTypeMapping ownerMapping = table.getMemberMapping(mmd);
            EmbeddedMapping embMapping = (EmbeddedMapping)ownerMapping;
            for (int k=0;k<embMapping.getNumberOfJavaTypeMappings();k++)
            {
                JavaTypeMapping subMapping = embMapping.getJavaTypeMapping(k);
                pkMappings[position] = subMapping;
                pkFieldValues[position] = getValueForMemberOfObject(ec, subMapping.getMemberMetaData(), fieldValue);
                position++;
            }
        }
        else
        {
            AbstractClassMetaData pcCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(pcMapping.getType(), clr);
            int[] pcPkPositions = pcCmd.getPKMemberPositions();
            for (int k=0;k<subMappings.length;k++)
            {
                AbstractMemberMetaData pcMmd = pcCmd.getMetaDataForManagedMemberAtAbsolutePosition(pcPkPositions[k]);
                if (subMappings[k] instanceof PersistableMapping)
                {
                    Object val = getValueForMemberOfObject(ec, pcMmd, fieldValue);
                    position = populatePrimaryKeyMappingsValuesForPCMapping(pkMappings, pkFieldValues, position, 
                        (PersistableMapping)subMappings[k], pcCmd, pcMmd, val, storeMgr, clr);
                }
                else
                {
                    Object val = getValueForMemberOfObject(ec, pcMmd, fieldValue);
                    pkMappings[position] = subMappings[k];
                    pkFieldValues[position] = val;
                    position++;
                }
            }
        }
        return position;
    }

    /**
     * Get the value of a managed field/property in the provided object.
     * @param ec execution context
     * @param mmd metadata for the field/property
     * @param object the pc object
     * @return The field value
     */
    public static Object getValueForMemberOfObject(ExecutionContext ec, AbstractMemberMetaData mmd, Object object)
    {
        if (ec == null)
        {
            // Transient or detached maybe
            return ClassUtils.getValueOfFieldByReflection(object, mmd.getName());
            // TODO What if this is a property?
        }

        ObjectProvider sm = ec.findObjectProvider(object);
        if (!mmd.isPrimaryKey())
        {
            // Make sure the field is loaded
            sm.isLoaded(mmd.getAbsoluteFieldNumber());
        }

        FieldManager fm = new SingleValueFieldManager();
        sm.provideFields(new int[] {mmd.getAbsoluteFieldNumber()}, fm);
        return fm.fetchObjectField(mmd.getAbsoluteFieldNumber());
    }

    /**
     * Create an equality expression "(expr == id)" for an application identity using reflection 
     * to retrieve values and generate the mappings.
     * @param id the identity to compare against
     * @param expr the object expression
     * @param storeMgr the StoreManager
     * @param clr the ClassLoaderResolver
     * @param acmd MetaData for the class the object id represents
     * @param index the current index in the source expression (internal)
     * @param bExpr the boolean equals expression (internal)
     * @return the equality expression
     */
    public static BooleanExpression getAppIdEqualityExpression(Object id, SQLExpression expr,
            RDBMSStoreManager storeMgr, ClassLoaderResolver clr, AbstractClassMetaData acmd, Integer index, BooleanExpression bExpr)
    {
        if (index == null)
        {
            index = Integer.valueOf(0);
        }

        String[] pkFieldNames = acmd.getPrimaryKeyMemberNames();
        for (int i=0;i<pkFieldNames.length;i++)
        {
            Object value = ClassUtils.getValueOfFieldByReflection(id, pkFieldNames[i]);
            String pcClassName = storeMgr.getClassNameForObjectID(value, clr, null);
            if (pcClassName != null)
            {
                // This part is the id of a PC class (compound identity), so recurse
                AbstractClassMetaData scmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(pcClassName, clr);
                if (bExpr == null)
                {
                    bExpr = getAppIdEqualityExpression(value, expr, storeMgr, clr, scmd, index, null);
                }
                else
                {
                    bExpr = bExpr.and(getAppIdEqualityExpression(value, expr, storeMgr, clr, scmd, index, bExpr));
                }
            }
            else
            {
                //if a simple value, we simply apply the equals
                SQLExpression source = expr.subExprs.getExpression(index);
                JavaTypeMapping mapping = storeMgr.getMappingManager().getMappingWithDatastoreMapping(value.getClass(), false, false, clr);
                SQLExpression target = expr.getSQLStatement().getSQLExpressionFactory().newLiteral(expr.getSQLStatement(), mapping, value);
                if (bExpr == null)
                {
                    bExpr = source.eq(target);
                }
                else
                {
                    bExpr = bExpr.and(source.eq(target));
                }

                if (target.subExprs.size() == 0)
                {
                    index++;
                }
                else
                {
                    index += target.subExprs.size();
                }
            }
        }
        return bExpr;
    }

    /**
     * Method to generate an equality/inequality expression between two ObjectExpressions.
     * Either or both of the expressions can be ObjectLiterals.
     * @param expr1 First expression
     * @param expr2 Second expression
     * @param equals Whether it is equality (otherwise inequality)
     * @return The expression
     */
    public static BooleanExpression getEqualityExpressionForObjectExpressions(ObjectExpression expr1, 
            ObjectExpression expr2, boolean equals)
    {
        SQLStatement stmt = expr1.stmt;
        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        ClassLoaderResolver clr = stmt.getClassLoaderResolver();
        ApiAdapter api = storeMgr.getApiAdapter();
        if (expr1 instanceof ObjectLiteral && expr2 instanceof ObjectLiteral)
        {
            // ObjectLiterall == ObjectLiteral
            ObjectLiteral lit1 = (ObjectLiteral)expr1;
            ObjectLiteral lit2 = (ObjectLiteral)expr2;
            return new BooleanLiteral(stmt, expr1.mapping, equals ? lit1.getValue().equals(lit2.getValue()) : !lit1.getValue().equals(lit2.getValue()));
        }
        else if (expr1 instanceof ObjectLiteral || expr2 instanceof ObjectLiteral)
        {
            // ObjectExpression == ObjectLiteral, ObjectLiteral == ObjectExpression
            BooleanExpression bExpr = null;
            boolean secondIsLiteral = (expr2 instanceof ObjectLiteral);
            Object value = (!secondIsLiteral ? ((ObjectLiteral)expr1).getValue() : ((ObjectLiteral)expr2).getValue());
            if (IdentityUtils.isDatastoreIdentity(value))
            {
                // Object is an OID
                Object valueKey = IdentityUtils.getTargetKeyForDatastoreIdentity(value);
                JavaTypeMapping m = storeMgr.getSQLExpressionFactory().getMappingForType(valueKey.getClass(), false);
                SQLExpression oidLit = exprFactory.newLiteral(stmt, m, valueKey);
                if (equals)
                {
                    return (secondIsLiteral ? expr1.subExprs.getExpression(0).eq(oidLit) : expr2.subExprs.getExpression(0).eq(oidLit));
                }
                return (secondIsLiteral ? expr1.subExprs.getExpression(0).ne(oidLit) : expr2.subExprs.getExpression(0).ne(oidLit));
            }
            else if (IdentityUtils.isSingleFieldIdentity(value))
            {
                // Object is SingleFieldIdentity
                Object valueKey = IdentityUtils.getTargetKeyForSingleFieldIdentity(value);
                // This used to use ((SingleFieldId)value).getTargetClass() for some reason, which contradicts the above datastore id method
                JavaTypeMapping m = storeMgr.getSQLExpressionFactory().getMappingForType(valueKey.getClass(), false);
                SQLExpression oidLit = exprFactory.newLiteral(stmt, m, valueKey);
                if (equals)
                {
                    return (secondIsLiteral ? expr1.subExprs.getExpression(0).eq(oidLit) : expr2.subExprs.getExpression(0).eq(oidLit));
                }
                return (secondIsLiteral ? expr1.subExprs.getExpression(0).ne(oidLit) : expr2.subExprs.getExpression(0).ne(oidLit));
            }
            else
            {
                AbstractClassMetaData cmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(value.getClass(), clr);
                if (cmd != null)
                {
                    // Value is a persistable object
                    if (cmd.getIdentityType() == IdentityType.APPLICATION)
                    {
                        // Application identity
                        if (api.getIdForObject(value) != null)
                        {
                            // Persistent PC object (FCO)
                            // Cater for composite PKs and parts of PK being PC mappings, and recursion
                            ObjectExpression expr = (secondIsLiteral ? expr1 : expr2);
                            JavaTypeMapping[] pkMappingsApp = new JavaTypeMapping[expr.subExprs.size()];
                            Object[] pkFieldValues = new Object[expr.subExprs.size()];
                            int position = 0;
                            ExecutionContext ec = api.getExecutionContext(value);
                            JavaTypeMapping thisMapping = expr.mapping;
                            if (expr.mapping instanceof ReferenceMapping)
                            {
                                // "InterfaceField == value", so pick an implementation mapping that is castable
                                thisMapping = null;
                                ReferenceMapping refMapping = (ReferenceMapping)expr.mapping;
                                JavaTypeMapping[] implMappings = refMapping.getJavaTypeMapping();
                                for (int i=0;i<implMappings.length;i++)
                                {
                                    Class implType = clr.classForName(implMappings[i].getType());
                                    if (implType.isAssignableFrom(value.getClass()))
                                    {
                                        thisMapping = implMappings[i];
                                        break;
                                    }
                                }
                            }
                            if (thisMapping == null)
                            {
                                // Just return a (1=0) since no implementation castable
                                return exprFactory.newLiteral(stmt, expr1.mapping, false).eq(exprFactory.newLiteral(stmt, expr1.mapping, true));
                            }

                            for (int i=0;i<cmd.getNoOfPrimaryKeyMembers();i++)
                            {
                                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(cmd.getPKMemberPositions()[i]);
                                Object fieldValue = ExpressionUtils.getValueForMemberOfObject(ec, mmd, value);
                                JavaTypeMapping mapping = ((PersistableMapping)thisMapping).getJavaTypeMapping()[i];
                                if (mapping instanceof PersistableMapping)
                                {
                                    position = ExpressionUtils.populatePrimaryKeyMappingsValuesForPCMapping(pkMappingsApp, 
                                        pkFieldValues, position, (PersistableMapping)mapping, cmd, mmd, fieldValue, storeMgr, clr);
                                }
                                else
                                {
                                    pkMappingsApp[position] = mapping;
                                    pkFieldValues[position] = fieldValue;
                                    position++;
                                }
                            }

                            for (int i=0; i<expr.subExprs.size(); i++)
                            {
                                SQLExpression source = expr.subExprs.getExpression(i);
                                SQLExpression target = exprFactory.newLiteral(stmt, pkMappingsApp[i], pkFieldValues[i]);
                                BooleanExpression subExpr = (secondIsLiteral ? source.eq(target) : target.eq(source));
                                if (bExpr == null)
                                {
                                    bExpr = subExpr;
                                }
                                else
                                {
                                    bExpr = bExpr.and(subExpr);
                                }
                            }
                        }
                        else
                        {
                            // PC object with no id (embedded, or transient maybe)
                            if (secondIsLiteral)
                            {
                                for (int i=0; i<expr1.subExprs.size(); i++)
                                {
                                    // Query should return nothing (so just do "(1 = 0)")
                                    NucleusLogger.QUERY.warn(Localiser.msg("037003", value));
                                    bExpr = exprFactory.newLiteral(stmt, expr1.mapping, false).eq(exprFactory.newLiteral(stmt, expr1.mapping, true));
                                    // It is arguable that we should compare the id with null (as below)
                                    /*bExpr = expr.eq(new NullLiteral(qs));*/
                                }
                            }
                            else
                            {
                                for (int i=0; i<expr2.subExprs.size(); i++)
                                {
                                    // Query should return nothing (so just do "(1 = 0)")
                                    NucleusLogger.QUERY.warn(Localiser.msg("037003", value));
                                    bExpr = exprFactory.newLiteral(stmt, expr2.mapping, false).eq(exprFactory.newLiteral(stmt, expr2.mapping, true));
                                    // It is arguable that we should compare the id with null (as below)
                                    /*bExpr = expr.eq(new NullLiteral(qs));*/
                                }
                            }
                        }
                        // TODO Allow for !equals
                        return bExpr;
                    }
                    else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        // Datastore identity
                        SQLExpression source = (secondIsLiteral ? expr1.subExprs.getExpression(0) : expr2.subExprs.getExpression(0));
                        JavaTypeMapping mapping = (secondIsLiteral ? expr1.mapping : expr2.mapping);
                        Object objectId = api.getIdForObject(value);
                        if (objectId == null)
                        {
                            // PC object with no id (embedded, or transient maybe)
                            // Query should return nothing (so just do "(1 = 0)")
                            NucleusLogger.QUERY.warn(Localiser.msg("037003", value));
                            // TODO Allow for !equals
                            return exprFactory.newLiteral(stmt, mapping, false).eq(exprFactory.newLiteral(stmt, mapping, true));
                            // It is arguable that we should compare the id with null (as below)
                            /*bExpr = expr.eq(new NullLiteral(qs));*/
                        }

                        Object objectIdKey = IdentityUtils.getTargetKeyForDatastoreIdentity(objectId);
                        JavaTypeMapping m = storeMgr.getSQLExpressionFactory().getMappingForType(objectIdKey.getClass(), false);
                        SQLExpression oidExpr = exprFactory.newLiteral(stmt, m, objectIdKey);
                        if (equals)
                        {
                            return source.eq(oidExpr);
                        }
                        return source.ne(oidExpr);
                    }
                }
                else
                {
                    // No metadata, so we either have an application identity, or any object
                    String pcClassName = storeMgr.getClassNameForObjectID(value, clr, null);
                    if (pcClassName != null)
                    {
                        // Object is an application identity
                        cmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(pcClassName, clr);
                        return (secondIsLiteral ?
                            ExpressionUtils.getAppIdEqualityExpression(value, expr1, storeMgr, clr, cmd, null, null) :
                            ExpressionUtils.getAppIdEqualityExpression(value, expr2, storeMgr, clr, cmd, null, null));
                        // TODO Allow for !equals
                    }
                    // Value not persistable nor an identity, so return nothing "(1 = 0)"
                    return exprFactory.newLiteral(stmt, expr1.mapping, false).eq(exprFactory.newLiteral(stmt, expr1.mapping, true));
                    // TODO Allow for !equals
                }
            }
        }
        else
        {
            // ObjectExpression == ObjectExpression
            BooleanExpression resultExpr = null;
            for (int i=0;i<expr1.subExprs.size();i++)
            {
                SQLExpression sourceExpr = expr1.subExprs.getExpression(i);
                SQLExpression targetExpr = expr2.subExprs.getExpression(i);
                if (resultExpr == null)
                {
                    resultExpr = sourceExpr.eq(targetExpr);
                }
                else
                {
                    resultExpr = resultExpr.and(sourceExpr.eq(targetExpr));
                }
            }
            if (!equals)
            {
                resultExpr = new BooleanExpression(Expression.OP_NOT, resultExpr.encloseInParentheses());
            }
            return resultExpr;
        }
        return null;
    }

    /**
     * Convenience method that compares the mappings used by the two expressions for compatibility for
     * use in a boolean comparison (eq, noteq, gt, gteq, lt, lteq) and, if necessary, updates the mapping
     * if one of them is a SQLLiteral and is deemed inconsistent with the other expression.
     * Additionally, if both sides of the comparison are parameters, this will swap one to be its literal value.
     * @param expr1 First expression
     * @param expr2 Second expression
     */
    public static void checkAndCorrectExpressionMappingsForBooleanComparison(SQLExpression expr1, SQLExpression expr2)
    {
        if (expr1.isParameter() && expr2.isParameter())
        {
            // If we have comparison of two parameters, swap one to be its (literal) value
            if (expr1 instanceof SQLLiteral && ((SQLLiteral)expr2).getValue() != null)
            {
                expr1.getSQLStatement().getQueryGenerator().useParameterExpressionAsLiteral((SQLLiteral)expr2);
            }
            else if (expr2 instanceof SQLLiteral && ((SQLLiteral)expr2).getValue() != null)
            {
                expr1.getSQLStatement().getQueryGenerator().useParameterExpressionAsLiteral((SQLLiteral)expr2);
            }
        }

        if (expr1 instanceof SQLLiteral)
        {
            checkAndCorrectLiteralForConsistentMappingsForBooleanComparison((SQLLiteral)expr1, expr2);
        }
        else if (expr2 instanceof SQLLiteral)
        {
            checkAndCorrectLiteralForConsistentMappingsForBooleanComparison((SQLLiteral)expr2, expr1);
        }
    }

    protected static void checkAndCorrectLiteralForConsistentMappingsForBooleanComparison(SQLLiteral lit, SQLExpression expr)
    {
        JavaTypeMapping litMapping = ((SQLExpression)lit).getJavaTypeMapping();
        JavaTypeMapping exprMapping = expr.getJavaTypeMapping();
        if (exprMapping == null || exprMapping.getNumberOfDatastoreMappings() == 0)
        {
            return;
        }
        if (litMapping instanceof PersistableMapping && exprMapping instanceof ReferenceMapping)
        {
            // Can compare implementation with reference
            return;
        }
        if (litMapping instanceof SingleCollectionMapping)
        {
            return;
        }

        boolean needsUpdating = false;
        if (litMapping.getNumberOfDatastoreMappings() != exprMapping.getNumberOfDatastoreMappings())
        {
            needsUpdating = true;
        }
        else
        {
            for (int i=0;i<litMapping.getNumberOfDatastoreMappings();i++)
            {
                DatastoreMapping colMapping = litMapping.getDatastoreMapping(i);
                if (colMapping == null || colMapping.getClass() != exprMapping.getDatastoreMapping(i).getClass())
                {
                    needsUpdating = true;
                    break;
                }
            }
        }

        if (needsUpdating)
        {
            // Make sure a change in mapping makes sense
            // This embeds some type conversion rules and would be nice to avoid it
            Class litMappingCls = litMapping.getJavaType();
            Class mappingCls = exprMapping.getJavaType();
            if (litMappingCls == Double.class || litMappingCls == Float.class || litMappingCls == BigDecimal.class)
            {
                // Comparison between integral, and floating point parameter, so don't convert the param mapping
                if (mappingCls == Integer.class || mappingCls == Long.class || mappingCls == Short.class || mappingCls == BigInteger.class || mappingCls == Byte.class)
                {
                    if (litMappingCls == BigDecimal.class)
                    {
                        // BigDecimal seems to need to have the value put into SQL directly 
                        // (see JDO TCK "Equality" test when comparing with integer-based field).
                        expr.getSQLStatement().getQueryGenerator().useParameterExpressionAsLiteral(lit);
                    }
                    needsUpdating = false;
                }
            }
            if (litMappingCls == Byte.class && mappingCls != Byte.class)
            {
                needsUpdating = false;
            }
        }
        if (needsUpdating)
        {
            NucleusLogger.QUERY.debug("Updating mapping of " + lit + " to be " + expr.getJavaTypeMapping());
            ((SQLExpression)lit).setJavaTypeMapping(expr.getJavaTypeMapping());
        }
    }
}