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

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.rdbms.mapping.java.BigDecimalMapping;
import org.datanucleus.store.rdbms.mapping.java.BigIntegerMapping;
import org.datanucleus.store.rdbms.mapping.java.BooleanMapping;
import org.datanucleus.store.rdbms.mapping.java.ByteMapping;
import org.datanucleus.store.rdbms.mapping.java.CharacterMapping;
import org.datanucleus.store.rdbms.mapping.java.DateMapping;
import org.datanucleus.store.rdbms.mapping.java.DiscriminatorMapping;
import org.datanucleus.store.rdbms.mapping.java.DoubleMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedMapping;
import org.datanucleus.store.rdbms.mapping.java.FloatMapping;
import org.datanucleus.store.rdbms.mapping.java.IntegerMapping;
import org.datanucleus.store.rdbms.mapping.java.InterfaceMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.LongMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableIdMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.ShortMapping;
import org.datanucleus.store.rdbms.mapping.java.SqlDateMapping;
import org.datanucleus.store.rdbms.mapping.java.SqlTimeMapping;
import org.datanucleus.store.rdbms.mapping.java.SqlTimestampMapping;
import org.datanucleus.store.rdbms.mapping.java.StringMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Representation of an Object expression in a Query. Typically represents a persistable object,
 * and so its identity, though could be used to represent any Object.
 * <p>
 * Let's take an example. We have classes A and B, and A contains a reference to B "b".
 * If we do a JDOQL query for class A of "b == value" then "b" is interpreted first 
 * and an ObjectExpression is created to represent that object (of type B).
 * </p>
 */
public class ObjectExpression extends SQLExpression
{
    /**
     * Constructor for an SQL expression for a (field) mapping in a specified table.
     * @param stmt The statement
     * @param table The table in the statement
     * @param mapping The mapping for the field
     */
    public ObjectExpression(SQLStatement stmt, SQLTable table, JavaTypeMapping mapping)
    {
        super(stmt, table, mapping);
    }

    /**
     * Generates statement as "FUNCTION_NAME(arg [,argN])".
     * @param stmt The statement
     * @param mapping Mapping to use
     * @param functionName Name of function
     * @param args SQLExpression list
     */
    public ObjectExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List args)
    {
        super(stmt, mapping, functionName, args, null);
    }

    /**
     * Generates statement as "FUNCTION_NAME(arg [AS type] [,argN [AS typeN]])".
     * @param stmt The statement
     * @param mapping Mapping to use
     * @param functionName Name of function
     * @param args SQLExpression list
     * @param types Optional String/SQLExpression list of types for the args
     */
    public ObjectExpression(SQLStatement stmt, JavaTypeMapping mapping, String functionName, List args, List types)
    {
        super(stmt, mapping, functionName, args, types);
    }

    /**
     * Method to change the expression to use only the first column.
     * This is used where we want to use the expression with COUNT(...) and that only allows 1 column.
     */
    public void useFirstColumnOnly()
    {
        if (mapping.getNumberOfColumnMappings() <= 1)
        {
            // Do nothing
            return;
        }

        // Replace the expressionList and SQL as if starting from scratch
        subExprs = new ColumnExpressionList();
        ColumnExpression colExpr = new ColumnExpression(stmt, table, mapping.getColumnMapping(0).getColumn());
        subExprs.addExpression(colExpr);
        st.clearStatement();
        st.append(subExprs.toString());
    }

    /**
     * Equals operator. Called when the query contains "obj == value" where "obj" is this object.
     * @param expr The expression we compare with (the right-hand-side in the query)
     * @return Boolean expression representing the comparison.
     */
    public BooleanExpression eq(SQLExpression expr)
    {
        addSubexpressionsToRelatedExpression(expr);

        // Check suitability for comparison
        // TODO Implement checks
        if (mapping instanceof PersistableIdMapping)
        {
            // Special Case : OID comparison ("id == val")
            if (expr instanceof StringLiteral)
            {
                String oidString = (String)((StringLiteral)expr).getValue();
                if (oidString != null)
                {
                    AbstractClassMetaData cmd = stmt.getRDBMSManager().getMetaDataManager().getMetaDataForClass(mapping.getType(),
                            stmt.getQueryGenerator().getClassLoaderResolver());
                    if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        try
                        {
                            Object id = stmt.getRDBMSManager().getNucleusContext().getIdentityManager().getDatastoreId(oidString);
                            if (id == null)
                            {
                                // TODO Implement this comparison with the key value
                            }
                        }
                        catch (IllegalArgumentException iae)
                        {
                            NucleusLogger.QUERY.info("Attempted comparison of " + this + " and " + expr +
                                " where the former is a datastore-identity and the latter is of incorrect form (" + oidString + ")");
                            SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
                            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
                            return exprFactory.newLiteral(stmt, m, false).eq(exprFactory.newLiteral(stmt, m, true));
                        }
                    }
                    else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                    {
                        // TODO Implement comparison with PK field(s)
                    }
                }
            }
        }

        if (mapping instanceof ReferenceMapping && expr.mapping instanceof PersistableMapping)
        {
            return processComparisonOfImplementationWithReference(this, expr, false);
        }
        else if (mapping instanceof PersistableMapping && expr.mapping instanceof ReferenceMapping)
        {
            return processComparisonOfImplementationWithReference(expr, this, false);
        }

        BooleanExpression bExpr = null;
        if (isParameter() || expr.isParameter())
        {
            if (subExprs != null && subExprs.size() > 1)
            {
                for (int i=0;i<subExprs.size();i++)
                {
                    BooleanExpression subexpr = subExprs.getExpression(i).eq(((ObjectExpression)expr).subExprs.getExpression(i));
                    bExpr = (bExpr == null ? subexpr : bExpr.and(subexpr));
                }
                return bExpr;
            }

            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof NullLiteral)
        {
            if (subExprs != null)
            {
                for (int i=0;i<subExprs.size();i++)
                {
                    BooleanExpression subexpr = expr.eq(subExprs.getExpression(i));
                    bExpr = (bExpr == null ? subexpr : bExpr.and(subexpr));
                }
            }
            return bExpr;
        }
        else if (literalIsValidForSimpleComparison(expr))
        {
            if (subExprs != null && subExprs.size() > 1)
            {
                // More than 1 value to compare with a simple literal!
                return super.eq(expr);
            }

            // Just do a direct comparison with the basic literals
            return new BooleanExpression(this, Expression.OP_EQ, expr);
        }
        else if (expr instanceof ObjectExpression)
        {
            return ExpressionUtils.getEqualityExpressionForObjectExpressions(this, (ObjectExpression)expr, true);
        }
        else
        {
            if (subExprs == null)
            {
                // ObjectExpression for a function call
                return new BooleanExpression(this, Expression.OP_EQ, expr);
            }
            return super.eq(expr);
        }
    }

    protected BooleanExpression processComparisonOfImplementationWithReference(SQLExpression refExpr, SQLExpression implExpr, boolean negate)
    {
        ReferenceMapping refMapping = (ReferenceMapping)refExpr.mapping;
        JavaTypeMapping[] implMappings = refMapping.getJavaTypeMapping();
        int subExprStart = 0;

        String implType = implExpr.mapping.getType();
        String implActualType;
        if (implExpr instanceof ObjectLiteral && ((ObjectLiteral)implExpr).getValue() != null)
        {
            // Use type of literal directly if available. This caters for the case where we have an interface implementation and it is sharing a table with another implementation
        	implActualType = ((ObjectLiteral)implExpr).getValue().getClass().getName();
        }else {
        	implActualType = implType;
        }

        for (int i=0;i<implMappings.length;i++)
        {
            // TODO Handle case where we have a subclass of the implementation here
            // Either implementation-classes is exactly implExpr or root mapping class of implExpr, but still fail if anything between
            if (implMappings[i].getType().equals(implActualType) || implMappings[i].getType().equals(implType))
            {
            	int subExprEnd = subExprStart + implMappings[i].getNumberOfColumnMappings();
            	int implMappingNum = 0;
                BooleanExpression bExpr = refExpr.subExprs.getExpression(subExprStart).eq(implExpr.subExprs.getExpression(implMappingNum++));
                for (int j=subExprStart + 1;j<subExprEnd;j++)
                {
                    bExpr = bExpr.and(refExpr.subExprs.getExpression(j).eq(implExpr.subExprs.getExpression(implMappingNum++)));
                }
                return (negate ? new BooleanExpression(Expression.OP_NOT, bExpr.encloseInParentheses()) : bExpr);
            }

            subExprStart += implMappings[i].getNumberOfColumnMappings();
        }

        // Implementation not found explicitly, so just treat as if "ObjectExpression.eq(ObjectExpression)". See e.g JDO TCK "companyPMInterface" test
        return ExpressionUtils.getEqualityExpressionForObjectExpressions((ObjectExpression) refExpr, (ObjectExpression)implExpr, true);
    }

    /**
     * Not equals operator. Called when the query contains "obj != value" where "obj" is this object.
     * @param expr The expression we compare with (the right-hand-side in the query)
     * @return Boolean expression representing the comparison.
     */
    public BooleanExpression ne(SQLExpression expr)
    {
        addSubexpressionsToRelatedExpression(expr);

        if (mapping instanceof ReferenceMapping && expr.mapping instanceof PersistableMapping)
        {
            return processComparisonOfImplementationWithReference(this, expr, true);
        }
        else if (mapping instanceof PersistableMapping && expr.mapping instanceof ReferenceMapping)
        {
            return processComparisonOfImplementationWithReference(expr, this, true);
        }

        BooleanExpression bExpr = null;
        if (isParameter() || expr.isParameter())
        {
            if (subExprs != null && subExprs.size() > 1)
            {
                for (int i=0;i<subExprs.size();i++)
                {
                    BooleanExpression subexpr = subExprs.getExpression(i).eq(((ObjectExpression)expr).subExprs.getExpression(i));
                    bExpr = (bExpr == null ? subexpr : bExpr.and(subexpr));
                }
                return new BooleanExpression(Expression.OP_NOT, bExpr != null ? bExpr.encloseInParentheses() : null);
            }

            // Comparison with parameter, so just give boolean compare
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof NullLiteral)
        {
            if (subExprs != null)
            {
                for (int i=0;i<subExprs.size();i++)
                {
                    BooleanExpression subexpr = expr.eq(subExprs.getExpression(i));
                    bExpr = (bExpr == null ? subexpr : bExpr.and(subexpr));
                }
            }
            return new BooleanExpression(Expression.OP_NOT, bExpr != null ? bExpr.encloseInParentheses() : null);
        }
        else if (literalIsValidForSimpleComparison(expr))
        {
            if (subExprs != null && subExprs.size() > 1)
            {
                // More than 1 value to compare with a literal!
                return super.ne(expr);
            }

            // Just do a direct comparison with the basic literals
            return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
        }
        else if (expr instanceof ObjectExpression)
        {
            return ExpressionUtils.getEqualityExpressionForObjectExpressions(this, (ObjectExpression)expr, false);
        }
        else
        {
            if (subExprs == null)
            {
                // ObjectExpression for a function call
                return new BooleanExpression(this, Expression.OP_NOTEQ, expr);
            }
            return super.ne(expr);
        }
    }

    /**
     * Updates the supplied expression with sub-expressions of consistent types to this expression.
     * This is called when we have some comparison expression (e.g this == expr) and where the
     * other expression has no sub-expressions currently.
     * @param expr The expression
     */
    protected void addSubexpressionsToRelatedExpression(SQLExpression expr)
    {
        if (expr.subExprs == null)
        {
            if (subExprs != null)
            {
                // operand has no sub-expressions yet (object input parameter) so add some
                expr.subExprs = new ColumnExpressionList();
                for (int i=0;i<subExprs.size();i++)
                {
                    // TODO Put value of subMapping in
                    expr.subExprs.addExpression(new ColumnExpression(stmt, expr.parameterName, expr.mapping, null, i));
                }
            }
        }
    }

    /**
     * Convenience method to return if this object is valid for simple comparison
     * with the passed expression. Performs a type comparison of the object and the expression
     * for compatibility. The expression must be a literal of a suitable type for simple
     * comparison (e.g where this object is a String, and the literal is a StringLiteral).
     * @param expr The expression
     * @return Whether a simple comparison is valid
     */
    private boolean literalIsValidForSimpleComparison(SQLExpression expr)
    {
        // Our mapping is a single field type and is of the same basic type as the expression
        if ((expr instanceof BooleanLiteral && (mapping instanceof BooleanMapping)) ||
            (expr instanceof ByteLiteral && (mapping instanceof ByteMapping)) ||
            (expr instanceof CharacterLiteral && (mapping instanceof CharacterMapping)) ||
            (expr instanceof FloatingPointLiteral && 
             (mapping instanceof FloatMapping || mapping instanceof DoubleMapping ||
              mapping instanceof BigDecimalMapping)) ||
            (expr instanceof IntegerLiteral &&
             (mapping instanceof IntegerMapping || mapping instanceof LongMapping ||
              mapping instanceof BigIntegerMapping) || mapping instanceof ShortMapping) ||
            (expr instanceof TemporalLiteral &&
             (mapping instanceof DateMapping || mapping instanceof SqlDateMapping || 
              mapping instanceof SqlTimeMapping || mapping instanceof SqlTimestampMapping)) ||
            (expr instanceof StringLiteral &&
             (mapping instanceof StringMapping || mapping instanceof CharacterMapping)))
        {
            return true;
        }

        return false;
    }

    public BooleanExpression in(SQLExpression expr, boolean not) 
    {
        return new BooleanExpression(this, not ? Expression.OP_NOTIN : Expression.OP_IN, expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#lt(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression lt(SQLExpression expr)
    {
        if (subExprs == null)
        {
            // ObjectExpression for a function call
            return new BooleanExpression(this, Expression.OP_LT, expr);
        }
        return super.lt(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#le(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression le(SQLExpression expr)
    {
        if (subExprs == null)
        {
            // ObjectExpression for a function call
            return new BooleanExpression(this, Expression.OP_LTEQ, expr);
        }
        return super.le(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#gt(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression gt(SQLExpression expr)
    {
        if (subExprs == null)
        {
            // ObjectExpression for a function call
            return new BooleanExpression(this, Expression.OP_GT, expr);
        }
        return super.gt(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.sql.expression.SQLExpression#ge(org.datanucleus.store.rdbms.sql.expression.SQLExpression)
     */
    @Override
    public BooleanExpression ge(SQLExpression expr)
    {
        if (subExprs == null)
        {
            // ObjectExpression for a function call
            return new BooleanExpression(this, Expression.OP_GTEQ, expr);
        }
        return super.ge(expr);
    }

    /**
     * Cast operator. Called when the query contains "(type)obj" where "obj" is this object.
     * @param expr Expression representing the type to cast to
     * @return Scalar expression representing the cast object.
     */
    public SQLExpression cast(SQLExpression expr)
    {
        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        ClassLoaderResolver clr = stmt.getClassLoaderResolver();

        // Extract cast type
        String castClassName = (String)((StringLiteral)expr).getValue();
        Class type = null;
        try
        {
            type = stmt.getQueryGenerator().resolveClass(castClassName);
        }
        catch (ClassNotResolvedException cnre)
        {
            type = null;
        }
        if (type == null)
        {
            throw new NucleusUserException(Localiser.msg("037017", castClassName));
        }

        // Extract type of this object and check obvious conditions
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        Class memberType = clr.classForName(mapping.getType());
        if (!memberType.isAssignableFrom(type) && !type.isAssignableFrom(memberType))
        {
            // object type and cast type are totally incompatible, so just return false
            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
            return exprFactory.newLiteral(stmt, m, false).eq(exprFactory.newLiteral(stmt, m, true));
        }
        else if (memberType == type)
        {
            // Just return this expression since it is already castable
            return this;
        }

        if (mapping instanceof EmbeddedMapping)
        {
            // Don't support embedded casts
            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
            return exprFactory.newLiteral(stmt, m, false).eq(exprFactory.newLiteral(stmt, m, true));
        }
        else if (mapping instanceof ReferenceMapping)
        {
            // This expression will be for the table containing the reference so need to join now
            ReferenceMapping refMapping = (ReferenceMapping)mapping;
            if (refMapping.getMappingStrategy() != ReferenceMapping.PER_IMPLEMENTATION_MAPPING)
            {
                throw new NucleusUserException("Impossible to do cast of interface to " + type.getName() +
                    " since interface is persisted as embedded String." +
                    " Use per-implementation mapping to allow this query");
            }
            JavaTypeMapping[] implMappings = refMapping.getJavaTypeMapping();
            for (int i=0;i<implMappings.length;i++)
            {
                Class implType = clr.classForName(implMappings[i].getType());
                if (type.isAssignableFrom(implType))
                {
                    DatastoreClass castTable = storeMgr.getDatastoreClass(type.getName(), clr);
                    SQLTable castSqlTbl = stmt.join(JoinType.LEFT_OUTER_JOIN, table, implMappings[i], refMapping, castTable, null, castTable.getIdMapping(), null, null, null, true, null);
                    return exprFactory.newExpression(stmt, castSqlTbl, castTable.getIdMapping());
                }
            }

            // No implementation matching this cast type, so return false
            NucleusLogger.QUERY.warn("Unable to process cast of interface field to " + type.getName() +
                " since it has no implementations that match that type");
            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
            return exprFactory.newLiteral(stmt, m, false).eq(exprFactory.newLiteral(stmt, m, true));
        }
        else if (mapping instanceof PersistableMapping)
        {
            // Check if there is already the cast table in the current table group
            DatastoreClass castTable = storeMgr.getDatastoreClass(type.getName(), clr);
            SQLTable castSqlTbl = stmt.getTable(castTable, table.getGroupName());
            if (castSqlTbl == null)
            {
                // Join not present, so join to the cast table
                castSqlTbl = stmt.join(JoinType.LEFT_OUTER_JOIN, table, table.getTable().getIdMapping(), castTable, null, castTable.getIdMapping(), null, table.getGroupName());
            }

            if (castSqlTbl == table)
            {
                AbstractClassMetaData castCmd = storeMgr.getMetaDataManager().getMetaDataForClass(type, clr);
                if (castCmd.hasDiscriminatorStrategy())
                {
                    // TODO How do we handle this? If this is part of the filter then need to hang a BooleanExpression off the ObjectExpression and apply later.
                    // If this is part of the result, do we just return null when this is not the right type?
                    NucleusLogger.QUERY.warn(">> Currently do not support adding restriction on discriminator for table=" + table + " to " + type);
                }
            }

            // Return an expression based on the cast table
            return exprFactory.newExpression(stmt, castSqlTbl, castTable.getIdMapping());
        }
        else
        {
            // TODO Handle other casts
        }

        // TODO Implement cast (left outer join to table of type, then return new ObjectExpression)
        throw new NucleusUserException("Dont currently support ObjectExpression.cast(" + type + ")");
    }

    /**
     * An "is" (instanceOf) expression, providing a BooleanExpression whether this expression is an instanceof the provided type.
     * @param expr The expression representing the type
     * @param not Whether the operator is "!instanceof"
     * @return Whether this expression is an instance of the provided type
     */
    public BooleanExpression is(SQLExpression expr, boolean not)
    {
        RDBMSStoreManager storeMgr = stmt.getRDBMSManager();
        ClassLoaderResolver clr = stmt.getClassLoaderResolver();

        // Extract instanceOf type
        String instanceofClassName = null;
        SQLExpression classExpr = expr;
        if (expr instanceof TypeConverterLiteral)
        {
            // For some reason the user has input "TYPE(field) = :param" and param is a type-converted value
            classExpr = ((TypeConverterLiteral)expr).getDelegate();
        }

        if (classExpr instanceof StringLiteral)
        {
            instanceofClassName = (String)((StringLiteral)classExpr).getValue();
        }
        else if (classExpr instanceof CollectionLiteral)
        {
            // "a instanceof (b1,b2,b3)"
            CollectionLiteral typesLiteral = (CollectionLiteral)classExpr;
            Collection values = (Collection) typesLiteral.getValue();
            if (values.size() == 1)
            {
                Object value = values.iterator().next();
                String valueStr = null;
                if (value instanceof Class)
                {
                    valueStr = ((Class)value).getCanonicalName();
                }
                else if (value instanceof String)
                {
                    valueStr = (String)value;
                }
                else
                {
                    throw new NucleusUserException("Do not support CollectionLiteral of element type " + value.getClass().getName());
                }
                return is(new StringLiteral(stmt, typesLiteral.getJavaTypeMapping(), valueStr, typesLiteral.getParameterName()), not);
            }

            List<BooleanExpression> listExp = new LinkedList<>();
            for (Object value : values)
            {
                String valueStr = null;
                if (value instanceof Class)
                {
                    valueStr = ((Class)value).getCanonicalName();
                }
                else if (value instanceof String)
                {
                    valueStr = (String)value;
                }
                else
                {
                    throw new NucleusUserException("Do not support CollectionLiteral of element type " + value.getClass().getName());
                }
                listExp.add(is(new StringLiteral(stmt, typesLiteral.getJavaTypeMapping(), valueStr, typesLiteral.getParameterName()), false));
            }

            BooleanExpression result = null;
            for (BooleanExpression sqlExpression : listExp)
            {
                result = result == null ? sqlExpression : result.ior(sqlExpression);
            }
            if (result != null)
            {
                return not ? result.not() : result;
            }
        }
        else
        {
            throw new NucleusUserException("Do not currently support `instanceof` with class expression of type " + classExpr);
        }

        Class type = null;
        try
        {
            type = stmt.getQueryGenerator().resolveClass(instanceofClassName);
        }
        catch (ClassNotResolvedException cnre)
        {
            type = null;
        }
        if (type == null)
        {
            throw new NucleusUserException(Localiser.msg("037016", instanceofClassName));
        }

        // Extract type of member and check obvious conditions
        SQLExpressionFactory exprFactory = stmt.getSQLExpressionFactory();
        Class memberType = clr.classForName(mapping.getType());
        if (!memberType.isAssignableFrom(type) && !type.isAssignableFrom(memberType))
        {
            // Member type and instanceof type are totally incompatible, so just return false
            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
            return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, not));
        }
        else if (memberType == type)
        {
            // instanceof type is the same as the member type therefore must comply (can't store supertypes)
            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
            return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, !not));
        }

        if (mapping instanceof EmbeddedMapping)
        {
            // Don't support embedded instanceof expressions
            AbstractClassMetaData fieldCmd = storeMgr.getMetaDataManager().getMetaDataForClass(mapping.getType(), clr);
            if (fieldCmd.hasDiscriminatorStrategy())
            {
                // Embedded field with inheritance so add discriminator restriction
                JavaTypeMapping discMapping = ((EmbeddedMapping)mapping).getDiscriminatorMapping();
                AbstractClassMetaData typeCmd = storeMgr.getMetaDataManager().getMetaDataForClass(type, clr);
                SQLExpression discExpr = stmt.getSQLExpressionFactory().newExpression(stmt, table, discMapping);
                SQLExpression discValExpr = stmt.getSQLExpressionFactory().newLiteral(stmt, discMapping, typeCmd.getDiscriminatorValue());
                BooleanExpression typeExpr = (not ? discExpr.ne(discValExpr) : discExpr.eq(discValExpr));

                Iterator<String> subclassIter = storeMgr.getSubClassesForClass(type.getName(), true, clr).iterator();
                while (subclassIter.hasNext())
                {
                    String subclassName = subclassIter.next();
                    AbstractClassMetaData subtypeCmd = storeMgr.getMetaDataManager().getMetaDataForClass(subclassName, clr);
                    Object subtypeDiscVal = subtypeCmd.getDiscriminatorValue();
                    discValExpr = stmt.getSQLExpressionFactory().newLiteral(stmt, discMapping, subtypeDiscVal);

                    BooleanExpression subtypeExpr = (not ? discExpr.ne(discValExpr) : discExpr.eq(discValExpr));
                    if (not)
                    {
                        typeExpr = typeExpr.and(subtypeExpr);
                    }
                    else
                    {
                        typeExpr = typeExpr.ior(subtypeExpr);
                    }
                }

                return typeExpr;
            }

            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
            return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, not));
        }
        else if (mapping instanceof PersistableMapping || mapping instanceof ReferenceMapping)
        {
            // Field has its own table, so join to it
            AbstractClassMetaData memberCmd = storeMgr.getMetaDataManager().getMetaDataForClass(mapping.getType(), clr);
            DatastoreClass memberTable = null;
            if (memberCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
            {
                // Field is a PC class that uses "subclass-table" inheritance strategy (and so has multiple possible tables to join to)
                AbstractClassMetaData[] cmds = storeMgr.getClassesManagingTableForClass(memberCmd, clr);
                if (cmds != null)
                {
                    // Join to the first table
                    // TODO Allow for all possible tables. Can we do an OR of the tables ? How ?
                    if (cmds.length > 1)
                    {
                        NucleusLogger.QUERY.warn(Localiser.msg("037006", mapping.getMemberMetaData().getFullFieldName(), cmds[0].getFullClassName()));
                    }
                    memberTable = storeMgr.getDatastoreClass(cmds[0].getFullClassName(), clr);
                }
                else
                {
                    // No subclasses with tables to join to, so throw a user error
                    throw new NucleusUserException(Localiser.msg("037005", mapping.getMemberMetaData().getFullFieldName()));
                }
            }
            else
            {
                // Class of the field will have its own table
                memberTable = storeMgr.getDatastoreClass(mapping.getType(), clr);
            }

            DiscriminatorMetaData dismd = memberTable.getDiscriminatorMetaData();
            DiscriminatorMapping discMapping = (DiscriminatorMapping)memberTable.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, false);
            if (discMapping != null)
            {
                SQLTable targetSqlTbl = null;
                if (mapping.getTable() != memberTable)
                {
                    // FK is on source table so inner join to target table (holding the discriminator)
                    targetSqlTbl = stmt.getTable(memberTable, null);
                    if (targetSqlTbl == null)
                    {
                        targetSqlTbl = stmt.join(JoinType.INNER_JOIN, getSQLTable(), mapping, memberTable, null, memberTable.getIdMapping(), null, null);
                    }
                }
                else
                {
                    // FK is on target side and already joined
                    targetSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(stmt, getSQLTable(), discMapping);
                }

                // Add restrict to discriminator for the instanceOf type and subclasses
                SQLTable discSqlTbl = targetSqlTbl;
                BooleanExpression discExpr = null;
                if (!Modifier.isAbstract(type.getModifiers()))
                {
                    discExpr = SQLStatementHelper.getExpressionForDiscriminatorForClass(stmt, type.getName(), dismd, discMapping, discSqlTbl, clr);
                }

                Iterator<String> subclassIter = storeMgr.getSubClassesForClass(type.getName(), true, clr).iterator();
                boolean multiplePossibles = false;
                while (subclassIter.hasNext())
                {
                    String subclassName = subclassIter.next();
                    Class subclass = clr.classForName(subclassName);
                    if (Modifier.isAbstract(subclass.getModifiers()))
                    {
                        continue;
                    }
                    BooleanExpression discExprSub = SQLStatementHelper.getExpressionForDiscriminatorForClass(stmt, subclassName, dismd, discMapping, discSqlTbl, clr);
                    if (discExpr != null)
                    {
                        multiplePossibles = true;
                        discExpr = discExpr.ior(discExprSub);
                    }
                    else
                    {
                        discExpr = discExprSub;
                    }
                }
                if (multiplePossibles && discExpr != null)
                {
                    discExpr.encloseInParentheses();
                }
                return ((not && discExpr!=null) ? discExpr.not() : discExpr);
            }

            // No discriminator, so the following is likely incomplete.

            // Join to member table
            DatastoreClass table = null;
            if (memberCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
            {
                // Field is a PC class that uses "subclass-table" inheritance strategy (and so has multiple possible tables to join to)
                AbstractClassMetaData[] cmds = storeMgr.getClassesManagingTableForClass(memberCmd, clr);
                if (cmds != null)
                {
                    // Join to the first table
                    // TODO Allow for all possible tables. Can we do an OR of the tables ? How ?
                    if (cmds.length > 1)
                    {
                        NucleusLogger.QUERY.warn(Localiser.msg("037006", mapping.getMemberMetaData().getFullFieldName(), cmds[0].getFullClassName()));
                    }
                    table = storeMgr.getDatastoreClass(cmds[0].getFullClassName(), clr);
                }
                else
                {
                    // No subclasses with tables to join to, so throw a user error
                    throw new NucleusUserException(Localiser.msg("037005", mapping.getMemberMetaData().getFullFieldName()));
                }
            }
            else
            {
                // Class of the field will have its own table
                table = storeMgr.getDatastoreClass(mapping.getType(), clr);
            }

            if (table.managesClass(type.getName()))
            {
                // This type is managed in this table so must be an instance TODO Is this correct, what if member is using discrim?
                JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
                return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, !not));
            }

            if (table == stmt.getPrimaryTable().getTable())
            {
                // This is member table, so just need to restrict to the instanceof type now
                JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
                if (stmt instanceof SelectStatement)
                {
                    SelectStatement selectStmt = (SelectStatement)stmt;
                    if (selectStmt.getNumberOfUnions() == 0)
                    {
                        // No UNIONs so just check the main statement and return according to whether it is allowed
                        Class mainCandidateCls = clr.classForName(stmt.getCandidateClassName());
                        if (type.isAssignableFrom(mainCandidateCls) == not)
                        {
                            SQLExpression returnExpr = exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, false));
                            return (BooleanExpression)returnExpr;
                        }

                        SQLExpression returnExpr = exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, true));
                        return (BooleanExpression)returnExpr;
                    }

                    NucleusLogger.QUERY.warn("TYPE/INSTANCEOF operator for class=" + memberCmd.getFullClassName() + " on table=" + memberTable + " for type=" + instanceofClassName +
                            " but there is no discriminator and using UNIONs. Any subsequent handling is likely incorrect TODO");
                    // a). we have unions for the member, so restrict to just the applicable unions
                    // Note that this is only really valid is wanting "a instanceof SUB1".
                    // It fails when we want to do "a instanceof SUB1 || a instanceof SUB2"
                    // TODO What if this "OP_IS" is in the SELECT clause??? Need to update QueryToSQLMapper.compileResult
                    Class mainCandidateCls = clr.classForName(stmt.getCandidateClassName());
                    if (type.isAssignableFrom(mainCandidateCls) == not)
                    {
                        SQLExpression unionClauseExpr = exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, false));
                        stmt.whereAnd((BooleanExpression)unionClauseExpr, false);
                    }
                    List<SelectStatement> unionStmts = selectStmt.getUnions();
                    for (SelectStatement unionStmt : unionStmts)
                    {
                        Class unionCandidateCls = clr.classForName(unionStmt.getCandidateClassName());
                        if (type.isAssignableFrom(unionCandidateCls) == not)
                        {
                            SQLExpression unionClauseExpr = exprFactory.newLiteral(unionStmt, m, true).eq(exprFactory.newLiteral(unionStmt, m, false));
                            unionStmt.whereAnd((BooleanExpression)unionClauseExpr, false); // TODO Avoid using whereAnd
                        }
                    }

                    // Just return true since we applied the condition direct to the unions
                    SQLExpression returnExpr = exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, true));
                    return (BooleanExpression)returnExpr;
                }

                // b). The member table doesn't manage the instanceof type, so do inner join to 
                // the table of the instanceof to impose the instanceof condition
                DatastoreClass instanceofTable = storeMgr.getDatastoreClass(type.getName(), clr);
                stmt.join(JoinType.INNER_JOIN, this.table, this.table.getTable().getIdMapping(), instanceofTable, null, instanceofTable.getIdMapping(), null, this.table.getGroupName());
                return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, !not));
            }

            // Do inner join to this table to impose the instanceOf
            DatastoreClass instanceofTable = storeMgr.getDatastoreClass(type.getName(), clr);
            stmt.join(JoinType.INNER_JOIN, this.table, this.table.getTable().getIdMapping(), instanceofTable, null, instanceofTable.getIdMapping(), null, this.table.getGroupName());
            JavaTypeMapping m = exprFactory.getMappingForType(boolean.class, true);
            return exprFactory.newLiteral(stmt, m, true).eq(exprFactory.newLiteral(stmt, m, !not));
        }
        else
        {
            // TODO Implement instanceof for other types
            throw new NucleusException("Dont currently support " + this + " instanceof " + type.getName());
        }
    }

    public SQLExpression invoke(String methodName, List args)
    {
        return stmt.getRDBMSManager().getSQLExpressionFactory().invokeMethod(stmt, Object.class.getName(), methodName, this, args);
    }
}