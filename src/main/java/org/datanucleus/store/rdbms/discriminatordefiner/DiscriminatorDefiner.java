package org.datanucleus.store.rdbms.discriminatordefiner;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.query.StatementClassMapping;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.expression.BooleanExpression;

/**
 * A way of defining a custom discriminator mechanism that goes beyond what can be
 * defined using standard JDO mechanisms (which is discriminating one a single
 * column).
 */
public interface DiscriminatorDefiner
{

    String METADATA_EXTENSION_DISCRIMINATOR_DEFINER = "discriminator-definer";

    /**
     * Return a custom class-name-resolver.
     * Return null to leave class name resolving to normal JDO mechanisms.
     * <p>
     * Current implementation only allows for discriminating on one string column.
     * <p>
     * The custom class-name-resolver enables to discriminate persistent
     * objects from the selected ResultSet - eg. by using more column values
     * to make a decision on which class should be instantiated from this DB row.
     * This calculation can be of any complexity using the DB row from ResultSet.
     *
     * @param ec            Execution context
     * @param resultMapping Mapping used for query, or null if fields of SQL table are used directly
     * @return null if no custom class name resolver is in use for supplied candidate class,
     * otherwise return a custom class name resolver instance to use.
     */
    CustomClassNameResolver getCustomClassNameResolver(ExecutionContext ec, StatementClassMapping resultMapping);

    /**
     * Return custom boolean expression for fetching object of class name when using JDO/SQL queries.
     * Return null to leave SQL generation to normal JDO mechanisms.
     * <p>
     * This enables to generate SQL for fetching custom discriminated classes
     * that might have been implemented in getCustomClassNameResolver method.
     *
     * @param stmt SQL statement being build
     * @param className name of class being queried in JDO/SQL query
     * @param dismd defined JDO discriminator meta data
     * @param discriminatorMapping defined JDO discriminator mapping
     * @param discrimSqlTbl table to query
     * @param clr class loader resolver
     * @return null, to use standard JDO discriminator mechanisms,
     * otherwise return full new boolean expression to be used for finding objects
     * using custom discriminator for class of className.
     */
    BooleanExpression getCustomExpressionForDiscriminatorForClass(SQLStatement stmt, String className, DiscriminatorMetaData dismd, JavaTypeMapping discriminatorMapping, SQLTable discrimSqlTbl, ClassLoaderResolver clr);
}
