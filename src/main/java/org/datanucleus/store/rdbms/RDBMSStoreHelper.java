/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved. 
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.InheritanceMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.discriminator.DiscriminatorClassNameResolver;
import org.datanucleus.store.rdbms.discriminator.DiscriminatorDefiner;
import org.datanucleus.store.rdbms.mapping.column.ColumnMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableIdMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.query.RDBMSQueryUtils;
import org.datanucleus.store.rdbms.sql.DiscriminatorStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.SelectStatementGenerator;
import org.datanucleus.store.rdbms.sql.UnionStatementGenerator;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.util.NucleusLogger;

/**
 * Provides a series of utilities assisting in the datastore management process for RDBMS datastores.
 */
public class RDBMSStoreHelper
{
    private RDBMSStoreHelper(){}
    /**
     * Utility that does a discriminator candidate query for the specified candidate and subclasses
     * and returns the class name of the instance that has the specified identity (if any).
     * @param storeMgr RDBMS StoreManager
     * @param ec execution context
     * @param id The id
     * @param cmd Metadata for the root candidate class
     * @return Name of the class with this identity (or null if none found)
     */
    public static String getClassNameForIdUsingDiscriminator(RDBMSStoreManager storeMgr, ExecutionContext ec, Object id, AbstractClassMetaData cmd)
    {
        // Check for input error
        if (cmd == null || id == null)
        {
            return null;
        }

        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        DatastoreClass primaryTable = storeMgr.getDatastoreClass(cmd.getFullClassName(), clr);

        // Form the query to find which one of these classes has the instance with this id
        DiscriminatorStatementGenerator stmtGen = new DiscriminatorStatementGenerator(storeMgr, clr, clr.classForName(cmd.getFullClassName()), true, null, null);
        stmtGen.setOption(SelectStatementGenerator.OPTION_RESTRICT_DISCRIM);
        SelectStatement sqlStmt = stmtGen.getStatement(ec);

        // Select the discriminator
        JavaTypeMapping discrimMapping = primaryTable.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, true);
        SQLTable discrimSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), discrimMapping);
        sqlStmt.select(discrimSqlTbl, discrimMapping, null);

        // Restrict to this id
        JavaTypeMapping idMapping = primaryTable.getIdMapping();
        JavaTypeMapping idParamMapping = new PersistableIdMapping((PersistableMapping) idMapping);
        SQLExpression sqlFldExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), idMapping);
        SQLExpression sqlFldVal = exprFactory.newLiteralParameter(sqlStmt, idParamMapping, id, "ID");
        sqlStmt.whereAnd(sqlFldExpr.eq(sqlFldVal), true);

        // check if custom class name definer has been defined
        final DiscriminatorDefiner discriminatorDefiner = storeMgr.getDiscriminatorDefiner(cmd, clr);
        DiscriminatorClassNameResolver customClassNameResolver = null;
        if (discriminatorDefiner != null)
        {
            customClassNameResolver = discriminatorDefiner.getDiscriminatorClassNameResolver(ec, null);
            if (customClassNameResolver != null)
            {
                // custom discriminator definer also gets access to fields in default fetch group
                Set<Column> alreadySelected = Arrays.stream(discrimMapping.getColumnMappings()).map(ColumnMapping::getColumn).collect(Collectors.toSet());
                final int[] dfgMemberPositions = cmd.getDFGMemberPositions();
                for (int dfgMemberPosition : dfgMemberPositions)
                {
                    final AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(dfgMemberPosition);
                    final JavaTypeMapping memberMapping = primaryTable.getMemberMapping(mmd);
                    for (ColumnMapping columnMapping : memberMapping.getColumnMappings())
                    {
                        final Column column = columnMapping.getColumn();
                        // not add discriminator column again
                        if (!alreadySelected.contains(column))
                        {
                            sqlStmt.select(discrimSqlTbl, column, null);
                        }
                    }
                }
            }
        }

        // Perform the query
        try
        {
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            if (ec.getSerializeReadForClass(cmd.getFullClassName()))
            {
                sqlStmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, true);
            }

            try
            {
                PreparedStatement ps = SQLStatementHelper.getPreparedStatementForSQLStatement(sqlStmt, ec, mconn, null, null);
                String statement = sqlStmt.getSQLText().toSQL();
                try
                {
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, statement, ps);
                    try
                    {
                        while (rs.next())
                        {
                            // check custom discriminator definer first
                            if (customClassNameResolver != null)
                            {
                                String customClassName = customClassNameResolver.getClassName(rs);
                                if (customClassName != null)
                                {
                                    return customClassName;
                                }
                            }
                            // no custom discriminator defined - business as usual
                            DiscriminatorMetaData dismd = discrimMapping.getTable().getDiscriminatorMetaData();
                            return RDBMSQueryUtils.getClassNameFromDiscriminatorResultSetRow(discrimMapping, dismd, rs, ec);
                        }
                    }
                    finally
                    {
                        rs.close();
                    }
                }
                finally
                {
                    sqlControl.closeStatement(mconn, ps);
                }
            }
            finally
            {
                mconn.release();
            }
        }
        catch (SQLException sqe)
        {
            NucleusLogger.DATASTORE.error("Exception thrown on querying of discriminator for id", sqe);
            throw new NucleusDataStoreException(sqe.toString(), sqe);
        }

        return null;
    }

    /**
     * Utility that does a union candidate query for the specified candidate(s) and subclasses
     * and returns the class name of the instance that has the specified identity (if any).
     * @param storeMgr RDBMS StoreManager
     * @param ec execution context
     * @param id The id
     * @param rootCmds Metadata for the classes at the root
     * @return Name of the class with this identity (or null if none found)
     */
    public static String getClassNameForIdUsingUnion(RDBMSStoreManager storeMgr, ExecutionContext ec, Object id, List<AbstractClassMetaData> rootCmds)
    {
        // Check for input error
        if (rootCmds == null || rootCmds.isEmpty() || id == null)
        {
            return null;
        }

        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();

        // Form a query UNIONing all possible root candidates (and their subclasses)
        Iterator<AbstractClassMetaData> rootCmdIter = rootCmds.iterator();

        AbstractClassMetaData sampleCmd = null; // Metadata for sample class in the tree so we can check if needs locking
        SelectStatement sqlStmtMain = null;
        while (rootCmdIter.hasNext())
        {
            AbstractClassMetaData rootCmd = rootCmdIter.next();
            DatastoreClass rootTbl = storeMgr.getDatastoreClass(rootCmd.getFullClassName(), clr);
            InheritanceMetaData rootInhmd = rootCmd.getBaseAbstractClassMetaData().getInheritanceMetaData();
            if (rootInhmd.getStrategy() == InheritanceStrategy.COMPLETE_TABLE)
            {
                // COMPLETE TABLE so use one branch of UNION for each possible class
                if (rootTbl != null)
                {
                    UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, clr.classForName(rootCmd.getFullClassName()), false, null, null);
                    stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                    if (sqlStmtMain == null)
                    {
                        sampleCmd = rootCmd;
                        sqlStmtMain = stmtGen.getStatement(ec);

                        // WHERE (object id) = ?
                        JavaTypeMapping idMapping = sqlStmtMain.getPrimaryTable().getTable().getIdMapping();
                        JavaTypeMapping idParamMapping = new PersistableIdMapping((PersistableMapping) idMapping);
                        SQLExpression fieldExpr = exprFactory.newExpression(sqlStmtMain, sqlStmtMain.getPrimaryTable(), idMapping);
                        SQLExpression fieldVal = exprFactory.newLiteralParameter(sqlStmtMain, idParamMapping, id, "ID");
                        sqlStmtMain.whereAnd(fieldExpr.eq(fieldVal), true);
                    }
                    else
                    {
                        SelectStatement sqlStmt = stmtGen.getStatement(ec);

                        // WHERE (object id) = ?
                        JavaTypeMapping idMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                        JavaTypeMapping idParamMapping = new PersistableIdMapping((PersistableMapping) idMapping);
                        SQLExpression fieldExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), idMapping);
                        SQLExpression fieldVal = exprFactory.newLiteralParameter(sqlStmt, idParamMapping, id, "ID");
                        sqlStmt.whereAnd(fieldExpr.eq(fieldVal), true);

                        sqlStmtMain.union(sqlStmt);
                    }
                }
                Collection<String> rootSubclassNames = storeMgr.getSubClassesForClass(rootCmd.getFullClassName(), true, clr);
                for (String rootSubclassName : rootSubclassNames)
                {
                    AbstractClassMetaData rootSubclassCmd = storeMgr.getMetaDataManager().getMetaDataForClass(rootSubclassName, clr);
                    DatastoreClass rootSubclassTbl = storeMgr.getDatastoreClass(rootSubclassCmd.getFullClassName(), clr);
                    if (rootSubclassTbl != null)
                    {

                        UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, clr.classForName(rootSubclassCmd.getFullClassName()), false, null, null);
                        stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                        if (sqlStmtMain == null)
                        {
                            sampleCmd = rootSubclassCmd;
                            sqlStmtMain = stmtGen.getStatement(ec);

                            // WHERE (object id) = ?
                            JavaTypeMapping idMapping = sqlStmtMain.getPrimaryTable().getTable().getIdMapping();
                            JavaTypeMapping idParamMapping = new PersistableIdMapping((PersistableMapping) idMapping);
                            SQLExpression fieldExpr = exprFactory.newExpression(sqlStmtMain, sqlStmtMain.getPrimaryTable(), idMapping);
                            SQLExpression fieldVal = exprFactory.newLiteralParameter(sqlStmtMain, idParamMapping, id, "ID");
                            sqlStmtMain.whereAnd(fieldExpr.eq(fieldVal), true);
                        }
                        else
                        {
                            SelectStatement sqlStmt = stmtGen.getStatement(ec);

                            // WHERE (object id) = ?
                            JavaTypeMapping idMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                            JavaTypeMapping idParamMapping = new PersistableIdMapping((PersistableMapping) idMapping);
                            SQLExpression fieldExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), idMapping);
                            SQLExpression fieldVal = exprFactory.newLiteralParameter(sqlStmt, idParamMapping, id, "ID");
                            sqlStmt.whereAnd(fieldExpr.eq(fieldVal), true);

                            sqlStmtMain.union(sqlStmt);
                        }
                    }
                }

                continue;
            }

            if (rootTbl == null)
            {
                // Class must be using "subclass-table" (no table of its own) so find where it is
                AbstractClassMetaData[] subcmds = storeMgr.getClassesManagingTableForClass(rootCmd, clr);
                if (subcmds == null || subcmds.length == 0)
                {
                    // No table for this class so ignore
                }
                else
                {
                    for (int i=0;i<subcmds.length;i++)
                    {
                        UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, clr.classForName(subcmds[i].getFullClassName()), true, null, null);
                        stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                        if (sqlStmtMain == null)
                        {
                            sampleCmd = subcmds[i];
                            sqlStmtMain = stmtGen.getStatement(ec);

                            // WHERE (object id) = ?
                            JavaTypeMapping idMapping = sqlStmtMain.getPrimaryTable().getTable().getIdMapping();
                            JavaTypeMapping idParamMapping = new PersistableIdMapping((PersistableMapping) idMapping);
                            SQLExpression fieldExpr = exprFactory.newExpression(sqlStmtMain, sqlStmtMain.getPrimaryTable(), idMapping);
                            SQLExpression fieldVal = exprFactory.newLiteralParameter(sqlStmtMain, idParamMapping, id, "ID");
                            sqlStmtMain.whereAnd(fieldExpr.eq(fieldVal), true);
                        }
                        else
                        {
                            SelectStatement sqlStmt = stmtGen.getStatement(ec);

                            // WHERE (object id) = ?
                            JavaTypeMapping idMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                            JavaTypeMapping idParamMapping = new PersistableIdMapping((PersistableMapping) idMapping);
                            SQLExpression fieldExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), idMapping);
                            SQLExpression fieldVal = exprFactory.newLiteralParameter(sqlStmt, idParamMapping, id, "ID");
                            sqlStmt.whereAnd(fieldExpr.eq(fieldVal), true);

                            sqlStmtMain.union(sqlStmt);
                        }
                    }
                }
            }
            else
            {
                UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, clr.classForName(rootCmd.getFullClassName()), true, null, null);
                stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                if (sqlStmtMain == null)
                {
                    sampleCmd = rootCmd;
                    sqlStmtMain = stmtGen.getStatement(ec);

                    // WHERE (object id) = ?
                    JavaTypeMapping idMapping = sqlStmtMain.getPrimaryTable().getTable().getIdMapping();
                    JavaTypeMapping idParamMapping = new PersistableIdMapping((PersistableMapping) idMapping);
                    SQLExpression fieldExpr = exprFactory.newExpression(sqlStmtMain, sqlStmtMain.getPrimaryTable(), idMapping);
                    SQLExpression fieldVal = exprFactory.newLiteralParameter(sqlStmtMain, idParamMapping, id, "ID");
                    sqlStmtMain.whereAnd(fieldExpr.eq(fieldVal), true);
                }
                else
                {
                    SelectStatement sqlStmt = stmtGen.getStatement(ec);

                    // WHERE (object id) = ?
                    JavaTypeMapping idMapping = sqlStmt.getPrimaryTable().getTable().getIdMapping();
                    JavaTypeMapping idParamMapping = new PersistableIdMapping((PersistableMapping) idMapping);
                    SQLExpression fieldExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), idMapping);
                    SQLExpression fieldVal = exprFactory.newLiteralParameter(sqlStmt, idParamMapping, id, "ID");
                    sqlStmt.whereAnd(fieldExpr.eq(fieldVal), true);

                    sqlStmtMain.union(sqlStmt);
                }
            }
        }

        // Perform the query
        if (sqlStmtMain != null)
        {
            try
            {
                ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
                SQLController sqlControl = storeMgr.getSQLController();
                if (sampleCmd != null && ec.getSerializeReadForClass(sampleCmd.getFullClassName()))
                {
                    sqlStmtMain.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, true);
                }

                try
                {
                    PreparedStatement ps = SQLStatementHelper.getPreparedStatementForSQLStatement(sqlStmtMain, ec, mconn, null, null);
                    String statement = sqlStmtMain.getSQLText().toSQL();
                    try
                    {
                        ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, statement, ps);
                        try
                        {
                            while (rs.next())
                            {
                                try
                                {
                                    return rs.getString(UnionStatementGenerator.DN_TYPE_COLUMN).trim();
                                }
                                catch (SQLException sqle)
                                {
                                }
                            }
                        }
                        finally
                        {
                            rs.close();
                        }
                    }
                    finally
                    {
                        sqlControl.closeStatement(mconn, ps);
                    }
                }
                finally
                {
                    mconn.release();
                }
            }
            catch (SQLException sqe)
            {
                NucleusLogger.DATASTORE.error("Exception with UNION statement", sqe);
                throw new NucleusDataStoreException(sqe.toString());
            }
        }

        return null;
    }
}