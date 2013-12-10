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
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableIdMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.query.RDBMSQueryUtils;
import org.datanucleus.store.rdbms.sql.DiscriminatorStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.StatementGenerator;
import org.datanucleus.store.rdbms.sql.UnionStatementGenerator;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.util.NucleusLogger;

/**
 * Provides a series of utilities assisting in the datastore management process for RDBMS datastores.
 */
public class RDBMSStoreHelper
{
    /**
     * Utility that does a discriminator candidate query for the specified candidate and subclasses
     * and returns the class name of the instance that has the specified identity (if any).
     * @param storeMgr RDBMS StoreManager
     * @param ec execution context
     * @param id The id
     * @param cmd Metadata for the root candidate class
     * @return Name of the class with this identity (or null if none found)
     */
    public static String getClassNameForIdUsingDiscriminator(RDBMSStoreManager storeMgr, ExecutionContext ec, Object id,
            AbstractClassMetaData cmd)
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
        DiscriminatorStatementGenerator stmtGen =
            new DiscriminatorStatementGenerator(storeMgr, clr, clr.classForName(cmd.getFullClassName()), true,
                null, null);
        stmtGen.setOption(StatementGenerator.OPTION_RESTRICT_DISCRIM);
        SQLStatement sqlStmt = stmtGen.getStatement();

        // Select the discriminator
        JavaTypeMapping discrimMapping = primaryTable.getDiscriminatorMapping(true);
        SQLTable discrimSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), discrimMapping);
        sqlStmt.select(discrimSqlTbl, discrimMapping, null);

        // Restrict to this id
        JavaTypeMapping idMapping = primaryTable.getIdMapping();
        JavaTypeMapping idParamMapping = new PersistableIdMapping((PersistableMapping) idMapping);
        SQLExpression sqlFldExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), idMapping);
        SQLExpression sqlFldVal = exprFactory.newLiteralParameter(sqlStmt, idParamMapping, id, "ID");
        sqlStmt.whereAnd(sqlFldExpr.eq(sqlFldVal), true);

        // Perform the query
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            if (ec.getSerializeReadForClass(cmd.getFullClassName()))
            {
                sqlStmt.addExtension("lock-for-update", true);
            }

            try
            {
                PreparedStatement ps =
                    SQLStatementHelper.getPreparedStatementForSQLStatement(sqlStmt, ec, mconn, null, null);
                String statement = sqlStmt.getSelectStatement().toSQL();
                try
                {
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, statement, ps);
                    try
                    {
                        if (rs != null)
                        {
                            while (rs.next())
                            {
                                DiscriminatorMetaData dismd = discrimMapping.getTable().getDiscriminatorMetaData();
                                return RDBMSQueryUtils.getClassNameFromDiscriminatorResultSetRow(
                                    discrimMapping, dismd, rs, ec);
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
    public static String getClassNameForIdUsingUnion(RDBMSStoreManager storeMgr, ExecutionContext ec, Object id,
            List<AbstractClassMetaData> rootCmds)
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
        SQLStatement sqlStmtMain = null;
        while (rootCmdIter.hasNext())
        {
            AbstractClassMetaData rootCmd = rootCmdIter.next();
            DatastoreClass rootTbl = storeMgr.getDatastoreClass(rootCmd.getFullClassName(), clr);
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
                        UnionStatementGenerator stmtGen =
                            new UnionStatementGenerator(storeMgr, clr, clr.classForName(subcmds[i].getFullClassName()), true,
                                null, null);
                        stmtGen.setOption(StatementGenerator.OPTION_SELECT_NUCLEUS_TYPE);
                        if (sqlStmtMain == null)
                        {
                            sampleCmd = subcmds[i];
                            sqlStmtMain = stmtGen.getStatement();
                        }
                        else
                        {
                            SQLStatement sqlStmt = stmtGen.getStatement();
                            sqlStmtMain.union(sqlStmt);
                        }
                    }
                }
            }
            else
            {
                UnionStatementGenerator stmtGen =
                    new UnionStatementGenerator(storeMgr, clr, clr.classForName(rootCmd.getFullClassName()), true,
                        null, null);
                stmtGen.setOption(StatementGenerator.OPTION_SELECT_NUCLEUS_TYPE);
                if (sqlStmtMain == null)
                {
                    sampleCmd = rootCmd;
                    sqlStmtMain = stmtGen.getStatement();
                }
                else
                {
                    SQLStatement sqlStmt = stmtGen.getStatement();
                    sqlStmtMain.union(sqlStmt);
                }
            }
        }

        // WHERE (object id) = ?
        JavaTypeMapping idMapping = sqlStmtMain.getPrimaryTable().getTable().getIdMapping();
        JavaTypeMapping idParamMapping = new PersistableIdMapping((PersistableMapping) idMapping);
        SQLExpression fieldExpr = exprFactory.newExpression(sqlStmtMain, sqlStmtMain.getPrimaryTable(), idMapping);
        SQLExpression fieldVal = exprFactory.newLiteralParameter(sqlStmtMain, idParamMapping, id, "ID");
        sqlStmtMain.whereAnd(fieldExpr.eq(fieldVal), true);

        // Perform the query
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            if (ec.getSerializeReadForClass(sampleCmd.getFullClassName()))
            {
                sqlStmtMain.addExtension("lock-for-update", true);
            }

            try
            {
                PreparedStatement ps =
                    SQLStatementHelper.getPreparedStatementForSQLStatement(sqlStmtMain, ec, mconn, null, null);
                String statement = sqlStmtMain.getSelectStatement().toSQL();
                try
                {
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, statement, ps);
                    try
                    {
                        if (rs != null)
                        {
                            while (rs.next())
                            {
                                try
                                {
                                    return rs.getString(UnionStatementGenerator.NUC_TYPE_COLUMN).trim();
                                }
                                catch (SQLException sqle)
                                {
                                }
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
            NucleusLogger.DATASTORE.error(sqe);
            throw new NucleusDataStoreException(sqe.toString());
        }

        return null;
    }
}