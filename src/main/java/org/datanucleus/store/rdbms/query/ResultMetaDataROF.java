/**********************************************************************
Copyright (c) 2007 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.query;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.QueryResultMetaData;
import org.datanucleus.metadata.QueryResultMetaData.ConstructorTypeMapping;
import org.datanucleus.metadata.QueryResultMetaData.PersistentTypeMapping;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * ResultObjectFactory that operates using a QueryResultMetaData and returns objects based on the definition.
 * A QueryResultMetaData allows for a row of a ResultSet to be returned as a mix of :-
 * <ul>
 * <li>a number of persistent objects (made up of several ResultSet columns)</li>
 * <li>a number of Objects (from individual ResultSet columns)</li>
 * </ul>
 * Each call to getObject() will then return a set of objects as per the MetaData definition.
 * <h3>ResultSet to object mapping</h3>
 * Each row of the ResultSet has a set of columns, and these columns are either used for direct outputting
 * back to the user as a "simple" object, or as a field in a persistent object. So you could have a situation
 * like this :-
 * <pre>
 * ResultSet Column   Output Object
 * ================   =============
 * COL1               PC1.field1
 * COL2               PC1.field2
 * COL3               Simple Object
 * COL4               PC2.field3
 * COL5               PC2.field1
 * COL6               PC2.field2
 * COL7               Simple Object
 * COL8               PC1.field3
 * ...
 * </pre>
 * So this example will return an Object[4] ... Object[0] = instance of PC1, Object[1] = instance of PC2,
 * Object[2] = simple object, Object[3] = simple object. 
 * When creating the instance of PC1 we take the ResultSet columns (COL1, COL2, COL8).
 * When creating the instance of PC2 we take the ResultSet columns (COL5, COL6, COL4).
 */
public class ResultMetaDataROF implements ResultObjectFactory
{
    RDBMSStoreManager storeMgr;

    /** MetaData defining the result from the Query. */
    QueryResultMetaData queryResultMetaData = null;

    String[] columnNames = null;

    private boolean ignoreCache = false;

    /**
     * Constructor.
     * @param storeMgr RDBMS StoreManager
     * @param qrmd MetaData defining the results from the query.
     */
    public ResultMetaDataROF(RDBMSStoreManager storeMgr, QueryResultMetaData qrmd)
    {
        this.storeMgr = storeMgr;
        this.queryResultMetaData = qrmd;
    }

    /**
     * Accessor for the object(s) from a row of the ResultSet.
     * @param ec execution context
     * @param rs ResultSet
     * @return The object(s) for this row of the ResultSet.
     */
    public Object getObject(ExecutionContext ec, ResultSet rs)
    {
        List returnObjects = new ArrayList();
        if (columnNames == null)
        {
            try
            {
                //obtain column names
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
                columnNames = new String[columnCount];
                for( int i=0; i<columnCount; i++)
                {
                    String colName = rsmd.getColumnName(i+1);
                    String colLabel = rsmd.getColumnLabel(i+1);
                    columnNames[i] = (StringUtils.isWhitespace(colLabel) ? colName : colLabel);
                }
            }
            catch(SQLException ex)
            {
                throw new NucleusDataStoreException("Error obtaining objects",ex);
            }
        }

        // A). Process persistent types
        PersistentTypeMapping[] persistentTypes = queryResultMetaData.getPersistentTypeMappings();
        if (persistentTypes != null)
        {
            int startColumnIndex = 0;
            for (int i=0;i<persistentTypes.length;i++)
            {
                Set columnsInThisType = new HashSet();
                AbstractMemberMetaData[] fmds = new AbstractMemberMetaData[columnNames.length];
                Map fieldColumns = new HashMap();
                DatastoreClass dc = storeMgr.getDatastoreClass(persistentTypes[i].getClassName(),
                    ec.getClassLoaderResolver());
                AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(persistentTypes[i].getClassName(), ec.getClassLoaderResolver());
                Object id = null;

                // Note that in this block we compare against the column name case-insensitive to attempt to catch
                // JDBC drivers that change the case of the columns that were passed in to the SQL statement. This
                // could potentially cause an issue if you're using a table which has case sensitive column names
                // and two columns with similar names e.g "Col1" and "col1". Until that situation comes up we ignore it :-)
                for (int j=startColumnIndex;j<columnNames.length;j++)
                {
                    if (columnsInThisType.contains(columnNames[j]))
                    {
                        //already added this column, so must be another persistent type
                        startColumnIndex = j;
                        break;
                    }

                    boolean found = false;
                    if (acmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        Column df = dc.getDatastoreIdMapping().getDatastoreMapping(0).getColumn();
                        if (df.getIdentifier().getName().equalsIgnoreCase(columnNames[j]))
                        {
                            //add +1 because result sets in jdbc starts with 1
                            int datastoreIdentityExpressionIndex = j+1;
                            //get object id if datastore identifier
                            if (dc.getDatastoreIdMapping() != null)
                            {
                                id = dc.getDatastoreIdMapping().getObject(ec, rs, 
                                    new int[] {datastoreIdentityExpressionIndex});
                            }
                            found=true;
                        }
                    }
                    for (int k=0; k<acmd.getNoOfManagedMembers()+acmd.getNoOfInheritedManagedMembers() && !found; k++)
                    {
                        AbstractMemberMetaData apmd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(k);
                        if (persistentTypes[i].getColumnForField(apmd.getName()) != null)
                        {
                            if (persistentTypes[i].getColumnForField(apmd.getName()).equalsIgnoreCase(columnNames[j]))
                            {
                                fieldColumns.put(columnNames[j], apmd);
                                columnsInThisType.add(columnNames[j]);
                                fmds[j] = apmd;
                                found = true;
                            }
                        }
                        else
                        {
                            JavaTypeMapping mapping = dc.getMemberMapping(apmd);
                            for(int l=0; l<mapping.getDatastoreMappings().length && !found; l++)
                            {
                                Column df = mapping.getDatastoreMapping(l).getColumn();
                                if (df.getIdentifier().getName().equalsIgnoreCase(columnNames[j]))
                                {
                                    fieldColumns.put(columnNames[j], apmd);
                                    columnsInThisType.add(columnNames[j]);
                                    fmds[j] = apmd;
                                    found = true;
                                }
                            }
                        }
                    }
                    if (!columnsInThisType.contains(columnNames[j]))
                    {
                        //column not found in this type, so must be another persistent type
                        startColumnIndex = j;
                        break;
                    }
                }

                // Build fields and mappings in the results
                StatementMappingIndex[] stmtMappings = 
                    new StatementMappingIndex[acmd.getNoOfManagedMembers() + acmd.getNoOfInheritedManagedMembers()];

                Set fields = new HashSet();
                fields.addAll(fieldColumns.values());
                int[] fieldNumbers = new int[fields.size()];
                Iterator it = fields.iterator();
                int j=0;
                while (it.hasNext())
                {
                    AbstractMemberMetaData apmd = (AbstractMemberMetaData)it.next();
                    StatementMappingIndex stmtMapping = new StatementMappingIndex(dc.getMemberMapping(apmd));

                    fieldNumbers[j] = apmd.getAbsoluteFieldNumber();
                    List indexes = new ArrayList();
                    for (int k=0; k<fmds.length; k++)
                    {
                        if (fmds[k] == apmd)
                        {
                            indexes.add(Integer.valueOf(k));
                        }
                    }
                    int[] indxs = new int[indexes.size()];
                    for( int k=0; k<indxs.length; k++)
                    {
                        //add +1 because result sets in JDBC starts with 1
                        indxs[k] = ((Integer)indexes.get(k)).intValue()+1;
                    }
                    stmtMapping.setColumnPositions(indxs);
                    stmtMappings[fieldNumbers[j]] = stmtMapping;
                    j++;
                }
                Object obj = null;
                Class type = ec.getClassLoaderResolver().classForName(persistentTypes[i].getClassName());
                if (acmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    obj = getObjectForApplicationId(ec, rs, fieldNumbers, acmd, type, false, stmtMappings);
                }
                else if (acmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    obj = getObjectForDatastoreId(ec, rs, fieldNumbers, acmd, id, type, stmtMappings);
                }
                returnObjects.add(obj);
            }
        }

        // B). Process simple columns
        String[] columns = queryResultMetaData.getScalarColumns();
        if (columns != null)
        {
            for (int i=0;i<columns.length;i++)
            {
                try
                {
                    Object obj = getResultObject(rs, columns[i]);
                    returnObjects.add(obj);
                }
                catch (SQLException sqe)
                {
                    String msg = Localiser.msg("059027", sqe.getMessage());
                    NucleusLogger.QUERY.error(msg);
                    throw new NucleusUserException(msg, sqe);
                }
            }
        }

        // C). Process constructor type mappings
        ConstructorTypeMapping[] ctrTypeMappings = queryResultMetaData.getConstructorTypeMappings();
        if (ctrTypeMappings != null)
        {
            for (int i=0;i<ctrTypeMappings.length;i++)
            {
                // TODO Support this
                NucleusLogger.QUERY.warn("Dont currently support Constructor mappings in result");
            }
        }

        if (returnObjects.size() == 0)
        {
            // No objects so user must have supplied incorrect MetaData
            return null;
        }
        else if (returnObjects.size() == 1)
        {
            // Return Object
            return returnObjects.get(0);
        }
        else
        {
            // Return Object[]
            return returnObjects.toArray(new Object[returnObjects.size()]);
        }
    }

    /**
     * Convenience method to read the value of a column out of the ResultSet.
     * @param rs ResultSet
     * @param columnName Name of the column
     * @return Value for the column for this row.
     * @throws SQLException Thrown if an error occurs on reading
     */
    private Object getResultObject(final ResultSet rs, String columnName)
    throws SQLException
    {
        return rs.getObject(columnName);
    }

    /**
     * Returns a PC instance from a ResultSet row with an application identity.
     * @param ec execution context
     * @param rs The ResultSet
     * @param fieldNumbers Numbers of the fields (of the class) found in the ResultSet
     * @param cmd MetaData for the class
     * @param pcClass persistable class
     * @param requiresInheritanceCheck Whether we need to check the inheritance level of the returned object
     * @param stmtMappings mappings for the results in the statement
     * @return The object with this application identity
     */
    private Object getObjectForApplicationId(final ExecutionContext ec, final ResultSet rs, final int[] fieldNumbers, 
            AbstractClassMetaData cmd, Class pcClass, boolean requiresInheritanceCheck, 
            StatementMappingIndex[] stmtMappings)
    {
        final StatementClassMapping resultMappings = new StatementClassMapping();
        for (int i=0;i<fieldNumbers.length;i++)
        {
            resultMappings.addMappingForMember(fieldNumbers[i], stmtMappings[fieldNumbers[i]]);
        }

        Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, requiresInheritanceCheck, 
            storeMgr.getFieldManagerForResultProcessing(ec, rs, resultMappings, cmd));

        return ec.findObject(id, new FieldValues()
        {
            public void fetchFields(ObjectProvider sm)
            {
                FieldManager fm = storeMgr.getFieldManagerForResultProcessing(sm, rs, resultMappings);
                sm.replaceFields(fieldNumbers, fm, false);
            }
            public void fetchNonLoadedFields(ObjectProvider sm)
            {
                FieldManager fm = storeMgr.getFieldManagerForResultProcessing(sm, rs, resultMappings);
                sm.replaceNonLoadedFields(fieldNumbers, fm);
            }
            public FetchPlan getFetchPlanForLoading()
            {
                return null;
            }
        }, pcClass, ignoreCache, false);
    }

    /**
     * Returns a PC instance from a ResultSet row with a datastore identity.
     * @param ec execution context
     * @param rs The ResultSet
     * @param fieldNumbers Numbers of the fields (of the class) found in the ResultSet
     * @param cmd MetaData for the class
     * @param oid The object id
     * @param pcClass The persistable class (where we know the instance type required, null if not)
     * @param stmtMappings mappings for the results in the statement
     * @return The Object
     */
    private Object getObjectForDatastoreId(final ExecutionContext ec, final ResultSet rs, final int[] fieldNumbers,
            AbstractClassMetaData cmd, Object oid, Class pcClass, StatementMappingIndex[] stmtMappings)
    {
        final StatementClassMapping resultMappings = new StatementClassMapping();
        for (int i=0;i<fieldNumbers.length;i++)
        {
            resultMappings.addMappingForMember(fieldNumbers[i], stmtMappings[fieldNumbers[i]]);
        }

        return ec.findObject(oid, new FieldValues()
        {
            public void fetchFields(ObjectProvider sm)
            {
                FieldManager fm = storeMgr.getFieldManagerForResultProcessing(sm, rs, resultMappings);
                sm.replaceFields(fieldNumbers, fm, false);
            }
            public void fetchNonLoadedFields(ObjectProvider sm)
            {
                FieldManager fm = storeMgr.getFieldManagerForResultProcessing(sm, rs, resultMappings);
                sm.replaceNonLoadedFields(fieldNumbers, fm);
            }
            public FetchPlan getFetchPlanForLoading()
            {
                return ec.getFetchPlan();
            }
        }, pcClass, ignoreCache, false);
    }
}