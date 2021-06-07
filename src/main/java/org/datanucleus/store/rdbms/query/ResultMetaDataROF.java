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
import org.datanucleus.metadata.QueryResultMetaData.ConstructorTypeColumn;
import org.datanucleus.metadata.QueryResultMetaData.ConstructorTypeMapping;
import org.datanucleus.metadata.QueryResultMetaData.PersistentTypeMapping;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.types.converters.TypeConversionHelper;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.fieldmanager.ResultSetGetter;
import org.datanucleus.util.ClassUtils;
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
 * back to the user as a "simple" object, or as a field in a persistent object. 
 * So you could have a situation like this :-
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
 * So this example will return an Object[4] comprised of Object[0] = instance of PC1, Object[1] = instance of PC2, Object[2] = simple object, Object[3] = simple object. 
 * When creating the instance of PC1 we take the ResultSet columns (COL1, COL2, COL8).
 * When creating the instance of PC2 we take the ResultSet columns (COL5, COL6, COL4).
 * <h3>Columns to persistable object mapping</h3>
 * Where we have a number of columns forming a persistable object, such as (COL1, COL2, COL8) above we make use of ResultSetGetter
 * to populate the fields of the persistable object from the ResultSet.
 */
public class ResultMetaDataROF extends AbstractROF
{
    /** MetaData defining the result from the Query. */
    QueryResultMetaData queryResultMetaData = null;

    /** Column names in the ResultSet. */
    String[] columnNames = null;

    /** ResultSetGetter objects used for any persistable objects in the result. Set when processing the first row. */
    protected ResultSetGetter[] persistentTypeResultSetGetters = null;

    /**
     * Constructor.
     * @param ec ExecutionContext
     * @param rs ResultSet
     * @param ignoreCache Whether we should ignore the cache(s) when instantiating persistable objects
     * @param fp FetchPlan
     * @param qrmd MetaData defining the results from the query.
     */
    public ResultMetaDataROF(ExecutionContext ec, ResultSet rs, boolean ignoreCache, FetchPlan fp, QueryResultMetaData qrmd)
    {
        super(ec, rs, ignoreCache, fp);

        this.queryResultMetaData = qrmd;

        try
        {
            //obtain column names
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            columnNames = new String[columnCount];
            for (int i=0; i<columnCount; i++)
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.query.ResultObjectFactory#getResultSet()
     */
    @Override
    public ResultSet getResultSet()
    {
        return rs;
    }

    /**
     * Accessor for the object(s) from the current row of the ResultSet.
     * @return The object(s) for this row of the ResultSet.
     */
    public Object getObject()
    {
        List returnObjects = new ArrayList();

        // A). Process persistent types
        PersistentTypeMapping[] persistentTypes = queryResultMetaData.getPersistentTypeMappings();
        if (persistentTypes != null)
        {
            if (persistentTypeResultSetGetters == null)
            {
                persistentTypeResultSetGetters = new ResultSetGetter[persistentTypes.length];
            }

            int startColumnIndex = 0;
            for (int i=0;i<persistentTypes.length;i++)
            {
                Set<String> columnsInThisType = new HashSet<>();
                AbstractMemberMetaData[] mmds = new AbstractMemberMetaData[columnNames.length];
                Map<String, AbstractMemberMetaData> fieldColumns = new HashMap<>();
                DatastoreClass dc = ((RDBMSStoreManager)ec.getStoreManager()).getDatastoreClass(persistentTypes[i].getClassName(), ec.getClassLoaderResolver());
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
                        JavaTypeMapping datastoreIdMapping = dc.getSurrogateMapping(SurrogateColumnType.DATASTORE_ID, false);
                        Column df = datastoreIdMapping.getColumnMapping(0).getColumn();
                        if (df.getIdentifier().getName().equalsIgnoreCase(columnNames[j]))
                        {
                            //add +1 because result sets in jdbc starts with 1
                            int datastoreIdentityExpressionIndex = j+1;
                            id = datastoreIdMapping.getObject(ec, rs, new int[] {datastoreIdentityExpressionIndex});
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
                                mmds[j] = apmd;
                                found = true;
                            }
                        }
                        else
                        {
                            JavaTypeMapping mapping = dc.getMemberMapping(apmd);
                            for(int l=0; l<mapping.getColumnMappings().length && !found; l++)
                            {
                                Column df = mapping.getColumnMapping(l).getColumn();
                                if (df.getIdentifier().getName().equalsIgnoreCase(columnNames[j]))
                                {
                                    fieldColumns.put(columnNames[j], apmd);
                                    columnsInThisType.add(columnNames[j]);
                                    mmds[j] = apmd;
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
                StatementMappingIndex[] stmtMappings = new StatementMappingIndex[acmd.getNoOfManagedMembers() + acmd.getNoOfInheritedManagedMembers()];

                Set<AbstractMemberMetaData> resultMmds = new HashSet<>();
                resultMmds.addAll(fieldColumns.values());
                int[] resultFieldNumbers = new int[resultMmds.size()];
                int j=0;
                for (AbstractMemberMetaData apmd : resultMmds)
                {
                    StatementMappingIndex stmtMapping = new StatementMappingIndex(dc.getMemberMapping(apmd));

                    resultFieldNumbers[j] = apmd.getAbsoluteFieldNumber();
                    List indexes = new ArrayList();
                    for (int k=0; k<mmds.length; k++)
                    {
                        if (mmds[k] == apmd)
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
                    stmtMappings[resultFieldNumbers[j]] = stmtMapping;
                    j++;
                }

                Object obj = null;
                Class type = ec.getClassLoaderResolver().classForName(persistentTypes[i].getClassName());
                if (acmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    if (persistentTypeResultSetGetters[i] == null)
                    {
                        final StatementClassMapping resultMappings = new StatementClassMapping();
                        for (int k=0;k<resultFieldNumbers.length;k++)
                        {
                            resultMappings.addMappingForMember(resultFieldNumbers[k], stmtMappings[resultFieldNumbers[k]]);
                        }
                        persistentTypeResultSetGetters[i] = new ResultSetGetter(ec, rs, resultMappings, acmd);
                    }
                    ResultSetGetter rsGetter = persistentTypeResultSetGetters[i];

                    // TODO Make use of discriminator like in PersistentClassROF and set the pcClass in this?
                    id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, acmd, type, false, rsGetter);

                    obj = ec.findObject(id, new FieldValues()
                    {
                        public void fetchFields(ObjectProvider op)
                        {
                            rsGetter.setObjectProvider(op);
                            op.replaceFields(resultFieldNumbers, rsGetter, false);
                        }
                        public void fetchNonLoadedFields(ObjectProvider op)
                        {
                            rsGetter.setObjectProvider(op);
                            op.replaceNonLoadedFields(resultFieldNumbers, rsGetter);
                        }
                        public FetchPlan getFetchPlanForLoading()
                        {
                            return null;
                        }
                    }, type, ignoreCache, false);
                }
                else if (acmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    if (persistentTypeResultSetGetters[i] == null)
                    {
                        final StatementClassMapping resultMappings = new StatementClassMapping();
                        for (int k=0;k<resultFieldNumbers.length;k++)
                        {
                            resultMappings.addMappingForMember(resultFieldNumbers[k], stmtMappings[resultFieldNumbers[k]]);
                        }
                        persistentTypeResultSetGetters[i] = new ResultSetGetter(ec, rs, resultMappings, acmd);
                    }
                    ResultSetGetter rsGetter = persistentTypeResultSetGetters[i];

                    obj = ec.findObject(id, new FieldValues()
                    {
                        public void fetchFields(ObjectProvider op)
                        {
                            rsGetter.setObjectProvider(op);
                            op.replaceFields(resultFieldNumbers, rsGetter, false);
                        }
                        public void fetchNonLoadedFields(ObjectProvider op)
                        {
                            rsGetter.setObjectProvider(op);
                            op.replaceNonLoadedFields(resultFieldNumbers, rsGetter);
                        }
                        public FetchPlan getFetchPlanForLoading()
                        {
                            return null;
                        }
                    }, type, ignoreCache, false);
                }
                else
                {
                    // TODO Handle non-durable
                    NucleusLogger.QUERY.warn("We do not currently support non-durable objects in the results of this type of query.");
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
                    Object obj = rs.getObject(columns[i]);
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
                String ctrClassName = ctrTypeMappings[i].getClassName();
                Class ctrCls = ec.getClassLoaderResolver().classForName(ctrClassName);
                List<ConstructorTypeColumn> ctrColumns = ctrTypeMappings[i].getColumnsForConstructor();
                Class[] ctrArgTypes = null;
                Object[] ctrArgVals = null;

                if (ctrColumns != null && ctrColumns.size() > 0)
                {
                    int j=0;
                    ctrArgTypes = new Class[ctrColumns.size()];
                    ctrArgVals = new Object[ctrColumns.size()];
                    Iterator<ConstructorTypeColumn> colIter = ctrColumns.iterator();
                    while (colIter.hasNext())
                    {
                        ConstructorTypeColumn ctrCol = colIter.next();
                        try
                        {
                            Object colVal = rs.getObject(ctrCol.getColumnName());
                            ctrArgTypes[j] = colVal.getClass();
                            if (ctrCol.getJavaType() != null)
                            {
                                // Attempt to convert to the type requested
                                ctrArgTypes[j] = ctrCol.getJavaType();
                                ctrArgVals[j] = TypeConversionHelper.convertTo(colVal, ctrArgTypes[j]);
                            }
                            else
                            {
                                ctrArgTypes[j] = colVal.getClass();
                                ctrArgVals[j] = colVal;
                            }
                        }
                        catch (SQLException sqle)
                        {
                            // TODO Handle this
                        }
                        j++;
                    }
                }

                returnObjects.add(ClassUtils.newInstance(ctrCls, ctrArgTypes, ctrArgVals));
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
}