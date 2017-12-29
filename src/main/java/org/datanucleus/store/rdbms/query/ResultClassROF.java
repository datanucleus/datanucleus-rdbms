/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved.
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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Take a ResultSet, and for each row retrieves an object of a specified result class type.
 * Follows the rules in the JDO spec [14.6.12] regarding the result class.
 * <P>
 * The <B>resultClass</B> will be used to create objects of that type when calling
 * <I>getObject()</I>. The <B>resultClass</B> can be one of the following
 * <UL>
 * <LI>Simple type - String, Long, Integer, Float, Boolean, Byte, Character, Double, Short, BigDecimal, BigInteger, java.util.Date, java.sql.Date, java.sql.Time, java.sql.Timestamp</LI>
 * <LI>java.util.Map - DataNucleus will choose the concrete impl of java.util.Map to use</LI>
 * <LI>Object[]</LI>
 * <LI>User defined type with either a constructor taking the result set fields, or a default constructor 
 * and setting the fields using a put(Object,Object) method, setXXX methods, or public fields</LI>
 * </UL>
 *
 * <P>
 * Objects of this class are created in 2 distinct situations. 
 * The first is where a candidate class is available, and consequently field position mappings are available. 
 * The second is where no candidate class is available and so only the field names are available, and the results are taken in ResultSet order.
 * These 2 modes have their own constructor.
 */
public class ResultClassROF extends AbstractROF
{
    /** The result class that we should create for each row of results. */
    private final Class resultClass;

    /** The index of fields position to mapping type. */
    private final StatementMappingIndex[] stmtMappings;

    /** Definition of results when the query has a result clause. */
    private StatementResultMapping resultDefinition;

    /** Names of the result field columns (in the ResultSet). */
    private final String[] resultFieldNames;

    private final Class[] resultFieldTypes;

    /** Map of the ResultClass Fields, keyed by the field names (only for user-defined result classes). */
    private final Map resultClassFieldsByName = new HashMap();

    /**
     * Constructor for a resultClass object factory where we have a result clause specified.
     * @param ec ExecutionContext
     * @param rs ResultSet being processed
     * @param ignoreCache Whether we should ignore the cache(s) when instantiating persistable objects
     * @param cls The result class to use (if any)
     * @param resultDefinition The mapping information for the result expressions
     */
    public ResultClassROF(ExecutionContext ec, ResultSet rs, boolean ignoreCache, Class cls, StatementResultMapping resultDefinition)
    {
        super(ec, rs, ignoreCache);

        // Set the result class that we convert each row into
        Class tmpClass = null;
        if (cls != null && cls.getName().equals("java.util.Map"))
        {
            // JDO Spec 14.6.12 If user specifies java.util.Map, then impl chooses its own implementation Map class
            tmpClass = HashMap.class;
        }
        else if (cls == null)
        {
            // No result class specified so return Object/Object[] depending on number of expressions
            if (resultDefinition != null && resultDefinition.getNumberOfResultExpressions() == 1)
            {
                tmpClass = Object.class;
            }
            else
            {
                tmpClass = Object[].class;
            }
        }
        else
        {
            tmpClass = cls;
        }
        this.resultClass = tmpClass;

        this.resultDefinition = resultDefinition;
        this.stmtMappings = null;
        if (resultDefinition != null)
        {
            this.resultFieldNames = new String[resultDefinition.getNumberOfResultExpressions()];
            this.resultFieldTypes = new Class[resultDefinition.getNumberOfResultExpressions()];
            for (int i=0;i<resultFieldNames.length;i++)
            {
                Object stmtMap = resultDefinition.getMappingForResultExpression(i);
                if (stmtMap instanceof StatementMappingIndex)
                {
                    StatementMappingIndex idx = (StatementMappingIndex)stmtMap;
                    resultFieldNames[i] = idx.getColumnAlias();
                    resultFieldTypes[i] = idx.getMapping().getJavaType();
                }
                else if (stmtMap instanceof StatementNewObjectMapping)
                {
                    // TODO Handle this
                }
                else if (stmtMap instanceof StatementClassMapping)
                {
                    // TODO Handle this
                }
                else
                {
                    throw new NucleusUserException("Unsupported component " + stmtMap.getClass().getName() + " found in results");
                }
            }
        }
        else
        {
            this.resultFieldNames = null;
            this.resultFieldTypes = null;
        }
    }

    /**
     * Constructor for a resultClass object factory where we have no result clause specified but a result class.
     * In this case the result will match the candidate class, but may not be the actual candidate class (e.g Object[])
     * @param ec ExecutionContext
     * @param rs ResultSet being processed
     * @param ignoreCache Whether we should ignore the cache(s) when instantiating persistable objects
     * @param cls The result class to use
     * @param classDefinition The mapping information for the (candidate) class
     */
    public ResultClassROF(ExecutionContext ec, ResultSet rs, boolean ignoreCache, Class cls, StatementClassMapping classDefinition)
    {
        super(ec, rs, ignoreCache);

        // Set the result class that we convert each row into
        Class tmpClass = null;
        if (cls != null && cls.getName().equals("java.util.Map"))
        {
            // JDO Spec 14.6.12 If user specifies java.util.Map, then impl chooses its own implementation Map class
            tmpClass = HashMap.class;
        }
        else
        {
            tmpClass = cls;
        }
        this.resultClass = tmpClass;
        this.resultDefinition = null;

        // TODO Change underlying to just save the classDefinition
        int[] memberNumbers = classDefinition.getMemberNumbers();
        stmtMappings = new StatementMappingIndex[memberNumbers.length];
        this.resultFieldNames = new String[stmtMappings.length];
        this.resultFieldTypes = new Class[stmtMappings.length];
        for (int i=0;i<stmtMappings.length;i++)
        {
            stmtMappings[i] = classDefinition.getMappingForMemberPosition(memberNumbers[i]);
            resultFieldNames[i] = stmtMappings[i].getMapping().getMemberMetaData().getName();
            resultFieldTypes[i] = stmtMappings[i].getMapping().getJavaType();
        }
    }

    /**
     * Constructor for cases where we have no candidate class and so have no mapping information
     * to base field positions on. The fields will be retrieved in the ResultSet order.
     * Used for SQL queries.
     * @param ec ExecutionContext
     * @param rs ResultSet being processed
     * @param ignoreCache Whether we should ignore the cache(s) when instantiating persistable objects
     * @param cls The result class to use
     * @param resultFieldNames Names for the result fields
     */
    public ResultClassROF(ExecutionContext ec, ResultSet rs, boolean ignoreCache, Class cls, String[] resultFieldNames)
    {
        super(ec, rs, ignoreCache);

        Class tmpClass = null;
        if (cls != null && cls.getName().equals("java.util.Map"))
        {
            // JDO Spec 14.6.12 If user specifies java.util.Map, then impl chooses its own implementation Map class
            tmpClass = HashMap.class;
        }
        else
        {
            tmpClass = cls;
        }
        this.resultClass = tmpClass;

        if (QueryUtils.resultClassIsUserType(resultClass.getName()))
        {
            AccessController.doPrivileged(new PrivilegedAction()
            {
                public Object run()
                {
                    populateDeclaredFieldsForUserType(resultClass);
                    return null;
                }
            });
        }

        this.stmtMappings = null;
        this.resultFieldTypes = null;
        this.resultFieldNames = (resultFieldNames != null) ? resultFieldNames : new String[0];
    }

    /**
     * Method to convert the ResultSet row into an Object of the ResultClass type. 
     * We have a special handling for "result" expressions when they include literals or "new Object()" expression due to
     * the fact that complex literals and "new Object()" cannot be added to the SQL queries.
     * @return The ResultClass object.
     */
    public Object getObject()
    {
        // Retrieve the field values from the ResultSet
        Object[] fieldValues = null;
        if (resultDefinition != null)
        {
            fieldValues = new Object[resultDefinition.getNumberOfResultExpressions()];
            for (int i=0;i<resultDefinition.getNumberOfResultExpressions();i++)
            {
                Object stmtMap = resultDefinition.getMappingForResultExpression(i);
                if (stmtMap instanceof StatementMappingIndex)
                {
                    StatementMappingIndex idx = (StatementMappingIndex)stmtMap;
                    fieldValues[i] = idx.getMapping().getObject(ec, rs, idx.getColumnPositions());
                }
                else if (stmtMap instanceof StatementNewObjectMapping)
                {
                    StatementNewObjectMapping newIdx = (StatementNewObjectMapping)stmtMap;
                    fieldValues[i] = getValueForNewObject(newIdx, ec, rs);
                }
                else if (stmtMap instanceof StatementClassMapping)
                {
                    StatementClassMapping classMap = (StatementClassMapping)stmtMap;
                    Class cls = ec.getClassLoaderResolver().classForName(classMap.getClassName());
                    AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(cls, ec.getClassLoaderResolver());
                    PersistentClassROF rof = new PersistentClassROF(ec, rs, ignoreCache, classMap, acmd, cls);
                    fieldValues[i] = rof.getObject();

                    if (resultDefinition.getNumberOfResultExpressions() == 1)
                    {
                        if (classMap.getClassName().equals(resultClass.getName()))
                        {
                            // Special case of the result class being a persistent class so just return it
                            return fieldValues[0];
                        }
                    }
                }
            }
        }
        else if (stmtMappings != null)
        {
            // Field mapping information available so use it to allocate our results
            fieldValues = new Object[stmtMappings.length];
            for (int i=0; i<stmtMappings.length; i++)
            {
                if (stmtMappings[i] != null)
                {
                    fieldValues[i] = stmtMappings[i].getMapping().getObject(ec, rs, stmtMappings[i].getColumnPositions());
                }
                else
                {
                    fieldValues[i] = null;
                }
            }
        }
        else
        {
            // No field mapping info, so allocate our results in the ResultSet parameter order.
            try
            {
                if (resultFieldNames != null)
                {
                    fieldValues = new Object[resultFieldNames.length];
                    for (int i=0; i<fieldValues.length; i++)
                    {
                        fieldValues[i] = getResultObject(rs, i+1);
                    }
                }
            }
            catch (SQLException sqe)
            {
                String msg = Localiser.msg("021043", sqe.getMessage());
                NucleusLogger.QUERY.error(msg);
                throw new NucleusUserException(msg);
            }
        }

        // If the user requires Object[] then just give them what we have
        if (resultClass == Object[].class)
        {
            return fieldValues;
        }

        if (QueryUtils.resultClassIsSimple(resultClass.getName()))
        {
            // User wants a single field
            if (fieldValues.length == 1 && (fieldValues[0] == null || resultClass.isAssignableFrom(fieldValues[0].getClass())))
            {
                // Simple object is the correct type so just give them the field
                return fieldValues[0];
            }
            else if (fieldValues.length == 1 && !resultClass.isAssignableFrom(fieldValues[0].getClass()))
            {
                // Simple object is not assignable to the ResultClass so throw an error
                String msg = Localiser.msg("021202", resultClass.getName(), fieldValues[0].getClass().getName());
                NucleusLogger.QUERY.error(msg);
                throw new NucleusUserException(msg);
            }
        }
        else
        {
            // User requires creation of one of his own type of objects, or a Map
            if (fieldValues.length == 1 && fieldValues[0] != null && resultClass.isAssignableFrom(fieldValues[0].getClass()))
            {
                // Special case where user has selected a single field and is of same type as result class
                // TODO Cater for case where result field type is right type but value is null
                return fieldValues[0];
            }

            // A. Find a constructor with the correct constructor arguments
            Object obj = QueryUtils.createResultObjectUsingArgumentedConstructor(resultClass, fieldValues, resultFieldTypes);
            if (obj != null)
            {
                return obj;
            }
            else if (NucleusLogger.QUERY.isDebugEnabled())
            {
                // Give debug message that no constructor was found with the right args
                if (resultFieldNames != null)
                {
                    Class[] ctr_arg_types = new Class[resultFieldNames.length];
                    for (int i=0;i<resultFieldNames.length;i++)
                    {
                        ctr_arg_types[i] = (fieldValues[i] != null) ? fieldValues[i].getClass() : null;
                    }
                    NucleusLogger.QUERY.debug(Localiser.msg("021206", resultClass.getName(), StringUtils.objectArrayToString(ctr_arg_types)));
                }
                else
                {
                    StringBuilder str = new StringBuilder();
                    if (stmtMappings != null)
                    {
                        for (StatementMappingIndex stmtMapping : stmtMappings)
                        {
                            if (str.length() > 0)
                            {
                                str.append(",");
                            }
                            Class javaType = stmtMapping.getMapping().getJavaType();
                            str.append(javaType.getName());
                        }
                        NucleusLogger.QUERY.debug(Localiser.msg("021206", resultClass.getName(), str.toString()));
                    }
                }
            }

            // B. No argumented constructor exists so create an object and update fields using fields/put method/set method
            obj = QueryUtils.createResultObjectUsingDefaultConstructorAndSetters(resultClass, resultFieldNames, resultClassFieldsByName, fieldValues);

            return obj;
        }

        // Impossible to satisfy the resultClass requirements so throw exception
        String msg = Localiser.msg("021203",resultClass.getName());
        NucleusLogger.QUERY.error(msg);
        throw new NucleusUserException(msg);
    }

    /**
     * Convenience method to return the value of a NewObject mapping for the current row of the provided query results.
     * @param newMap new object mapping
     * @param ec ExecutionContext
     * @param rs Query results
     * @return The value of the new object
     */
    protected Object getValueForNewObject(StatementNewObjectMapping newMap, ExecutionContext ec, ResultSet rs)
    {
        Object value = null;

        if (newMap.getNumberOfConstructorArgMappings() == 0)
        {
            try
            {
                value = newMap.getObjectClass().newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusException("Attempt to create object for query result row of type " + newMap.getObjectClass().getName() + " threw an exception", e);
            }
        }
        else
        {
            int numArgs = newMap.getNumberOfConstructorArgMappings();
            Class[] ctrArgTypes = new Class[numArgs];
            Object[] ctrArgValues = new Object[numArgs];
            for (int i=0;i<numArgs;i++)
            {
                Object obj = newMap.getConstructorArgMapping(i);
                if (obj instanceof StatementMappingIndex)
                {
                    StatementMappingIndex idx = (StatementMappingIndex)obj;
                    ctrArgValues[i] = idx.getMapping().getObject(ec, rs, idx.getColumnPositions());
                }
                else if (obj instanceof StatementNewObjectMapping)
                {
                    ctrArgValues[i] = getValueForNewObject((StatementNewObjectMapping)obj, ec, rs);
                }
                else
                {
                    // Literal
                    ctrArgValues[i] = obj;
                }

                ctrArgTypes[i] = (ctrArgValues[i] != null) ? ctrArgValues[i].getClass() : null;
            }

            Constructor ctr = ClassUtils.getConstructorWithArguments(newMap.getObjectClass(), ctrArgTypes);
            if (ctr == null)
            {
                // Unable to work out which constructor to use, so give an informative message
                StringBuilder str = new StringBuilder(newMap.getObjectClass().getName() + "(");
                for (int i=0;i<ctrArgTypes.length;i++)
                {
                    if (ctrArgTypes[i] != null)
                    {
                        str.append(ctrArgTypes[i].getName());
                    }
                    else
                    {
                        str.append("(null)");
                    }

                    if (i != ctrArgTypes.length-1)
                    {
                        str.append(',');
                    }
                }
                str.append(")");

                throw new NucleusUserException(Localiser.msg("037013", str.toString()));
            }

            try
            {
                value = ctr.newInstance(ctrArgValues);
            }
            catch (Exception e)
            {
                throw new NucleusUserException(Localiser.msg("037015", newMap.getObjectClass().getName(), e));
            }
        }

        return value;
    }

    /**
     * Populate a map with the declared fields of the result class and super classes.
     * @param cls the class to find the declared fields and populate the map
     */
    private void populateDeclaredFieldsForUserType(Class cls)
    {
        Field[] declaredFields = cls.getDeclaredFields();
        for (Field field : declaredFields)
        {
            if (!field.isSynthetic() && resultClassFieldsByName.put(field.getName().toUpperCase(), field) != null && 
                !field.getName().startsWith(ec.getMetaDataManager().getEnhancedMethodNamePrefix()))
            {
                throw new NucleusUserException(Localiser.msg("021210", field.getName()));
            }
        }

        if (cls.getSuperclass() != null)
        {
            populateDeclaredFieldsForUserType(cls.getSuperclass());
        }
    }

    /**
     * Invokes a type-specific getter on given ResultSet
     */
    private static interface ResultSetGetter
    {
        Object getValue(ResultSet rs, int i) throws SQLException;
    }

    /** Map<Class, ResultSetGetter> ResultSetGetters by result classes */
    private static Map<Class, ResultSetGetter> resultSetGetters = new HashMap(15);
    static
    {
        // any type specific getter from ResultSet that we can guess from the desired result class
        resultSetGetters.put(Boolean.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return Boolean.valueOf(rs.getBoolean(i));
            }
        });
        resultSetGetters.put(Byte.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return Byte.valueOf(rs.getByte(i));
            }
        });
        resultSetGetters.put(Short.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return Short.valueOf(rs.getShort(i));
            }
        });
        resultSetGetters.put(Integer.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return Integer.valueOf(rs.getInt(i));
            }
        });
        resultSetGetters.put(Long.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return Long.valueOf(rs.getLong(i));
            }
        });
        resultSetGetters.put(Float.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return Float.valueOf(rs.getFloat(i));
            }
        });
        resultSetGetters.put(Double.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return Double.valueOf(rs.getDouble(i));
            }
        });

        resultSetGetters.put(BigDecimal.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return rs.getBigDecimal(i);
            }
        });
        resultSetGetters.put(String.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return rs.getString(i);
            }
        });

        ResultSetGetter timestampGetter = new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return rs.getTimestamp(i);
            }
        };
        resultSetGetters.put(java.sql.Timestamp.class, timestampGetter);
        // also use Timestamp getter for Date, so it also has time of the day e.g. with Oracle
        resultSetGetters.put(java.util.Date.class, timestampGetter);
        resultSetGetters.put(java.sql.Date.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return rs.getDate(i);
            }
        });

        resultSetGetters.put(byte[].class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return rs.getBytes(i);
            }
        });
        resultSetGetters.put(java.io.Reader.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return rs.getCharacterStream(i);
            }
        });
        resultSetGetters.put(java.sql.Array.class, new ResultSetGetter()
        {
            public Object getValue(ResultSet rs, int i) throws SQLException
            {
                return rs.getArray(i);
            }
        });
    }

    /**
     * Convenience method to read the value of a column out of the ResultSet.
     * @param rs ResultSet
     * @param columnNumber Number of the column (starting at 1)
     * @return Value for the column for this row.
     * @throws SQLException Thrown if an error occurs on reading
     */
    private Object getResultObject(final ResultSet rs, int columnNumber) throws SQLException
    {
        // use getter on ResultSet specific to our desired resultClass
        ResultSetGetter getter = resultSetGetters.get(resultClass);
        if (getter != null)
        {
            // User has specified a result type for this column so use the specific getter
            return getter.getValue(rs, columnNumber);
        }

        // User has specified Object/Object[] so just retrieve generically
        return rs.getObject(columnNumber);
    }
}