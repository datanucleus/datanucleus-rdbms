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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.query.QueryUtils;
import org.datanucleus.store.types.converters.TypeConversionHelper;
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
    private final Class<?> resultClass;

    /** The index of fields position to mapping type. */
    private final StatementMappingIndex[] stmtMappings;

    /** Definition of results when the query has a result clause. */
    private StatementResultMapping resultDefinition;

    /** Names of the result field columns (in the ResultSet). */
    private final String[] resultFieldNames;

    /** Types of the result field columns (in the ResultSet). */
    private final Class[] resultFieldTypes;

    /** Map of the ResultClass Fields, keyed by the "field" names (only for user-defined result classes). */
    private final Map<String, Field> resultClassFieldsByName = new HashMap<>();

    boolean constructionDefined = false;
    private Constructor resultClassArgConstructor = null;
    private Constructor resultClassDefaultConstructor = null;
    private ResultClassMemberSetter[] resultClassMemberSetters = null;

    /**
     * Constructor for a resultClass object factory where we have a result clause specified.
     * @param ec ExecutionContext
     * @param rs ResultSet being processed
     * @param fp FetchPlan
     * @param cls The result class to use (if any)
     * @param resultDefinition The mapping information for the result expressions
     */
    public ResultClassROF(ExecutionContext ec, ResultSet rs, FetchPlan fp, Class cls, StatementResultMapping resultDefinition)
    {
        super(ec, rs, fp);

        // Set the result class that we convert each row into
        if (cls != null && cls.getName().equals("java.util.Map"))
        {
            // JDO Spec 14.6.12 If user specifies java.util.Map, then impl chooses its own implementation Map class
            this.resultClass = HashMap.class;
        }
        else if (cls == null)
        {
            // No result class specified so return Object/Object[] depending on number of expressions
            this.resultClass = (resultDefinition != null && resultDefinition.getNumberOfResultExpressions() == 1) ? Object.class : Object[].class;
        }
        else
        {
            this.resultClass = cls;
        }

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
     * @param cls The result class to use
     * @param classDefinition The mapping information for the (candidate) class
     */
    public ResultClassROF(ExecutionContext ec, ResultSet rs, Class cls, StatementClassMapping classDefinition)
    {
        super(ec, rs, null);

        // JDO Spec 14.6.12 If user specifies java.util.Map, then impl chooses its own implementation Map class
        this.resultClass = (cls != null && cls.getName().equals("java.util.Map")) ? HashMap.class : cls;
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
     * Constructor for cases where we have no candidate class and so have no mapping information to base field positions on. 
     * The fields will be retrieved in the ResultSet order. Used for SQL queries.
     * @param ec ExecutionContext
     * @param rs ResultSet being processed
     * @param cls The result class to use
     * @param resultFieldNames Names for the result fields
     */
    public ResultClassROF(ExecutionContext ec, ResultSet rs, Class cls, String[] resultFieldNames)
    {
        super(ec, rs, null);

        // JDO Spec 14.6.12 If user specifies java.util.Map, then impl chooses its own implementation Map class
        this.resultClass = (cls != null && cls.getName().equals("java.util.Map")) ? HashMap.class : cls;

        if (QueryUtils.resultClassIsUserType(resultClass.getName()))
        {
            populateDeclaredFieldsForUserType(resultClass);
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
        Object[] resultFieldValues = null;
        if (resultDefinition != null)
        {
            resultFieldValues = new Object[resultDefinition.getNumberOfResultExpressions()];
            for (int i=0;i<resultDefinition.getNumberOfResultExpressions();i++)
            {
                Object stmtMap = resultDefinition.getMappingForResultExpression(i);
                if (stmtMap instanceof StatementMappingIndex)
                {
                    StatementMappingIndex idx = (StatementMappingIndex)stmtMap;
                    resultFieldValues[i] = idx.getMapping().getObject(ec, rs, idx.getColumnPositions());
                }
                else if (stmtMap instanceof StatementNewObjectMapping)
                {
                    StatementNewObjectMapping newIdx = (StatementNewObjectMapping)stmtMap;
                    resultFieldValues[i] = getValueForNewObject(newIdx, ec, rs);
                }
                else if (stmtMap instanceof StatementClassMapping)
                {
                    StatementClassMapping classMap = (StatementClassMapping)stmtMap;
                    Class<?> cls = ec.getClassLoaderResolver().classForName(classMap.getClassName());
                    AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(cls, ec.getClassLoaderResolver());
                    PersistentClassROF rof = new PersistentClassROF<>(ec, rs, fp, classMap, acmd, cls);
                    rof.setIgnoreCache(ignoreCache);
                    resultFieldValues[i] = rof.getObject();

                    if (resultDefinition.getNumberOfResultExpressions() == 1)
                    {
                        if (classMap.getClassName().equals(resultClass.getName()))
                        {
                            // Special case of the result class being a persistent class so just return it
                            return resultFieldValues[0];
                        }
                    }
                }
            }
        }
        else if (stmtMappings != null)
        {
            // Field mapping information available so use it to allocate our results
            resultFieldValues = new Object[stmtMappings.length];
            for (int i=0; i<stmtMappings.length; i++)
            {
                resultFieldValues[i] = (stmtMappings[i] != null) ? stmtMappings[i].getMapping().getObject(ec, rs, stmtMappings[i].getColumnPositions()) : null;
            }
        }
        else
        {
            // No field mapping info, so allocate our results in the ResultSet parameter order.
            try
            {
                resultFieldValues = new Object[resultFieldNames != null ? resultFieldNames.length : 0];
                for (int i=0; i<resultFieldValues.length; i++)
                {
                    resultFieldValues[i] = getResultObject(rs, i+1);
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
            return resultFieldValues;
        }

        if (QueryUtils.resultClassIsSimple(resultClass.getName()))
        {
            if (resultFieldValues.length == 1)
            {
                if (resultFieldValues[0] == null)
                {
                    return null;
                }
                Object theValue = TypeConversionHelper.convertTo(resultFieldValues[0], resultClass);
                if (theValue != null && resultClass.isAssignableFrom(theValue.getClass()))
                {
                    // TODO If we had an error handler from TypeConversionHelper.convertTo then we could detect failure to convert
                    return theValue;
                }
            }
        }
        else if (resultFieldValues.length == 1 && resultFieldValues[0] != null && resultClass.isAssignableFrom(resultFieldValues[0].getClass()))
        {
            // Special case where user has selected a single field and is of same type as result class
            // TODO Cater for case where result field type is right type but value is null
            return resultFieldValues[0];
        }
        else
        {
            // Subsequent row
            if (constructionDefined)
            {
                if (resultClassArgConstructor != null)
                {
                    return QueryUtils.createResultObjectUsingArgumentedConstructor(resultClassArgConstructor, resultFieldValues);
                }
                else if (resultClassDefaultConstructor != null)
                {
                    Object obj = null;
                    try
                    {
                        // Create the object
                        obj = resultClassDefaultConstructor.newInstance();
                    }
                    catch (Exception e)
                    {
                        String msg = Localiser.msg("021205", resultClass.getName());
                        NucleusLogger.QUERY.error(msg);
                        throw new NucleusUserException(msg);
                    }

                    for (int i=0;i<resultFieldValues.length;i++)
                    {
                        // Update the fields of our object with the field values
                        ResultClassMemberSetter setMember = resultClassMemberSetters[i];
                        if (setMember != null)
                        {
                            boolean set = setMember.set(obj, resultFieldNames[i], resultFieldValues[i]);
                            if (!set)
                            {
                                String fieldType = (resultFieldValues[i] != null) ? resultFieldValues[i].getClass().getName() : "null";
                                String msg = Localiser.msg("021204", resultClass.getName(), resultFieldNames[i], fieldType);
                                NucleusLogger.QUERY.error(msg);
                                throw new NucleusUserException(msg);
                            }
                        }
                    }

                    return obj;
                }
            }

            // First row
            if (!constructionDefined)
            {
                // A. Find a constructor with the correct constructor arguments
                Constructor ctr = QueryUtils.getResultClassConstructorForArguments(resultClass, resultFieldTypes, resultFieldValues);
                if (ctr != null)
                {
                    try
                    {
                        Object obj = ctr.newInstance(resultFieldValues);
                        if (NucleusLogger.QUERY.isDebugEnabled())
                        {
                            NucleusLogger.QUERY.debug(Localiser.msg("021216", resultClass.getName(), StringUtils.objectArrayToString(resultFieldValues)));
                        }
                        resultClassArgConstructor = ctr;
                        constructionDefined = true;
                        return obj;
                    }
                    catch (Exception e)
                    {
                        // do nothing
                    }
                }
                else
                {
                    if (NucleusLogger.QUERY.isDebugEnabled())
                    {
                        // Give debug message that no constructor was found with the right args
                        if (resultFieldNames != null)
                        {
                            Class[] ctrArgTypes = new Class[resultFieldNames.length];
                            for (int i=0;i<resultFieldNames.length;i++)
                            {
                                ctrArgTypes[i] = (resultFieldValues[i] != null) ? resultFieldValues[i].getClass() : null;
                            }
                            if (NucleusLogger.QUERY.isDebugEnabled())
                            {
                                NucleusLogger.QUERY.debug(Localiser.msg("021206", resultClass.getName(), StringUtils.objectArrayToString(ctrArgTypes)));
                            }
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

                    // No argumented constructor, so create using default constructor
                    Object obj = null;
                    try
                    {
                        ctr = resultClass.getDeclaredConstructor();
                        obj = ctr.newInstance();
                    }
                    catch (Exception e)
                    {
                        String msg = Localiser.msg("021205", resultClass.getName());
                        NucleusLogger.QUERY.error(msg);
                        throw new NucleusUserException(msg);
                    }
                    if (NucleusLogger.QUERY.isDebugEnabled())
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("021217", resultClass.getName()));
                    }
                    resultClassDefaultConstructor = ctr;

                    // Extract appropriate member for setting each Field
                    resultClassMemberSetters = new ResultClassMemberSetter[resultFieldValues.length];
                    for (int i=0;i<resultFieldValues.length;i++)
                    {
                        // Update the fields of our object with the field values
                        String fieldName = resultFieldNames[i];
                        Field field = resultClassFieldsByName.get(fieldName.toUpperCase());
                        Object value = resultFieldValues[i];

                        // Try (public) field
                        String declaredFieldName = (field != null) ? field.getName() : fieldName;
                        Field f = ClassUtils.getFieldForClass(resultClass, declaredFieldName);
                        if (f != null && Modifier.isPublic(f.getModifiers()))
                        {
                            resultClassMemberSetters[i] = new ResultClassFieldSetter(f);
                            resultClassMemberSetters[i].set(obj, declaredFieldName, value);
                        }

                        if (resultClassMemberSetters[i] == null)
                        {
                            // Try (public) setXXX() method
                            String setMethodName = (field != null) ? 
                                    "set" + fieldName.substring(0,1).toUpperCase() + field.getName().substring(1) : 
                                        "set" + fieldName.substring(0,1).toUpperCase() + fieldName.substring(1);
                            Class argType = (value != null) ? value.getClass() : (field != null) ? field.getType() : null;
                            Method setMethod = ClassUtils.getMethodWithArgument(resultClass, setMethodName, argType);
                            if (setMethod != null && Modifier.isPublic(setMethod.getModifiers()))
                            {
                                // Where a set method with the exact argument type exists use it. Set argType as it might
                                // be used for conversion, in case we don't now the value type at this stage for null values.
                                try
                                {
                                    resultClassMemberSetters[i] = new ResultClassSetMethodSetter(setMethod, argType);
                                    resultClassMemberSetters[i].set(obj, fieldName, value);
                                }
                                catch (Exception e)
                                {
                                    //do nothing
                                }
                            }
                            else if (setMethod == null)
                            {
                                // Find a method with the right name and a single argument
                                Method[] methods = resultClass.getDeclaredMethods();
                                for (int j=0;j<methods.length;j++)
                                {
                                    Class[] args = methods[j].getParameterTypes();
                                    if (methods[j].getName().equals(setMethodName) && Modifier.isPublic(methods[j].getModifiers()) && args != null && args.length == 1)
                                    {
                                        try
                                        {
                                            resultClassMemberSetters[i] = new ResultClassSetMethodSetter(methods[j], args[0]);
                                            resultClassMemberSetters[i].set(obj, fieldName, value);
                                            break;
                                        }
                                        catch (Exception e)
                                        {
                                            //do nothing
                                        }
                                    }
                                }
                            }
                            if (resultClassMemberSetters[i] == null)
                            {
                                if (NucleusLogger.QUERY.isDebugEnabled())
                                {
                                    NucleusLogger.QUERY.debug(Localiser.msg("021207", obj.getClass().getName(), setMethodName, argType != null ? argType.getName() : null));
                                }
                            }
                        }

                        if (resultClassMemberSetters[i] == null)
                        {
                            // Try (public) put() method
                            Method putMethod = QueryUtils.getPublicPutMethodForResultClass(resultClass);
                            if (putMethod != null)
                            {
                                try
                                {
                                    resultClassMemberSetters[i] = new ResultClassPutMethodSetter(putMethod);
                                    resultClassMemberSetters[i].set(obj, fieldName, value);
                                }
                                catch (Exception e)
                                {
                                    //do nothing
                                }
                            }
                            else
                            {
                                if (NucleusLogger.QUERY.isDebugEnabled())
                                {
                                    NucleusLogger.QUERY.debug(Localiser.msg("021208", obj.getClass().getName(), "put"));
                                }
                            }
                        }

                        if (resultClassMemberSetters[i] == null)
                        {
                            // Impossible to set the field value
                            if (field == null) 
                            {
                                // Field was null, and impossible to find setter etc either. Column doesn't exist in result class
                                NucleusLogger.QUERY.info(Localiser.msg("021215", resultFieldNames[i]));
                            }
                            else
                            {
                                String fieldType = (resultFieldValues[i] != null) ? resultFieldValues[i].getClass().getName() : "null";
                                String msg = Localiser.msg("021204", resultClass.getName(), resultFieldNames[i], fieldType);
                                NucleusLogger.QUERY.error(msg);
                                throw new NucleusUserException(msg);
                            }
                        }
                    }

                    constructionDefined = true;
                    return obj;
                }
            }
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
                value = newMap.getObjectClass().getDeclaredConstructor().newInstance();
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
            if (!field.isSynthetic() && !field.getName().startsWith(ec.getMetaDataManager().getEnhancedMethodNamePrefix()))
            {
                Field currValue = resultClassFieldsByName.put(field.getName().toUpperCase(), field);
                if (currValue != null)
                {
                    if (currValue.getName().equals(field.getName()))
                    {
                        // Shadowed field, same name but in superclass. Ignore for now. TODO Allow setting of this
                        NucleusLogger.QUERY.info("Result column=" + field.getName().toUpperCase() + " is already mapped to \"" + currValue.toString() + "\"" +
                            " but result class also has \"" + field.toString() + "\"; this latter field will not be set.");
                    }
                    else
                    {
                        // Case sensitive field name, whereas column name is not
                        throw new NucleusUserException(Localiser.msg("021210", field.getName()));
                    }
                }
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
    private static Map<Class, ResultSetGetter> resultSetGetters = new HashMap<>(15);
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
     * Uses "rs.getBoolean", "rs.getInt", etc, otherwise "rs.getObject".
     * @param rs ResultSet
     * @param columnNumber Number of the column (starting at 1)
     * @return Value for the column for this row.
     * @throws SQLException Thrown if an error occurs on reading
     */
    private Object getResultObject(final ResultSet rs, int columnNumber) throws SQLException
    {
        // Where the result class is a basic type
        ResultSetGetter getter = resultSetGetters.get(resultClass);
        if (getter != null)
        {
            // User has specified a result type for this column so use the specific getter
            return getter.getValue(rs, columnNumber);
        }

        // User has specified resultClass as Object/Object[] or user-type so just retrieve generically
        // TODO We could pass in the "type" that we expect the column to be (whether field, or constructor arg etc), and then use getXXX here
        return rs.getObject(columnNumber);
    }

    interface ResultClassMemberSetter
    {
        public boolean set(Object obj, String fieldName, Object value);
    }

    /**
     * Class that sets a field of the ResultClass using a public field.
     * This is done either using the value passed in directly, or by first converting it to the required field type.
     */
    class ResultClassFieldSetter implements ResultClassMemberSetter
    {
        Field field;
        public ResultClassFieldSetter(Field f)
        {
            this.field = f;
        }

        public boolean set(Object obj, String fieldName, Object value)
        {
            Object fieldValue = value;
            if (value != null && !field.getType().isAssignableFrom(value.getClass()))
            {
                // Field is not of assignable type so try to convert it
                Object convertedValue = TypeConversionHelper.convertTo(value, field.getType());
                if (convertedValue != value)
                {
                    fieldValue = convertedValue;
                }
                else
                {
                    // TODO Flag the error here
                }
            }

            try
            {
                field.set(obj, fieldValue);
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    if (fieldValue != value)
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("021219", resultClass.getName(), fieldName));
                    }
                    else
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("021218", resultClass.getName(), fieldName));
                    }
                }
                return true;
            }
            catch (Exception e)
            {
            	// Maybe we need to update TypeConversionHelper with further conversions
                NucleusLogger.DATASTORE_RETRIEVE.warn("Unable to convert query value of type " + value.getClass().getName() + " to field of type " + field.getType().getName());
            }

            return false;
        }
    }

    /**
     * Class that sets a field of the ResultClass using a setter method.
     * This is done either using the value passed in directly, or by first converting it to a specified argument type.
     */
    class ResultClassSetMethodSetter implements ResultClassMemberSetter
    {
        Method setterMethod;
        Class argType;
        public ResultClassSetMethodSetter(Method m, Class argType)
        {
            this.setterMethod = m;
            this.argType = argType;
        }

        public boolean set(Object obj, String fieldName, Object value)
        {
            if (argType == null)
            {
                // Setter takes in the precise value
                try
                {
                    setterMethod.invoke(obj, new Object[]{value});
                    if (NucleusLogger.QUERY.isDebugEnabled())
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("021220", resultClass.getName(), fieldName));
                    }
                    return true;
                }
                catch (Exception e)
                {
                }
            }
            else
            {
                // Setter takes in a value that needs converting first
                try
                {
                    Object convValue = TypeConversionHelper.convertTo(value, argType);
                    setterMethod.invoke(obj, new Object[]{convValue});
                    if (NucleusLogger.QUERY.isDebugEnabled())
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("021221", resultClass.getName(), fieldName));
                    }
                    return true;
                }
                catch (Exception e)
                {
                    //do nothing
                }
            }

            return false;
        }
    }


    /**
     * Class that sets a field of the ResultClass using a put() method.
     */
    class ResultClassPutMethodSetter implements ResultClassMemberSetter
    {
        Method putMethod;

        public ResultClassPutMethodSetter(Method m)
        {
            this.putMethod = m;
        }

        public boolean set(Object obj, String fieldName, Object value)
        {
            try
            {
                putMethod.invoke(obj, new Object[]{fieldName, value});
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug(Localiser.msg("021222", resultClass.getName(), fieldName));
                }
                return true;
            }
            catch (Exception e)
            {
                //do nothing
            }

            return false;
        }
    }
}