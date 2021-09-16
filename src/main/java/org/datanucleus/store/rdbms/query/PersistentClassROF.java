/**********************************************************************
Copyright (c) 2002 Kelly Grizzle (TJDO) and others. All rights reserved. 
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
2003 Erik Bengtson - removed subclasses operation
2003 Andy Jefferson - comments, and update to returned class name
2003 Erik Bengtson - added getObjectByAid
2004 Erik Bengtson - throws an JDOObjectNotFoundException
2004 Erik Bengtson - removed unused variable and import
2004 Erik Bengtson - added support for ignoreCache
2004 Andy Jefferson - coding standards
2005 Andy Jefferson - added support for using discriminator to distinguish objects 
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.query;

import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InterfaceMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.rdbms.fieldmanager.ResultSetGetter;
import org.datanucleus.util.ConcurrentReferenceHashMap;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;
import org.datanucleus.util.ConcurrentReferenceHashMap.ReferenceType;

/**
 * Result-object factory that takes a JDBC ResultSet, a results mapping, and creates a persistable object instance for each row in the ResultSet. 
 * We use information in the result set to determine the object type; this can be a discriminator column, or can be a special "NucleusType" column defined just for result processing.
 * @param <T> Type of the persistent object that this creates
 */
public final class PersistentClassROF<T> extends AbstractROF<T>
{
    /** Metadata for the (root) persistable candidate class. */
    protected final AbstractClassMetaData rootCmd;

    /** Persistent class that this factory will generate (may be the root class). */
    protected Class<T> persistentClass;

    /** Mapping of the results to members of this class (and sub-objects). */
    protected StatementClassMapping resultMapping = null;

    protected ResultSetGetter resultSetGetter = null;
    protected StatementClassMapping mappingDefinition;
    protected int[] mappedFieldNumbers;

    /** Resolved classes for metadata / discriminator keyed by class names. */
    private Map resolvedClasses = new ConcurrentReferenceHashMap<>(1, ReferenceType.STRONG, ReferenceType.SOFT);

    /**
     * Constructor.
     * @param ec ExecutionContext
     * @param rs ResultSet being processed
     * @param ignoreCache Whether to ignore the cache
     * @param fp FetchPlan
     * @param resultMapping Mapping information for the result set and how it maps to the class
     * @param acmd MetaData for the (root) candidate class
     * @param persistentClass Class that this factory will create instances of (or subclasses)
     */
    public PersistentClassROF(ExecutionContext ec, ResultSet rs, boolean ignoreCache, FetchPlan fp, StatementClassMapping resultMapping, AbstractClassMetaData acmd, Class<T> persistentClass)
    {
        super(ec, rs, ignoreCache, fp);

        this.resultMapping = resultMapping;
        this.rootCmd = acmd;
        this.persistentClass = persistentClass;
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
     * Method to convert the current ResultSet row into a persistable Object.
     * @return The persistable object.
     */
    public T getObject()
    {
        // Find the class of the returned object in this row of the ResultSet
        String className = null;
        boolean requiresInheritanceCheck = true;
        String discrimValue = null;

        // Used for reporting details of a failed class lookup by discriminator
        boolean hasDiscrimValue = false;
	    boolean foundClassByDiscrim = false;

        StatementMappingIndex discrimMapIdx = resultMapping.getMappingForMemberPosition(SurrogateColumnType.DISCRIMINATOR.getFieldNumber());
        if (discrimMapIdx != null)
        {
            // Discriminator mapping registered so use that
            try
            {
                discrimValue = rs.getString(discrimMapIdx.getColumnPositions()[0]);
                if (discrimValue == null)
                {
                    // Discriminator has no value so return null object
                    NucleusLogger.DATASTORE_RETRIEVE.debug("Value of discriminator is null so assuming object is null");
                    return null;
                }
                hasDiscrimValue = true;
                JavaTypeMapping discrimMapping = discrimMapIdx.getMapping();
                DiscriminatorMetaData dismd = (discrimMapping != null ? discrimMapping.getTable().getDiscriminatorMetaData() : null);
                className = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discrimValue, dismd);
                if (className != null)
                {
                	foundClassByDiscrim = true;
                }
                requiresInheritanceCheck = false;
            }
            catch (SQLException sqle)
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug("Exception obtaining value of discriminator : " + sqle.getMessage());
            }
        }
        else if (resultMapping.getNucleusTypeColumnName() != null)
        {
            // Extract the object type using the NucleusType column (if available)
            try
            {
                className = rs.getString(resultMapping.getNucleusTypeColumnName());
                if (className == null)
                {
                    // Discriminator has no value so return null object
                    NucleusLogger.DATASTORE_RETRIEVE.debug("Value of determiner column is null so assuming object is null");
                    return null;
                }

                className = className.trim();
                requiresInheritanceCheck = false;
            }
            catch (SQLException sqle)
            {
                // NucleusType column not found so ignore
            }
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        Class pcClassForObject = persistentClass;
        if (className != null)
        {
            Class cls = (Class) resolvedClasses.get(className);
            if (cls != null)
            {
                pcClassForObject = cls;
            }
            else
            {
                if (persistentClass.getName().equals(className))
                {
                    pcClassForObject = persistentClass;
                }
                else
                {
                    pcClassForObject = clr.classForName(className, persistentClass.getClassLoader());
                }
                resolvedClasses.put(className, pcClassForObject);
            }
        }
        if (requiresInheritanceCheck)
        {
            // Check if no instantiable subclasses
            String[] subclasses = ec.getMetaDataManager().getSubclassesForClass(pcClassForObject.getName(), false);
            if (subclasses == null || subclasses.length == 0)
            {
                requiresInheritanceCheck = false;
            }
        }

        String warnMsg = null;
        if (Modifier.isAbstract(pcClassForObject.getModifiers()))
        {
            // Persistent class is abstract so we can't create instances of that type!
            // This can happen if the user is using subclass-table and hasn't provided a discriminator in 
            // the table. Try going out one level and find a (single) concrete subclass 
            // TODO make this more robust and go out further
            String[] subclasses = ec.getMetaDataManager().getSubclassesForClass(pcClassForObject.getName(), false);
            if (subclasses != null)
            {
                Class concreteSubclass = null;
                int numConcreteSubclasses = 0;
                for (int i=0;i<subclasses.length;i++)
                {
                    Class subcls = clr.classForName(subclasses[i]);
                    if (!Modifier.isAbstract(subcls.getModifiers()))
                    {
                        numConcreteSubclasses++;
                        concreteSubclass = subcls;
                    }
                }
                if (numConcreteSubclasses == 1)
                {
                    // Only one possible subclass, so use that
                    NucleusLogger.DATASTORE_RETRIEVE.warn(Localiser.msg("052300", pcClassForObject.getName(), concreteSubclass.getName()));
                    pcClassForObject = concreteSubclass;
                }
                else if (numConcreteSubclasses == 0)
                {
                    throw new NucleusUserException(Localiser.msg("052301", pcClassForObject.getName()));
                }
                else
                {
                    // More than 1 possible so notify the user. Really should return the abstract
	                String warnMsgSuffix;
	                if (hasDiscrimValue && !foundClassByDiscrim)
	                	warnMsgSuffix = "No persistent class could be found that matches the discriminator value '" + discrimValue +
				                "'. Has the metadata for all persistent concrete subclasses been added?";
	                else
	                	warnMsgSuffix = "Really you need a discriminator to help identifying the type.";

	                warnMsg = "Found type=" + pcClassForObject + " but abstract and more than 1 concrete subclass (" +
                        StringUtils.objectArrayToString(subclasses) + ") when searching immediate subclasses. Choosing " + concreteSubclass + ". " + warnMsgSuffix;
                    pcClassForObject = concreteSubclass;
                    requiresInheritanceCheck = true;
                }
            }
        }

        // Find the statement mappings and field numbers to use for the result class
        // Caters for persistent-interfaces and the result class being an implementation
        AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(pcClassForObject, clr);
        if (cmd == null)
        {
            // No way of identifying the object type so assumed to be null
            // This can happen when selecting a class and also a related class via LEFT OUTER JOIN
            // and the LEFT OUTER JOIN results in no related object, hence null discriminator etc
            // TODO Improve this and check PK cols
            return null;
        }

        int[] fieldNumbers = resultMapping.getMemberNumbers();

        if (resultSetGetter == null)
        {
            // First time through, so generate mapping lookups and ResultSetGetter
            if (rootCmd instanceof InterfaceMetaData)
            {
                // Persistent-interface : create new mapping definition for a result type of the implementation
                mappingDefinition = new StatementClassMapping();
                mappingDefinition.setNucleusTypeColumnName(resultMapping.getNucleusTypeColumnName());
                mappedFieldNumbers = new int[fieldNumbers.length];
                for (int i = 0; i < fieldNumbers.length; i++)
                {
                    AbstractMemberMetaData mmd = rootCmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                    mappedFieldNumbers[i] = cmd.getAbsolutePositionOfMember(mmd.getName());
                    mappingDefinition.addMappingForMember(mappedFieldNumbers[i], resultMapping.getMappingForMemberPosition(fieldNumbers[i]));
                }
            }
            else
            {
                // Persistent class
                mappingDefinition = resultMapping;
                mappedFieldNumbers = fieldNumbers;
            }

            // Use this result mapping definition for our ResultSetGetter
            this.resultSetGetter = new ResultSetGetter(ec, rs, mappingDefinition, rootCmd);
        }

        // Extract any surrogate version
        Object surrogateVersion = null;
        VersionMetaData vermd = cmd.getVersionMetaDataForClass();
        if (vermd != null)
        {
            StatementMappingIndex versionMappingIdx = null;
            if (vermd.getFieldName() == null)
            {
                versionMappingIdx = resultMapping.getMappingForMemberPosition(SurrogateColumnType.VERSION.getFieldNumber());
            }
            else
            {
                versionMappingIdx = resultMapping.getMappingForMemberPosition(cmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber());
            }

            if (versionMappingIdx != null)
            {
                // Surrogate version column returned by query
                surrogateVersion = versionMappingIdx.getMapping().getObject(ec, rs, versionMappingIdx.getColumnPositions());
            }
        }

        if (persistentClass.isInterface() && !cmd.isImplementationOfPersistentDefinition())
        {
            // Querying by interface, and not a generated implementation so use the metadata for the interface
            cmd = ec.getMetaDataManager().getMetaDataForInterface(persistentClass, clr);
            if (cmd == null)
            {
                // Fallback to the class we had
                cmd = ec.getMetaDataManager().getMetaDataForClass(pcClassForObject, clr);
            }
        }

        // Extract the object from the ResultSet
        T obj = null;
        boolean needToSetVersion = false;
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            // Check if the PK field(s) are all null (implies null object, when using OUTER JOIN)
            int[] pkNumbers = cmd.getPKMemberPositions();
            boolean nullObject = true;
            for (int i=0;i<pkNumbers.length;i++)
            {
                StatementMappingIndex pkIdx = mappingDefinition.getMappingForMemberPosition(pkNumbers[i]);
                if (pkIdx == null)
                {
                    throw new NucleusException("You have just executed an SQL statement yet the information " +
                        "for the primary key column(s) is not available! Please generate a testcase and report this issue");
                }
                int[] colPositions = pkIdx.getColumnPositions();
                for (int j=0;j<colPositions.length;j++)
                {
                    try
                    {
                        if (rs.getObject(colPositions[j]) != null)
                        {
                            nullObject = false;
                            break;
                        }
                    }
                    catch (SQLException sqle)
                    {
                        NucleusLogger.DATASTORE_RETRIEVE.warn("Exception thrown while retrieving results ", sqle);
                    }
                    if (!nullObject)
                    {
                        break;
                    }
                }
            }

            if (!nullObject)
            {
                // Retrieve the object with this application-identity
                if (warnMsg != null)
                {
                    NucleusLogger.DATASTORE_RETRIEVE.warn(warnMsg);
                }

                Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, pcClassForObject, requiresInheritanceCheck, resultSetGetter);
                String idClassName = IdentityUtils.getTargetClassNameForIdentity(id);
                if (idClassName != null)
                {
                    // "identity" defines the class name
                    pcClassForObject = clr.classForName(idClassName);
                }

                obj = findObjectWithIdAndLoadFields(id, mappedFieldNumbers, pcClassForObject, cmd, surrogateVersion);
            }
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            // Generate the "id" for this object (of type pcClassForObject), and find the object for that
            StatementMappingIndex datastoreIdMapping = resultMapping.getMappingForMemberPosition(SurrogateColumnType.DATASTORE_ID.getFieldNumber());
            JavaTypeMapping mapping = datastoreIdMapping.getMapping();
            Object id = mapping.getObject(ec, rs, datastoreIdMapping.getColumnPositions());
            if (id != null)
            {
                String idClassName = IdentityUtils.getTargetClassNameForIdentity(id);
                if (!pcClassForObject.getName().equals(idClassName))
                {
                    // Get a DatastoreId for the right inheritance level
                    id = ec.getNucleusContext().getIdentityManager().getDatastoreId(pcClassForObject.getName(), IdentityUtils.getTargetKeyForDatastoreIdentity(id));
                }

                if (warnMsg != null)
                {
                    NucleusLogger.DATASTORE_RETRIEVE.warn(warnMsg);
                }
                if (mappedFieldNumbers == null)
                {
                    obj = (T) ec.findObject(id, false, requiresInheritanceCheck, null);
                    needToSetVersion = true;
                }
                else
                {
                    obj = findObjectWithIdAndLoadFields(id, mappedFieldNumbers, requiresInheritanceCheck ? null : pcClassForObject, cmd, surrogateVersion);
                }
            }
        }
        else if (cmd.getIdentityType() == IdentityType.NONDURABLE)
        {
            String classNameForId = className;
            if (className == null)
            {
                // No discriminator info from the query, so just fallback to default type
                classNameForId = cmd.getFullClassName();
            }
            Object id = ec.newObjectId(classNameForId, null);
            if (mappedFieldNumbers == null)
            {
                obj = (T) ec.findObject(id, false, requiresInheritanceCheck, null);
                needToSetVersion = true;
            }
            else
            {
                obj = findObjectWithIdAndLoadFields(id, fieldNumbers, pcClassForObject, cmd, surrogateVersion);
            }
        }

        if (obj != null && needToSetVersion)
        {
            // Set the version of the object where possible
            if (surrogateVersion != null)
            {
                ObjectProvider objOP = ec.findObjectProvider(obj);
                objOP.setVersion(surrogateVersion);
            }
            else
            {
                if (vermd != null && vermd.getFieldName() != null)
                {
                    // Version stored in a normal field
                    int versionFieldNumber = rootCmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber();
                    if (resultMapping.getMappingForMemberPosition(versionFieldNumber) != null)
                    {
                        ObjectProvider objOP = ec.findObjectProvider(obj);
                        Object verFieldValue = objOP.provideField(versionFieldNumber);
                        if (verFieldValue != null)
                        {
                            objOP.setVersion(verFieldValue);
                        }
                    }
                }
            }
        }

        return obj;
    }

    /**
     * Method to lookup an object for an id, and specify its FieldValues using the ResultSet. Works for all identity types.
     * @param id The identity (DatastoreId, Application id, or SCOID when nondurable)
     * @param fieldNumbers Field numbers that we have results for
     * @param pcClass The class of the required object if known
     * @param cmd Metadata for the type
     * @param surrogateVersion The version when the object has a surrogate version field
     * @return The persistable object for this id
     */
    private T findObjectWithIdAndLoadFields(final Object id, final int[] fieldNumbers, Class pcClass, final AbstractClassMetaData cmd, final Object surrogateVersion)
    {
        return (T) ec.findObject(id, new FieldValues()
        {
            // TODO If we ever support just loading a FK value but not instantiating this needs to store the value in the ObjectProvider.
            public void fetchFields(ObjectProvider op)
            {
                resultSetGetter.setObjectProvider(op);
                op.replaceFields(fieldNumbers, resultSetGetter, false);

                // Set version
                if (surrogateVersion != null)
                {
                    // Surrogate version field
                    op.setVersion(surrogateVersion);
                }
                else if (cmd.getVersionMetaData() != null && cmd.getVersionMetaData().getFieldName() != null)
                {
                    // Version stored in a normal field
                    VersionMetaData vermd = cmd.getVersionMetaData();
                    int versionFieldNumber = rootCmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber();
                    if (resultMapping.getMappingForMemberPosition(versionFieldNumber) != null)
                    {
                        Object verFieldValue = op.provideField(versionFieldNumber);
                        if (verFieldValue != null)
                        {
                            op.setVersion(verFieldValue);
                        }
                    }
                }
            }
            public void fetchNonLoadedFields(ObjectProvider op)
            {
                resultSetGetter.setObjectProvider(op);
                op.replaceNonLoadedFields(fieldNumbers, resultSetGetter);
            }

            public FetchPlan getFetchPlanForLoading()
            {
                return fp;
            }
        }, pcClass, ignoreCache, false);
    }
}