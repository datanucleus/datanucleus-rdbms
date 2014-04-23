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
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InterfaceMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.fieldmanager.ResultSetGetter;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.SoftValueMap;
import org.datanucleus.util.StringUtils;

/**
 * ResultObjectFactory that takes a JDBC ResultSet and create a persistable object instance for
 * each row in the ResultSet. We use information in the result set to determine the object type; this
 * can be a discriminator column, or can be a special "NucleusType" column defined just for result
 * processing.
 */
public final class PersistentClassROF implements ResultObjectFactory
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.rdbms.Localisation", RDBMSStoreManager.class.getClassLoader());

    protected final RDBMSStoreManager storeMgr;

    /** Metadata for the persistent class. */
    protected final AbstractClassMetaData acmd;

    /** Persistent class that this factory will generate (may be the base class). */
    private Class persistentClass;

    /** Mapping for the statement to members of this class (and sub-objects). */
    protected StatementClassMapping stmtMapping = null;

    /** Fetch Plan to use when loading fields (if any). */
    protected final FetchPlan fetchPlan;

    /** Whether to ignore the cache */
    private final boolean ignoreCache;

    /** Resolved classes for metadata / discriminator keyed by class names. */
    private Map resolvedClasses = new SoftValueMap();

    /**
     * Constructor.
     * @param storeMgr RDBMS StoreManager
     * @param acmd MetaData for the class (base class)
     * @param mappingDefinition Mapping information for the result set and how it maps to the class
     * @param ignoreCache Whether to ignore the cache
     * @param fetchPlan the Fetch Plan
     * @param persistentClass Class that this factory will create instances of (or subclasses)
     */
    public PersistentClassROF(RDBMSStoreManager storeMgr, AbstractClassMetaData acmd, StatementClassMapping mappingDefinition,
                           boolean ignoreCache, FetchPlan fetchPlan, Class persistentClass)
    {
        if (mappingDefinition == null)
        {
            throw new NucleusException("Attempt to create PersistentIDROF with null mappingDefinition");
        }

        this.storeMgr = storeMgr;
        this.stmtMapping = mappingDefinition;
        this.acmd = acmd;
        this.ignoreCache = ignoreCache;
        this.fetchPlan = fetchPlan;
        this.persistentClass = persistentClass;
    }

    /**
     * Method to update the persistent class that the result object factory requires.
     * This is used where we have an Extent(BaseClass) and we pass it to a JDOQL query but the query
     * has been specified with a persistent class that is a subclass. So we use this method to restrict
     * the results further.
     * @param cls The Class the result factory requires.
     */
    public void setPersistentClass(Class cls)
    {
        this.persistentClass = cls;
    }

    /**
     * Method to convert the current ResultSet row into an Object.
     * @param ec execution context
     * @param rs The ResultSet from the Query.
     * @return The (persisted) object.
     */
    public Object getObject(final ExecutionContext ec, final ResultSet rs)
    {
        // Find the class of the returned object in this row of the ResultSet
        String className = null;
        boolean requiresInheritanceCheck = true;
        StatementMappingIndex discrimMapIdx = stmtMapping.getMappingForMemberPosition(StatementClassMapping.MEMBER_DISCRIMINATOR);
        if (discrimMapIdx != null)
        {
            // Discriminator mapping registered so use that
            try
            {
                String discrimValue = rs.getString(discrimMapIdx.getColumnPositions()[0]);
                if (discrimValue == null)
                {
                    // Discriminator has no value so return null object
                    NucleusLogger.DATASTORE_RETRIEVE.debug("Value of discriminator is null so assuming object is null");
                    return null;
                }
                JavaTypeMapping discrimMapping = discrimMapIdx.getMapping();
                DiscriminatorMetaData dismd =
                    (discrimMapping != null ? discrimMapping.getTable().getDiscriminatorMetaData() : null);
                className = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discrimValue, dismd);
                requiresInheritanceCheck = false;
            }
            catch (SQLException sqle)
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug("Exception obtaining value of discriminator : " + sqle.getMessage());
            }
        }
        else if (stmtMapping.getNucleusTypeColumnName() != null)
        {
            // Extract the object type using the NucleusType column (if available)
            try
            {
                className = rs.getString(stmtMapping.getNucleusTypeColumnName()).trim();
                if (className == null)
                {
                    // Discriminator has no value so return null object
                    NucleusLogger.DATASTORE_RETRIEVE.debug("Value of determiner column is null so assuming object is null");
                    return null;
                }
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
                    pcClassForObject = concreteSubclass;
                    NucleusLogger.DATASTORE_RETRIEVE.warn(LOCALISER.msg("052300", 
                        pcClassForObject.getName(), concreteSubclass.getName()));
                }
                else if (numConcreteSubclasses == 0)
                {
                    throw new NucleusUserException(LOCALISER.msg("052301", pcClassForObject.getName()));
                }
                else
                {
                    // More than 1 possible so notify the user. Really should return the abstract
                    warnMsg = "Found type=" + pcClassForObject +
                        " but abstract and more than 1 concrete subclass (" +
                        StringUtils.objectArrayToString(subclasses) + "). Really you need a discriminator " +
                        " to help identifying the type. Choosing " + concreteSubclass;
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

        int[] fieldNumbers = stmtMapping.getMemberNumbers();
        StatementClassMapping mappingDefinition;
        int[] mappedFieldNumbers;
        if (acmd instanceof InterfaceMetaData)
        {
            // Persistent-interface : create new mapping definition for a result type of the implementation
            mappingDefinition = new StatementClassMapping();
            mappingDefinition.setNucleusTypeColumnName(stmtMapping.getNucleusTypeColumnName());
            mappedFieldNumbers = new int[fieldNumbers.length];
            for (int i = 0; i < fieldNumbers.length; i++)
            {
                AbstractMemberMetaData mmd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                mappedFieldNumbers[i] = cmd.getAbsolutePositionOfMember(mmd.getName());
                mappingDefinition.addMappingForMember(mappedFieldNumbers[i], 
                    stmtMapping.getMappingForMemberPosition(fieldNumbers[i]));
            }
        }
        else
        {
            // Persistent class
            mappingDefinition = stmtMapping;
            mappedFieldNumbers = fieldNumbers;
        }

        // Extract any surrogate version
        VersionMetaData vermd = cmd.getVersionMetaDataForClass();
        Object surrogateVersion = null;
        StatementMappingIndex versionMapping = null;
        if (vermd != null)
        {
            if (vermd.getFieldName() == null)
            {
                versionMapping = stmtMapping.getMappingForMemberPosition(StatementClassMapping.MEMBER_VERSION);
            }
            else
            {
                AbstractMemberMetaData vermmd = cmd.getMetaDataForMember(vermd.getFieldName());
                versionMapping = stmtMapping.getMappingForMemberPosition(vermmd.getAbsoluteFieldNumber());
            }
        }
        if (versionMapping != null)
        {
            // Surrogate version column returned by query
            JavaTypeMapping mapping = versionMapping.getMapping();
            surrogateVersion = mapping.getObject(ec, rs, versionMapping.getColumnPositions());
        }

        // Extract the object from the ResultSet
        Object obj = null;
        boolean needToSetVersion = false;
        if (persistentClass.isInterface() && !cmd.isImplementationOfPersistentDefinition())
        {
            // Querying by interface, and not a generated implementation so use the metadata for the interface
            cmd = ec.getMetaDataManager().getMetaDataForInterface(persistentClass, clr);
            if (cmd == null)
            {
                // Fallback to the value we had
                cmd = ec.getMetaDataManager().getMetaDataForClass(pcClassForObject, clr);
            }
        }

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
                        "for the primary key column(s) is not available! " + 
                        "Please generate a testcase and report this issue");
                }
                int[] colPositions = pkIdx.getColumnPositions();
                for (int j=0;j<colPositions.length;j++)
                {
                    try
                    {
                        Object pkObj = rs.getObject(colPositions[j]);
                        if (pkObj != null)
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
                obj = getObjectForApplicationId(ec, rs, mappingDefinition, mappedFieldNumbers,
                    pcClassForObject, cmd, requiresInheritanceCheck, surrogateVersion);
            }
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            // Generate the "oid" for this object (of type pcClassForObject), and find the object for that
            StatementMappingIndex datastoreIdMapping =
                stmtMapping.getMappingForMemberPosition(StatementClassMapping.MEMBER_DATASTORE_ID);
            JavaTypeMapping mapping = datastoreIdMapping.getMapping();
            Object id = mapping.getObject(ec, rs, datastoreIdMapping.getColumnPositions());
            if (id != null)
            {
                if (!pcClassForObject.getName().equals(IdentityUtils.getTargetClassNameForIdentitySimple(id)))
                {
                    // Get an OID for the right inheritance level
                    id = OIDFactory.getInstance(ec.getNucleusContext(), pcClassForObject.getName(), IdentityUtils.getTargetKeyForDatastoreIdentity(id));
                }

                if (warnMsg != null)
                {
                    NucleusLogger.DATASTORE_RETRIEVE.warn(warnMsg);
                }
                if (mappedFieldNumbers == null)
                {
                    obj = ec.findObject(id, false, requiresInheritanceCheck, null);
                    needToSetVersion = true;
                }
                else
                {
                    obj = getObjectForDatastoreId(ec, rs, mappingDefinition, mappedFieldNumbers, id, requiresInheritanceCheck ? null : pcClassForObject, cmd, surrogateVersion);
                }
            }
        }
        else if (cmd.getIdentityType() == IdentityType.NONDURABLE)
        {
            Object id = ec.newObjectId(className, null);
            if (mappedFieldNumbers == null)
            {
                obj = ec.findObject(id, false, requiresInheritanceCheck, null);
                needToSetVersion = true;
            }
            else
            {
                obj = getObjectForDatastoreId(ec, rs, mappingDefinition, mappedFieldNumbers, 
                    id, pcClassForObject, cmd, surrogateVersion);
            }
        }

        if (obj != null && needToSetVersion)
        {
            // Set the version of the object where possible
            if (surrogateVersion != null)
            {
                ObjectProvider objSM = ec.findObjectProvider(obj);
                objSM.setVersion(surrogateVersion);
            }
            else
            {
                if (vermd != null && vermd.getFieldName() != null)
                {
                    // Version stored in a normal field
                    int versionFieldNumber = acmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber();
                    if (stmtMapping.getMappingForMemberPosition(versionFieldNumber) != null)
                    {
                        ObjectProvider objSM = ec.findObjectProvider(obj);
                        Object verFieldValue = objSM.provideField(versionFieldNumber);
                        if (verFieldValue != null)
                        {
                            objSM.setVersion(verFieldValue);
                        }
                    }
                }
            }
        }

        return obj;
    }

    /**
     * Returns a PC instance from a ResultSet row with an application identity.
     * @param ec execution context
     * @param resultSet The ResultSet
     * @param mappingDefinition The mapping info for the result class
     * @param fieldNumbers Numbers of the fields (of the class) found in the ResultSet
     * @param pcClass persistable class
     * @param cmd Metadata for the class
     * @param requiresInheritanceCheck Whether we need to check the inheritance level of the returned object
     * @param surrogateVersion Surrogate version if available
     * @return The object with this application identity
     */
    private Object getObjectForApplicationId(final ExecutionContext ec, final ResultSet resultSet, final StatementClassMapping mappingDefinition, 
            final int[] fieldNumbers, Class pcClass, final AbstractClassMetaData cmd, boolean requiresInheritanceCheck, final Object surrogateVersion)
    {
        Object id = getIdentityForResultSetRow(storeMgr, resultSet, mappingDefinition, ec, cmd, pcClass, requiresInheritanceCheck);
        if (IdentityUtils.isSingleFieldIdentity(id))
        {
            // Any single-field identity will have the precise target class determined above, so use it
            pcClass = ec.getClassLoaderResolver().classForName(IdentityUtils.getTargetClassNameForIdentitySimple(id));
        }

        return ec.findObject(id, new FieldValues()
        {
            public void fetchFields(ObjectProvider sm)
            {
                FieldManager fm = storeMgr.getFieldManagerForResultProcessing(sm, resultSet, mappingDefinition);
                sm.replaceFields(fieldNumbers, fm, false);

                // Set version
                if (surrogateVersion != null)
                {
                    // Surrogate version field
                    sm.setVersion(surrogateVersion);
                }
                else if (cmd.getVersionMetaData() != null && cmd.getVersionMetaData().getFieldName() != null)
                {
                    VersionMetaData vermd = cmd.getVersionMetaData();
                    // Version stored in a normal field
                    int versionFieldNumber = acmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber();
                    if (stmtMapping.getMappingForMemberPosition(versionFieldNumber) != null)
                    {
                        Object verFieldValue = sm.provideField(versionFieldNumber);
                        if (verFieldValue != null)
                        {
                            sm.setVersion(verFieldValue);
                        }
                    }
                }
            }
            public void fetchNonLoadedFields(ObjectProvider sm)
            {
                FieldManager fm = storeMgr.getFieldManagerForResultProcessing(sm, resultSet, mappingDefinition);
                sm.replaceNonLoadedFields(fieldNumbers, fm);
            }
            public FetchPlan getFetchPlanForLoading()
            {
                return fetchPlan;
            }
        }, pcClass, ignoreCache, false);
    }

    /**
     * Method to return the object identity for a row of the result set.
     * @param storeMgr RDBMS StoreManager
     * @param resultSet Result set
     * @param mappingDefinition Mapping definition for the candidate class
     * @param ec Execution Context
     * @param cmd Metadata for the class
     * @param pcClass The class required
     * @param inheritanceCheck Whether need an inheritance check (may be for a subclass)
     * @return The identity (if found) or null (if either not sure of inheritance, or not known).
     */
    public static Object getIdentityForResultSetRow(RDBMSStoreManager storeMgr, final ResultSet resultSet, 
            final StatementClassMapping mappingDefinition, ExecutionContext ec, AbstractClassMetaData cmd, 
            Class pcClass, boolean inheritanceCheck)
    {
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            return getDatastoreIdentityForResultSetRow(ec, cmd, pcClass, inheritanceCheck, resultSet, mappingDefinition);
        }
        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            FieldManager resultsFM = new ResultSetGetter(storeMgr, ec, resultSet, mappingDefinition, cmd);
            return IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, pcClass, inheritanceCheck, resultsFM);
        }
        return null;
    }

    /**
     * Method to return the object datastore identity for a row of the result set.
     * If the class isn't using datastore identity then returns null
     * @param ec Execution Context
     * @param cmd Metadata for the class
     * @param pcClass The class required
     * @param inheritanceCheck Whether need an inheritance check (may be for a subclass)
     * @param resultSet Result set
     * @param mappingDefinition Mapping definition for the candidate class
     * @return The identity (if found) or null (if either not sure of inheritance, or not known).
     */
    public static Object getDatastoreIdentityForResultSetRow(ExecutionContext ec, 
            AbstractClassMetaData cmd, Class pcClass, boolean inheritanceCheck, 
            final ResultSet resultSet, final StatementClassMapping mappingDefinition)
    {
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            if (pcClass == null)
            {
                pcClass = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
            }
            StatementMappingIndex datastoreIdMapping =
                mappingDefinition.getMappingForMemberPosition(StatementClassMapping.MEMBER_DATASTORE_ID);
            JavaTypeMapping mapping = datastoreIdMapping.getMapping();
            Object id = mapping.getObject(ec, resultSet, datastoreIdMapping.getColumnPositions());
            if (id != null)
            {
                if (!pcClass.getName().equals(IdentityUtils.getTargetClassNameForIdentitySimple(id)))
                {
                    // Get an OID for the right inheritance level
                    id = OIDFactory.getInstance(ec.getNucleusContext(), pcClass.getName(), IdentityUtils.getTargetKeyForDatastoreIdentity(id));
                }
            }
            if (inheritanceCheck)
            {
                // Check if this identity exists in the cache(s)
                if (ec.hasIdentityInCache(id))
                {
                    return id;
                }

                // Check if this id for any known subclasses is in the cache to save searching
                String[] subclasses = ec.getMetaDataManager().getSubclassesForClass(pcClass.getName(), true);
                if (subclasses != null)
                {
                    for (int i=0;i<subclasses.length;i++)
                    {
                        id = OIDFactory.getInstance(ec.getNucleusContext(), subclasses[i], IdentityUtils.getTargetKeyForDatastoreIdentity(id));
                        if (ec.hasIdentityInCache(id))
                        {
                            return id;
                        }
                    }
                }

                // Check the inheritance with the store manager (may involve a trip to the datastore)
                String className = ec.getStoreManager().getClassNameForObjectID(id, ec.getClassLoaderResolver(), ec);
                return OIDFactory.getInstance(ec.getNucleusContext(), className, IdentityUtils.getTargetKeyForDatastoreIdentity(id));
            }
            return id;
        }
        return null;
    }

    /**
     * Returns a PC instance from a ResultSet row with a datastore identity.
     * @param ec execution context
     * @param resultSet The ResultSet
     * @param mappingDefinition The mapping info for the result class
     * @param fieldNumbers Numbers of the fields (of the class) found in the ResultSet
     * @param cmd MetaData for the class
     * @param oid The object id
     * @param pcClass The persistable class (where we know the instance type required, null if not)
     * @param cmd Metadata for the class
     * @param surrogateVersion Surrogate version (if applicable)
     * @return The Object
     */
    private Object getObjectForDatastoreId(final ExecutionContext ec, final ResultSet resultSet, 
            final StatementClassMapping mappingDefinition, final int[] fieldNumbers,
            Object oid, Class pcClass, final AbstractClassMetaData cmd, final Object surrogateVersion)
    {
        if (oid == null)
        {
            return null;
        }

        return ec.findObject(oid, new FieldValues()
        {
            public void fetchFields(ObjectProvider sm)
            {
                FieldManager fm = storeMgr.getFieldManagerForResultProcessing(sm, resultSet,
                    mappingDefinition);
                sm.replaceFields(fieldNumbers, fm, false);

                // Set version
                if (surrogateVersion != null)
                {
                    // Surrogate version field
                    sm.setVersion(surrogateVersion);
                }
                else if (cmd.getVersionMetaData() != null && cmd.getVersionMetaData().getFieldName() != null)
                {
                    VersionMetaData vermd = cmd.getVersionMetaData();
                    // Version stored in a normal field
                    int versionFieldNumber = acmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber();
                    if (stmtMapping.getMappingForMemberPosition(versionFieldNumber) != null)
                    {
                        Object verFieldValue = sm.provideField(versionFieldNumber);
                        if (verFieldValue != null)
                        {
                            sm.setVersion(verFieldValue);
                        }
                    }
                }
            }
            public void fetchNonLoadedFields(ObjectProvider sm)
            {
                FieldManager fm = storeMgr.getFieldManagerForResultProcessing(sm, resultSet,
                    mappingDefinition);
                sm.replaceNonLoadedFields(fieldNumbers, fm);
            }
            public FetchPlan getFetchPlanForLoading()
            {
                return fetchPlan;
            }
        }, pcClass, ignoreCache, false);
    }
}