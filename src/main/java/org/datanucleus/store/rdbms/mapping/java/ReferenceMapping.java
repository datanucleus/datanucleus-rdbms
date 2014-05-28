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
2007 Andy Jefferson - implement RelationMappingCallbacks
    ...
***********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.ElementMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.KeyMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.ValueMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.exceptions.NoTableManagedException;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.ColumnCreator;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Mapping for a "reference" type.
 * This can be used for things like interfaces, or Object which are simply a reference to some specific (persistable) class. 
 * This can be persisted in several ways (see "mappingStrategy") :-
 * <ul>
 * <li>List of possible "implementations" of the reference type where column(s) are created for each 
 * possible implementation of the reference as a FK to the implementation table. 
 * This has the advantage that it retains referential integrity since direct FKs are used.</li>
 * <li>What Kodo/Xcalia used was a single column storing the identity toString() form.</li>
 * </ul>
 */
public abstract class ReferenceMapping extends MultiPersistableMapping implements MappingCallbacks
{
    /** Each implementation has its own column(s) as a FK to the related table. */
    public static final int PER_IMPLEMENTATION_MAPPING = 0;

    /** Single column containing the "identity" of an object. */
    public static final int ID_MAPPING = 1;

    /** Single column containing the Xcalia form of the "identity" of an object. */
    public static final int XCALIA_MAPPING = 2;

    /** Strategy for how the reference(s) are mapped. */
    protected int mappingStrategy = PER_IMPLEMENTATION_MAPPING;

    /**
     * Initialize this JavaTypeMapping for the specified field/property.
     * @param mmd AbstractMemberMetaData for the field to be mapped (if any)
     * @param table The datastore container storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     */
    public void initialize(AbstractMemberMetaData mmd, Table table, 
            ClassLoaderResolver clr)
    {
        if (mmd.hasExtension("mapping-strategy"))
        {
            String strategy = mmd.getValueForExtension("mapping-strategy");
            if (strategy.equalsIgnoreCase("identity"))
            {
                mappingStrategy = ID_MAPPING;
            }
            else if (strategy.equalsIgnoreCase("xcalia"))
            {
                mappingStrategy = XCALIA_MAPPING;
            }
        }

        numberOfDatastoreMappings = 0; // Reset indicator for datastore fields
        super.initialize(mmd, table, clr);
        prepareDatastoreMapping(clr);
    }

    /**
     * Accessor for the mapping strategy. There are various supported strategies for reference
     * fields with the default being one mapping per implementation, but also allowing a single
     * (String) mapping for all implementations.
     * @return The mapping strategy
     */
    public int getMappingStrategy()
    {
        return mappingStrategy;
    }

    /**
     * Convenience method to create the necessary columns to represent this reference in the datastore.
     * With "per-implementation" mapping strategy will create columns for each of the possible implementations.
     * With "identity"/"xcalia" will create a single column to store a reference to the implementation value.
     * @param clr The ClassLoaderResolver
     */
    protected void prepareDatastoreMapping(ClassLoaderResolver clr)
    {
        // TODO NUCRDBMS-19 will need this block enabling to generate the correct schema. All tests pass with it enabled but
        // left commented out until we can do getObject/setObject 
        /*RelationType relationType = mmd.getRelationType(clr);
        String fieldTypeName = getReferenceFieldType(roleForMember);
        boolean isPersistentInterfaceField = storeMgr.getNucleusContext().getMetaDataManager().isPersistentInterface(fieldTypeName);
        if (!isPersistentInterfaceField && relationType == RelationType.MANY_TO_ONE_BI) // TODO Should do the same with N-1 uni
        {
            AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
            if (mmd.getJoinMetaData() != null || relatedMmds[0].getJoinMetaData() != null)
            {
                // N-1 bi with join table should have no column and join table has FK to this table
                referenceMapping = storeMgr.getDatastoreClass(relatedMmds[0].getAbstractClassMetaData().getFullClassName(), clr).getIdMapping();
                return;
            }
        }*/

        if (mappingStrategy == PER_IMPLEMENTATION_MAPPING)
        {
            // Mapping per reference implementation, so create columns for each possible implementation
            if (roleForMember == FieldRole.ROLE_ARRAY_ELEMENT)
            {
                // Creation of columns in join table for array of references
                ColumnMetaData[] colmds = null;
                ElementMetaData elemmd = mmd.getElementMetaData();
                if (elemmd != null && elemmd.getColumnMetaData() != null && elemmd.getColumnMetaData().length > 0)
                {
                    // Column mappings defined at this side (1-N, M-N)
                    colmds = elemmd.getColumnMetaData();
                }
                createPerImplementationColumnsForReferenceField(false, false, false, false, 
                    roleForMember, colmds, clr);
            }
            else if (roleForMember == FieldRole.ROLE_COLLECTION_ELEMENT)
            {
                // Creation of columns in join table for collection of references
                ColumnMetaData[] colmds = null;
                AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                ElementMetaData elemmd = mmd.getElementMetaData();
                if (elemmd != null && elemmd.getColumnMetaData() != null && elemmd.getColumnMetaData().length > 0)
                {
                    // Column mappings defined at this side (1-N, M-N)
                    colmds = elemmd.getColumnMetaData();
                }
                else if (relatedMmds != null && relatedMmds[0].getJoinMetaData() != null && 
                        relatedMmds[0].getJoinMetaData().getColumnMetaData() != null &&
                        relatedMmds[0].getJoinMetaData().getColumnMetaData().length > 0)
                {
                    // Column mappings defined at other side (M-N) on <join>
                    colmds = relatedMmds[0].getJoinMetaData().getColumnMetaData();
                }
                createPerImplementationColumnsForReferenceField(false, false, false, false, 
                    roleForMember, colmds, clr);
            }
            else if (roleForMember == FieldRole.ROLE_MAP_KEY)
            {
                // Creation of columns in join table for map of references as keys
                ColumnMetaData[] colmds = null;
                KeyMetaData keymd = mmd.getKeyMetaData();
                if (keymd != null && keymd.getColumnMetaData() != null && keymd.getColumnMetaData().length > 0)
                {
                    // Column mappings defined at this side (1-N, M-N)
                    colmds = keymd.getColumnMetaData();
                }
                createPerImplementationColumnsForReferenceField(false, false, false, false, 
                    roleForMember, colmds, clr);
            }
            else if (roleForMember == FieldRole.ROLE_MAP_VALUE)
            {
                // Creation of columns in join table for map of references as values
                ColumnMetaData[] colmds = null;
                ValueMetaData valuemd = mmd.getValueMetaData();
                if (valuemd != null && valuemd.getColumnMetaData() != null && valuemd.getColumnMetaData().length > 0)
                {
                    // Column mappings defined at this side (1-N, M-N)
                    colmds = valuemd.getColumnMetaData();
                }
                createPerImplementationColumnsForReferenceField(false, false, false, false, 
                    roleForMember, colmds, clr);
            }
            else
            {
                if (mmd.getMappedBy() == null)
                {
                    // Unidirectional 1-1
                    createPerImplementationColumnsForReferenceField(false, true, false, 
                        mmd.isEmbedded() || mmd.getElementMetaData() != null,
                        roleForMember, mmd.getColumnMetaData(), clr);
                }
                else
                {
                    // Bidirectional 1-1/N-1
                    AbstractClassMetaData refCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForInterface(mmd.getType(), clr);
                    if (refCmd != null && refCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
                    {
                        // TODO Is this block actually reachable ? Would we specify "inheritance" under "interface" elements?
                        // Find the actual tables storing the other end (can be multiple subclasses)
                        AbstractClassMetaData[] cmds = storeMgr.getClassesManagingTableForClass(refCmd, clr);
                        if (cmds != null && cmds.length > 0)
                        {
                            if (cmds.length > 1)
                            {
                                NucleusLogger.PERSISTENCE.warn("Field " + mmd.getFullFieldName() + 
                                    " represents either a 1-1 relation, or a N-1 relation where the other end uses" +
                                    " \"subclass-table\" inheritance strategy and more than 1 subclasses with a table. " +
                                    "This is not fully supported currently");
                            }
                        }
                        else
                        {
                            // No subclasses of the class using "subclasses-table" so no mapping!
                            // TODO Throw an exception ?
                            return;
                        }
                        // TODO We need a mapping for each of the possible subclass tables
                        /*JavaTypeMapping referenceMapping = */storeMgr.getDatastoreClass(cmds[0].getFullClassName(), clr).getIdMapping();
                    }
                    else
                    {
                        String[] implTypes = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, 
                            FieldRole.ROLE_FIELD, clr, storeMgr.getMetaDataManager());
                        for (int j=0; j<implTypes.length; j++)
                        {
                            JavaTypeMapping refMapping = storeMgr.getDatastoreClass(implTypes[j], clr).getIdMapping();
                            JavaTypeMapping mapping = storeMgr.getMappingManager().getMapping(clr.classForName(implTypes[j]));
                            mapping.setReferenceMapping(refMapping);
                            this.addJavaTypeMapping(mapping);
                        }
                    }
                }
            }
        }
        else if (mappingStrategy == ID_MAPPING || mappingStrategy == XCALIA_MAPPING)
        {
            // Single (String) column storing the identity of the related object
            MappingManager mapMgr = storeMgr.getMappingManager();
            JavaTypeMapping mapping = mapMgr.getMapping(String.class);
            mapping.setMemberMetaData(mmd);
            mapping.setTable(table);
            mapping.setRoleForMember(roleForMember);
            Column col = mapMgr.createColumn(mapping, String.class.getName(), 0);
            mapMgr.createDatastoreMapping(mapping, mmd, 0, col);
            this.addJavaTypeMapping(mapping);
        }
    }

    /**
     * Convenience method to extract the type of the reference field.
     * @param fieldRole Role of this field
     * @return The field type name
     */
    private String getReferenceFieldType(FieldRole fieldRole)
    {
        String fieldTypeName = mmd.getTypeName();
        if (mmd.getFieldTypes() != null && mmd.getFieldTypes().length == 1)
        {
            // "field-type" specified
            fieldTypeName = mmd.getFieldTypes()[0];
        }
        if (mmd.hasCollection())
        {
            fieldTypeName = mmd.getCollection().getElementType();
        }
        else if (mmd.hasArray())
        {
            fieldTypeName = mmd.getArray().getElementType();
        }
        else if (mmd.hasMap())
        {
            if (fieldRole == FieldRole.ROLE_MAP_KEY)
            {
                fieldTypeName = mmd.getMap().getKeyType();
            }
            else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
            {
                fieldTypeName = mmd.getMap().getValueType();
            }
        }
        return fieldTypeName;
    }

    /**
     * Create columns for reference (Interface/Object) fields on a per-implementation basis.
     * This call ColumnCreator.createColumnsForField for each implementation class of the reference.
     */
    void createPerImplementationColumnsForReferenceField(boolean pk, boolean nullable, boolean serialised, 
            boolean embedded, FieldRole fieldRole, ColumnMetaData[] columnMetaData, ClassLoaderResolver clr)
    {
        if (this instanceof InterfaceMapping && mmd != null && mmd.hasExtension("implementation-classes"))
        {
            // Store the implementation-classes with the mapping (persistent interfaces?)
            ((InterfaceMapping) this).setImplementationClasses(mmd.getValueForExtension("implementation-classes"));
        }

        // Find the available implementations that we are creating columns for
        String[] implTypes = null;
        try
        {
            implTypes = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, fieldRole, clr, storeMgr.getMetaDataManager());
        }
        catch (NucleusUserException nue)
        {
            // No implementation classes found, so log warning and return
            if (storeMgr.getBooleanProperty(PropertyNames.PROPERTY_STORE_ALLOW_REFS_WITHOUT_IMPLS, false))
            {
                NucleusLogger.DATASTORE_SCHEMA.warn("Possible problem encountered while adding columns for field " +
                    mmd.getFullFieldName() + " : " + nue.getMessage());
                return;
            }
            else
            {
                throw nue;
            }
        }

        // Set the PK and nullability of column(s) for the implementations (based on the number of impls etc)
        if (implTypes.length > 1)
        {
            pk = false; // Cannot be part of PK if more than 1 implementation
        }
        if (implTypes.length > 1 && !pk)
        {
            nullable = true; // Must be nullable if more than 1 impl (since only 1 impl can have value at a time)
        }

        // Create list of classes that require columns.
        // We only add columns for the implementation that is the root of a particular inheritance tree
        // e.g if we have A implements I1, and B extends A then they both are valid implementations
        // but we only want to create column(s) for A.
        Collection implClasses = new ArrayList();
        for (int i=0;i<implTypes.length;i++)
        {
            Class type = clr.classForName(implTypes[i]);
            if (type == null)
            {
                throw new NucleusUserException(Localiser.msg("020189", mmd.getTypeName(), implTypes[i]));
            }
            else if (type.isInterface())
            {
                throw new NucleusUserException(Localiser.msg("020190", mmd.getFullFieldName(), 
                    mmd.getTypeName(), implTypes[i]));
            }

            Iterator iter = implClasses.iterator();
            boolean toBeAdded = true;
            Class clsToSwap = null;
            while (iter.hasNext())
            {
                Class cls = (Class)iter.next();
                if (cls == type)
                {
                    // Implementation already present
                    toBeAdded = false;
                    break;
                }
                else
                {
                    if (type.isAssignableFrom(cls))
                    {
                        // "type" is superclass of "cls" so swap subclass for this class
                        clsToSwap = cls;
                        toBeAdded = false;
                        break;
                    }
                    else if (cls.isAssignableFrom(type))
                    {
                        toBeAdded = false;
                        break;
                    }
                }
            }
            if (toBeAdded)
            {
                implClasses.add(type);
            }
            else if (clsToSwap != null)
            {
                implClasses.remove(clsToSwap);
                implClasses.add(type);
            }
        }

        // Add columns for each of these implementations
        int colPos = 0;
        Iterator implClsIter = implClasses.iterator();
        while (implClsIter.hasNext())
        {
            Class implClass = (Class)implClsIter.next();

            boolean present = false;
            int numJavaTypeMappings = getJavaTypeMapping().length;
            for (int i=0;i<numJavaTypeMappings;i++)
            {
                JavaTypeMapping implMapping = getJavaTypeMapping()[i];
                if (implClass.getName().equals(implMapping.getType()))
                {
                    present = true;
                }
            }
            if (present)
            {
                // Implementation already present in mapping (e.g reinitialising) so skip this
                continue;
            }

            String fieldTypeName = getReferenceFieldType(fieldRole);
            boolean isPersistentInterfaceField = storeMgr.getNucleusContext().getMetaDataManager().isPersistentInterface(fieldTypeName);

            boolean columnsNeeded = true;
            if (isPersistentInterfaceField && 
                !storeMgr.getNucleusContext().getMetaDataManager().isPersistentInterfaceImplementation(fieldTypeName, implClass.getName()))
            {
                // We have a "persistent-interface" field yet this is not a generated implementation so ignore it
                // It is arguable if we should allow the real implementations of this interface here, but the JDO2 TCK doesn't
                // make that assumption so we don't either
                columnsNeeded = false;
            }

            if (columnsNeeded)
            {
                // Get the mapping for this implementation
                JavaTypeMapping m;
                if (storeMgr.getMappedTypeManager().isSupportedMappedType(implClass.getName()))
                {
                    m = storeMgr.getMappingManager().getMapping(implClass, serialised, embedded, 
                        mmd.getFullFieldName());
                }
                else
                {
                    try
                    {
                        DatastoreClass dc = storeMgr.getDatastoreClass(implClass.getName(), clr);
                        m = dc.getIdMapping();
                    }
                    catch (NoTableManagedException ex)
                    {
                        // TODO Localise this message
                        throw new NucleusUserException("Cannot define columns for " + mmd.getFullFieldName() + 
                            " due to " + ex.getMessage(), ex);
                    }
                }

                ColumnMetaData[] columnMetaDataForType = null;
                if (columnMetaData != null && columnMetaData.length > 0)
                {
                    if (columnMetaData.length < colPos+m.getNumberOfDatastoreMappings())
                    {
                        throw new NucleusUserException(Localiser.msg("020186", 
                            mmd.getFullFieldName(), "" + columnMetaData.length, 
                            "" + (colPos + m.getNumberOfDatastoreMappings())));
                    }
                    columnMetaDataForType = new ColumnMetaData[m.getNumberOfDatastoreMappings()];
                    System.arraycopy(columnMetaData, colPos, columnMetaDataForType, 0, columnMetaDataForType.length);
                    colPos += columnMetaDataForType.length;
                }

                // Create the FK column(s) for this implementation
                ColumnCreator.createColumnsForField(implClass, this, table, storeMgr, mmd, pk, 
                    nullable, serialised, embedded, fieldRole, columnMetaDataForType, clr, true);

                if (NucleusLogger.DATASTORE.isInfoEnabled())
                {
                    NucleusLogger.DATASTORE.info(Localiser.msg("020188", implClass, mmd.getName()));
                }
            }
        }
    }

    /**
     * Accessor for the java type represented by a particular datastore mapping.
     * This implementation relays to the superclass implementation except in the case of 
     * "identity" mapping strategy, in which case it returns "java.lang.String".
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForDatastoreMapping(int index)
    {
        if ((mappingStrategy == ID_MAPPING || mappingStrategy == XCALIA_MAPPING) && index == 0)
        {
            return String.class.getName();
        }
        return super.getJavaTypeForDatastoreMapping(index);
    }

    /**
     * Convenience accessor for the number of the java type mapping where the passed value would be
     * stored. If no suitable mapping is found will return -1. If is a persistent interface then will
     * return -2 meaning persist against *any* mapping
     * @param ec ExecutionContext
     * @param value The value
     * @return The index of javaTypeMappings to use (if any), or -1 (none), or -2 (any)
     */
    public int getMappingNumberForValue(ExecutionContext ec, Object value)
    {
        if (mappingStrategy == PER_IMPLEMENTATION_MAPPING)
        {
            return super.getMappingNumberForValue(ec, value);
        }
        else if (mappingStrategy == ID_MAPPING || mappingStrategy == XCALIA_MAPPING)
        {
            return -2; // We can persist any implementation this way
        }
        else
        {
            throw new NucleusException("Mapping strategy of interface/Object fields not yet supported");
        }
    }

    /**
     * Sets the specified positions in the PreparedStatement associated with this field, and value.
     * @param ec the ExecutionContext 
     * @param ps a datastore object that executes statements in the database
     * @param pos The position(s) of the PreparedStatement to populate
     * @param value the value stored in this field
     * @param ownerOP the owner ObjectProvider
     * @param ownerFieldNumber the owner absolute field number
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] pos, Object value, ObjectProvider ownerOP, int ownerFieldNumber)
    {
        // TODO Cater for case where this mapping has no datastore columns (N-1 join table)
      /*if (getNumberOfDatastoreMappings() == 0)
        {
            if (value == null)
            {
                return;
            }
            else
            {
                ObjectProvider valueSM = ec.findObjectProvider(value);
                NucleusLogger.GENERAL.info(">> RefMapping.setObject need to process " + valueSM);
                return;
            }
        }*/

        if (mappingStrategy == PER_IMPLEMENTATION_MAPPING)
        {
            super.setObject(ec, ps, pos, value, ownerOP, ownerFieldNumber);
        }
        else if (mappingStrategy == ID_MAPPING || mappingStrategy == XCALIA_MAPPING)
        {
            if (value == null)
            {
                getJavaTypeMapping()[0].setString(ec, ps, pos, null);
            }
            else
            {
                String refString = getReferenceStringForObject(ec, value);
                getJavaTypeMapping()[0].setString(ec, ps, pos, refString);
            }
        }
    }

    /**
     * Method to retrieve an object of this type from the ResultSet.
     * @param ec ExecutionContext
     * @param rs The ResultSet
     * @param pos The parameter positions
     * @return The object
     */
    public Object getObject(ExecutionContext ec, final ResultSet rs, int[] pos)
    {
        // TODO Cater for case where this mapping has no datastore columns (N-1 join table)
      /*if (getNumberOfDatastoreMappings() == 0)
        {
            NucleusLogger.PERSISTENCE.debug("ReferenceMapping.getObject to extract related object from join table mapping at " + mmd.getFullFieldName() + " not yet supported");
        }*/

        if (mappingStrategy == PER_IMPLEMENTATION_MAPPING)
        {
            return super.getObject(ec, rs, pos);
        }
        else if (mappingStrategy == ID_MAPPING || mappingStrategy == XCALIA_MAPPING)
        {
            String refString = getJavaTypeMapping()[0].getString(ec, rs, pos);
            if (refString == null)
            {
                return null;
            }
            else
            {
                return getObjectForReferenceString(ec, refString);
            }
        }
        else
        {
            throw new NucleusException("Mapping strategy of interface/Object fields not yet supported");
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#getJavaType()
     */
    public Class getJavaType()
    {
        return null;
    }

    /**
     * Method to convert an object to be stored into a "reference string" to store.
     * Reference string is of the form :
     * <ul>
     * <li>ID_MAPPING : "{classname}:{id}"</li>
     * <li>XCALIA_MAPPING (datastore-id) : "{definer}:{id-key}" where definer is discriminator/classname</li>
     * <li>XCALIA_MAPPING (app-id) : "{definer}:{id}" where definer is discriminator/classname</li>
     * </ul>
     * @param ec ExecutionContext
     * @param value The object
     * @return The reference string
     */
    protected String getReferenceStringForObject(ExecutionContext ec, Object value)
    {
        if (ec.getApiAdapter().isPersistable(value))
        {
            ObjectProvider op = ec.findObjectProvider(value);
            if (op == null)
            {
                // Referenced object is not yet persistent, so persist it
                ec.persistObjectInternal(value, null, -1, ObjectProvider.PC);
                op = ec.findObjectProvider(value);
                op.flush(); // Make sure the object is in the datastore so the id is set
            }

            String refString = null;
            if (mappingStrategy == ID_MAPPING)
            {
                refString = value.getClass().getName() + ":" + op.getInternalObjectId();
            }
            else if (mappingStrategy == XCALIA_MAPPING)
            {
                AbstractClassMetaData cmd = op.getClassMetaData();
                DiscriminatorMetaData dismd = cmd.getDiscriminatorMetaData();
                String definer = null;
                if (dismd != null && dismd.getValue() != null)
                {
                    definer = dismd.getValue();
                }
                else
                {
                    definer = cmd.getFullClassName();
                }
                if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    refString = definer + ":" + IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId());
                }
                else
                {
                    refString = definer + ":" + op.getInternalObjectId().toString();
                }
            }
            return refString;
        }
        else
        {
            // Cater for non-persistable objects
            throw new NucleusException("Identity mapping of non-persistable interface/Object fields not supported");
        }
    }

    /**
     * Method to convert a "reference string" into the associated object.
     * Reference string is of the form :
     * <ul>
     * <li>ID_MAPPING : "{classname}:{id}"</li>
     * <li>XCALIA_MAPPING (datastore-id) : "{definer}:{id-key}" where definer is discriminator/classname</li>
     * <li>XCALIA_MAPPING (app-id) : "{definer}:{id}" where definer is discriminator/classname</li>
     * </ul>
     * @param ec execution context
     * @param refString The reference string
     * @return The referenced object
     */
    protected Object getObjectForReferenceString(ExecutionContext ec, String refString)
    {
        int sepPos = refString.indexOf(':');
        String refDefiner = refString.substring(0, sepPos);;
        String refClassName = null;
        String refId = refString.substring(sepPos+1);
        AbstractClassMetaData refCmd = null;
        if (mappingStrategy == ID_MAPPING)
        {
            refCmd = ec.getMetaDataManager().getMetaDataForClass(refDefiner, ec.getClassLoaderResolver());
        }
        else
        {
            refCmd = ec.getMetaDataManager().getMetaDataForClass(refDefiner, ec.getClassLoaderResolver());
            if (refCmd == null)
            {
                refCmd = ec.getMetaDataManager().getMetaDataForDiscriminator(refDefiner);
            }
        }
        if (refCmd == null)
        {
            throw new NucleusException("Reference field contains reference to class of type " + refDefiner + " but no metadata found for this class");
        }
        else
        {
            refClassName = refCmd.getFullClassName();
        }

        // Obtain the identity
        Object id = null;
        if (refCmd.getIdentityType() == IdentityType.DATASTORE)
        {
            if (mappingStrategy == ID_MAPPING)
            {
                // refId is the OID.toString() form
                id = ec.getNucleusContext().getIdentityManager().getDatastoreId(refId);
            }
            else if (mappingStrategy == XCALIA_MAPPING)
            {
                // refId is simply the OID key in this case
                id = ec.getNucleusContext().getIdentityManager().getDatastoreId(refCmd.getFullClassName(), refId);
            }
        }
        else if (refCmd.getIdentityType() == IdentityType.APPLICATION)
        {
            id = ec.getNucleusContext().getIdentityManager().getApplicationId(ec.getClassLoaderResolver(), refCmd, refId);
        }

        // Retrieve the referenced object with this id
        return ec.findObject(id, true, false, refClassName);
    }

    // -------------------------- MappingCallbacks methods ----------------------------

    /**
     * Method executed just after a fetch of the owning object, allowing any necessary action
     * to this field and the object stored in it.
     * @param op ObjectProvider for the owner.
     */
    public void postFetch(ObjectProvider op)
    {
    }

    /**
     * Method executed just after the insert of the owning object, allowing any necessary action
     * to this field and the object stored in it.
     * @param op ObjectProvider for the owner.
     */
    public void insertPostProcessing(ObjectProvider op)
    {
    }

    /**
     * Method executed just after the insert of the owning object, allowing any necessary action
     * to this field and the object stored in it.
     * @param op ObjectProvider for the owner.
     */
    public void postInsert(ObjectProvider op)
    {
    }

    /**
     * Method executed just afer any update of the owning object, allowing any necessary action
     * to this field and the object stored in it.
     * @param op ObjectProvider for the owner.
     */
    public void postUpdate(ObjectProvider op)
    {
    }

    /**
     * Method executed just before the owning object is deleted, allowing tidying up of any
     * relation information.
     * @param op ObjectProvider for the owner.
     */
    public void preDelete(ObjectProvider op)
    {
        boolean isDependentElement = mmd.isDependent();
        if (!isDependentElement)
        {
            // Not dependent so do nothing, or should we null here ?
            return;
        }

        // Loop through all implementations
        for (int i=0;i<javaTypeMappings.length;i++)
        {
            final JavaTypeMapping mapping = javaTypeMappings[i];
            if (mapping instanceof PersistableMapping)
            {
                // makes sure field is loaded
                int fieldNumber = getMemberMetaData().getAbsoluteFieldNumber();
                op.isLoaded(fieldNumber);
                Object pc = op.provideField(fieldNumber);
                if (pc != null)
                {
                    // Null out the FK in the datastore using a direct update (since we are deleting)
                    op.replaceFieldMakeDirty(fieldNumber, null);
                    storeMgr.getPersistenceHandler().updateObject(op, new int[]{fieldNumber});

                    // delete object
                    op.getExecutionContext().deleteObjectInternal(pc);
                }
            }
        }
    }
}