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
package org.datanucleus.store.rdbms.scostore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.Transaction;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.MapMetaData;
import org.datanucleus.metadata.MapMetaData.MapType;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.rdbms.exceptions.ClassDefinitionException;
import org.datanucleus.store.rdbms.mapping.MappingHelper;
import org.datanucleus.store.rdbms.mapping.MappingType;
import org.datanucleus.store.rdbms.mapping.StatementClassMapping;
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;
import org.datanucleus.store.rdbms.mapping.datastore.AbstractDatastoreMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedKeyPCMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedPCMapping;
import org.datanucleus.store.rdbms.mapping.java.SerialisedReferenceMapping;
import org.datanucleus.store.rdbms.JDBCUtils;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.SQLController;
import org.datanucleus.store.rdbms.query.PersistentClassROF;
import org.datanucleus.store.rdbms.query.ResultObjectFactory;
import org.datanucleus.store.rdbms.query.StatementParameterMapping;
import org.datanucleus.store.rdbms.sql.DiscriminatorStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLStatement;
import org.datanucleus.store.rdbms.sql.SQLStatementHelper;
import org.datanucleus.store.rdbms.sql.SQLTable;
import org.datanucleus.store.rdbms.sql.SelectStatement;
import org.datanucleus.store.rdbms.sql.SelectStatementGenerator;
import org.datanucleus.store.rdbms.sql.UnionStatementGenerator;
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType;
import org.datanucleus.store.rdbms.sql.expression.SQLExpression;
import org.datanucleus.store.rdbms.sql.expression.SQLExpressionFactory;
import org.datanucleus.store.rdbms.table.JoinTable;
import org.datanucleus.store.types.scostore.CollectionStore;
import org.datanucleus.store.types.scostore.MapStore;
import org.datanucleus.store.types.scostore.SetStore;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;

/**
 * RDBMS-specific implementation of an {@link MapStore} where either the value has a FK to the owner (and the key
 * stored in the value), or whether the key has a FK to the owner (and the value stored in the key).
 */
public class FKMapStore<K, V> extends AbstractMapStore<K, V>
{
    /** Statement for updating a foreign key for the map. */
    private String updateFkStmt;

    /** JDBC statement to use for retrieving the value of the map for a key (locking). */
    private String getStmtLocked = null;

    /** JDBC statement to use for retrieving the value of the map for a key (not locking). */
    private String getStmtUnlocked = null;

    private StatementClassMapping getMappingDef = null;
    private StatementParameterMapping getMappingParams = null;

    /** Field number of owner link in value class. */
    private final int ownerFieldNumber;

    /** Field number of key in value class (when Key=Non-PC, Value=PC). */
    protected int keyFieldNumber = -1;

    /** Field number of value in key class (when Key=PC, value=Non-PC). */
    private int valueFieldNumber = -1;

    /**
     * Constructor for the backing store for an FK Map for RDBMS.
     * @param mmd Field Meta-Data for the Map field.
     * @param storeMgr The Store Manager we are using.
     * @param clr The ClassLoaderResolver
     */
    public FKMapStore(AbstractMemberMetaData mmd, RDBMSStoreManager storeMgr, ClassLoaderResolver clr)
    {
        super(storeMgr, clr);
        setOwner(mmd);
        MapMetaData mapmd = (MapMetaData)mmd.getContainer();
        if (mapmd == null)
        {
            // No <map> specified for this field!
            throw new NucleusUserException(Localiser.msg("056002", mmd.getFullFieldName()));
        }

        // Check whether we store the key in the value, or the value in the key
        boolean keyStoredInValue = false;
        if (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getMappedBy() != null)
        {
            keyStoredInValue = true;
        }
        else if (mmd.getValueMetaData() != null && mmd.getValueMetaData().getMappedBy() == null)
        {
            // No mapped-by specified on either key or value so we dont know what to do with this relation!
            throw new NucleusUserException(Localiser.msg("056071", mmd.getFullFieldName()));
        }
        else
        {
            // Should throw an exception since must store key in value or value in key
        }

        // Load the key and value classes
        keyType = mapmd.getKeyType();
        valueType = mapmd.getValueType();
        Class keyClass = clr.classForName(keyType);
        Class valueClass = clr.classForName(valueType);

        ApiAdapter api = getStoreManager().getApiAdapter();
        if (keyStoredInValue && !api.isPersistable(valueClass))
        {
            // key stored in value but value is not PC!
            throw new NucleusUserException(Localiser.msg("056072", mmd.getFullFieldName(), valueType));
        }
        if (!keyStoredInValue && !api.isPersistable(keyClass))
        {
            // value stored in key but key is not PC!
            throw new NucleusUserException(Localiser.msg("056073", mmd.getFullFieldName(), keyType));
        }

        String ownerFieldName = mmd.getMappedBy();
        if (keyStoredInValue)
        {
            // Key = field in value, Value = PC
            valueCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(valueClass, clr);
            if (valueCmd == null)
            {
                // Value has no MetaData!
                throw new NucleusUserException(Localiser.msg("056070", valueType, mmd.getFullFieldName()));
            }

            valueTable = storeMgr.getDatastoreClass(valueType, clr);
            valueMapping  = storeMgr.getDatastoreClass(valueType, clr).getIdMapping();
            valuesAreEmbedded = false;
            valuesAreSerialised = false;

            if (mmd.getMappedBy() != null)
            {
                // 1-N bidirectional : The value class has a field for the owner.
                AbstractMemberMetaData vofmd = valueCmd.getMetaDataForMember(ownerFieldName);
                if (vofmd == null)
                {
                    throw new NucleusUserException(Localiser.msg("056067", mmd.getFullFieldName(), ownerFieldName, valueClass.getName()));
                }

                // Check that the type of the value "mapped-by" field is consistent with the owner type
                if (!clr.isAssignableFrom(vofmd.getType(), mmd.getAbstractClassMetaData().getFullClassName()))
                {
                    throw new NucleusUserException(Localiser.msg("056068", mmd.getFullFieldName(),
                        vofmd.getFullFieldName(), vofmd.getTypeName(), mmd.getAbstractClassMetaData().getFullClassName()));
                }

                ownerFieldNumber = valueCmd.getAbsolutePositionOfMember(ownerFieldName);
                ownerMapping = valueTable.getMemberMapping(vofmd);
                if (ownerMapping == null)
                {
                    throw new NucleusUserException(Localiser.msg("RDBMS.SCO.Map.InverseOwnerMappedByFieldNotPresent", 
                        mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), valueType, ownerFieldName));
                }
                if (isEmbeddedMapping(ownerMapping))
                {
                    throw new NucleusUserException(Localiser.msg("056055", ownerFieldName, valueType, vofmd.getTypeName(), mmd.getClassName()));
                }
            }
            else
            {
                // 1-N Unidirectional : The value class knows nothing about the owner
                ownerFieldNumber = -1;
                ownerMapping = valueTable.getExternalMapping(mmd, MappingType.EXTERNAL_FK);
                if (ownerMapping == null)
                {
                    throw new NucleusUserException(Localiser.msg("056056", mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), valueType));
                }
            }

            if (mmd.getKeyMetaData() == null || mmd.getKeyMetaData().getMappedBy() == null)
            {
                throw new NucleusUserException(Localiser.msg("056050", valueClass.getName()));
            }

            AbstractMemberMetaData vkfmd = null;
            String key_field_name = mmd.getKeyMetaData().getMappedBy();
            if (key_field_name != null)
            {
                // check if key field exists in the ClassMetaData for the element-value type
                AbstractClassMetaData vkCmd = storeMgr.getMetaDataManager().getMetaDataForClass(valueClass, clr);
                vkfmd = (vkCmd != null ? vkCmd.getMetaDataForMember(key_field_name) : null);
                if (vkfmd == null)
                {
                    throw new NucleusUserException(Localiser.msg("056052", valueClass.getName(), key_field_name));
                }
            }
            if (vkfmd == null)
            {
                throw new ClassDefinitionException(Localiser.msg("056050", mmd.getFullFieldName()));
            }

            // Check that the key type is correct for the declared type
            if (!ClassUtils.typesAreCompatible(vkfmd.getType(), keyType, clr))
            {
                throw new NucleusUserException(Localiser.msg("056051", mmd.getFullFieldName(), keyType, vkfmd.getType().getName()));
            }

            // Set up key field
            String keyFieldName = vkfmd.getName();
            keyFieldNumber = valueCmd.getAbsolutePositionOfMember(keyFieldName);
            keyMapping = valueTable.getMemberMapping(valueCmd.getMetaDataForManagedMemberAtAbsolutePosition(keyFieldNumber));
            if (keyMapping == null)
            {
                throw new NucleusUserException(Localiser.msg("056053", mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), valueType, keyFieldName));
            }

            if (!keyMapping.hasSimpleDatastoreRepresentation())
            {
                // Check the type of the mapping
                throw new NucleusUserException("Invalid field type for map key field: " + mmd.getFullFieldName());
            }
            keysAreEmbedded = isEmbeddedMapping(keyMapping);
            keysAreSerialised = isEmbeddedMapping(keyMapping);

            mapTable = valueTable;
            if (mmd.getMappedBy() != null && ownerMapping.getTable() != mapTable)
            {
                // Value and owner don't have consistent tables so use the one with the mapping
                // e.g map value is subclass, yet superclass has the link back to the owner
                mapTable = ownerMapping.getTable();
            }
        }
        else
        {
            // Key = PC, Value = field in key
            keyCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(keyClass, clr);
            if (keyCmd == null)
            {
                // Key has no MetaData!
                throw new NucleusUserException(Localiser.msg("056069", keyType, mmd.getFullFieldName()));
            }

            // TODO This should be called keyvalueTable or something and not valueTable
            valueTable = storeMgr.getDatastoreClass(keyType, clr);
            keyMapping  = storeMgr.getDatastoreClass(keyType, clr).getIdMapping();
            keysAreEmbedded = false;
            keysAreSerialised = false;

            if (mmd.getMappedBy() != null)
            {
                // 1-N bidirectional : The key class has a field for the owner.
                AbstractMemberMetaData kofmd = keyCmd.getMetaDataForMember(ownerFieldName);
                if (kofmd == null)
                {
                    throw new NucleusUserException(Localiser.msg("056067", mmd.getFullFieldName(), ownerFieldName, keyClass.getName()));
                }

                // Check that the type of the key "mapped-by" field is consistent with the owner type
                if (!ClassUtils.typesAreCompatible(kofmd.getType(), mmd.getAbstractClassMetaData().getFullClassName(), clr))
                {
                    throw new NucleusUserException(Localiser.msg("056068", mmd.getFullFieldName(),
                        kofmd.getFullFieldName(), kofmd.getTypeName(), mmd.getAbstractClassMetaData().getFullClassName()));
                }

                ownerFieldNumber = keyCmd.getAbsolutePositionOfMember(ownerFieldName);
                ownerMapping = valueTable.getMemberMapping(kofmd);
                if (ownerMapping == null)
                {
                    throw new NucleusUserException(Localiser.msg("RDBMS.SCO.Map.InverseOwnerMappedByFieldNotPresent", 
                        mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), keyType, ownerFieldName));
                }
                if (isEmbeddedMapping(ownerMapping))
                {
                    throw new NucleusUserException(Localiser.msg("056055", ownerFieldName, keyType, kofmd.getTypeName(), mmd.getClassName()));
                }
            }
            else
            {
                // 1-N Unidirectional : The key class knows nothing about the owner
                ownerFieldNumber = -1;
                ownerMapping = valueTable.getExternalMapping(mmd, MappingType.EXTERNAL_FK);
                if (ownerMapping == null)
                {
                    throw new NucleusUserException(Localiser.msg("056056", mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), keyType));
                }
            }

            if (mmd.getValueMetaData() == null || mmd.getValueMetaData().getMappedBy() == null)
            {
                throw new NucleusUserException(Localiser.msg("056057", keyClass.getName()));
            }

            AbstractMemberMetaData vkfmd = null;
            String value_field_name = mmd.getValueMetaData().getMappedBy();
            if (value_field_name != null)
            {
                // check if value field exists in the ClassMetaData for the element-value type
                AbstractClassMetaData vkCmd = storeMgr.getMetaDataManager().getMetaDataForClass(keyClass, clr);
                vkfmd = (vkCmd != null ? vkCmd.getMetaDataForMember(value_field_name) : null);
                if (vkfmd == null)
                {
                    throw new NucleusUserException(Localiser.msg("056059", keyClass.getName(), value_field_name));
                }
            }
            if (vkfmd == null)
            {
                throw new ClassDefinitionException(Localiser.msg("056057", mmd.getFullFieldName()));
            }

            // Check that the value type is consistent with the declared type
            if (!ClassUtils.typesAreCompatible(vkfmd.getType(), valueType, clr))
            {
                throw new NucleusUserException(Localiser.msg("056058", mmd.getFullFieldName(), valueType, vkfmd.getType().getName()));
            }

            // Set up value field
            String valueFieldName = vkfmd.getName();
            valueFieldNumber = keyCmd.getAbsolutePositionOfMember(valueFieldName);
            valueMapping = valueTable.getMemberMapping(keyCmd.getMetaDataForManagedMemberAtAbsolutePosition(valueFieldNumber));
            if (valueMapping == null)
            {
                throw new NucleusUserException(Localiser.msg("056054", mmd.getAbstractClassMetaData().getFullClassName(), mmd.getName(), keyType, valueFieldName));
            }

            if (!valueMapping.hasSimpleDatastoreRepresentation())
            {
                // Check the type of the mapping
                throw new NucleusUserException("Invalid field type for map value field: " + mmd.getFullFieldName());
            }
            valuesAreEmbedded = isEmbeddedMapping(valueMapping);
            valuesAreSerialised = isEmbeddedMapping(valueMapping);

            mapTable = valueTable;
            if (mmd.getMappedBy() != null && ownerMapping.getTable() != mapTable)
            {
                // Key and owner don't have consistent tables so use the one with the mapping
                // e.g map key is subclass, yet superclass has the link back to the owner
                mapTable = ownerMapping.getTable();
            }
        }

        // Generate the statements
        initialise();
    }

    protected void initialise()
    {
        super.initialise();
        updateFkStmt = getUpdateFkStmt();
    }

    /**
     * Utility to update a foreign-key in the value in the case of a unidirectional 1-N relationship.
     * @param op ObjectProvider for the owner
     * @param value The value to update
     * @param owner The owner object to set in the FK
     * @return Whether it was performed successfully
     */
    private boolean updateValueFk(ObjectProvider op, Object value, Object owner)
    {
        if (value == null)
        {
            return false;
        }
        validateValueForWriting(op, value);
        return updateValueFkInternal(op, value, owner);
    }

    /**
     * Utility to update a foreign-key in the key in the case of
     * a unidirectional 1-N relationship.
     * @param op ObjectProvider for the owner
     * @param key The key to update
     * @param owner The owner object to set in the FK
     * @return Whether it was performed successfully
     */
    private boolean updateKeyFk(ObjectProvider op, Object key, Object owner)
    {
        if (key == null)
        {
            return false;
        }
        validateKeyForWriting(op, key);
        return updateKeyFkInternal(op, key, owner);
    }

    /**
     * Utility to validate the type of a value for storing in the Map.
     * @param value The value to check.
     * @param clr The ClassLoaderResolver
     **/
    protected void validateValueType(ClassLoaderResolver clr, Object value)
    {
        if (value == null)
        {
            throw new NullPointerException(Localiser.msg("056063"));
        }

        super.validateValueType(clr, value);
    }

    /**
     * Method to put an item in the Map.
     * @param op ObjectProvider for the map.
     * @param newKey The key to store the value against
     * @param newValue The value to store.
     * @return The value stored.
     */
    public V put(final ObjectProvider op, final K newKey, V newValue)
    {
        ExecutionContext ec = op.getExecutionContext();
        if (keyFieldNumber >= 0)
        {
            validateKeyForWriting(op, newKey);
            validateValueType(ec.getClassLoaderResolver(), newValue);
        }
        else
        {
            validateKeyType(ec.getClassLoaderResolver(), newKey);
            validateValueForWriting(op, newValue);
        }

        // Check if there is an existing value for this key
        V oldValue = get(op, newKey);
        if (oldValue != newValue)
        {
            if (valueCmd != null)
            {
                if (oldValue != null && !oldValue.equals(newValue))
                {
                    // Key is stored in the value and the value has changed so remove the old value
                    removeValue(op, newKey, oldValue);
                }

                final Object newOwner = op.getObject();

                if (ec.getApiAdapter().isPersistent(newValue))
                {
                    /*
                     * The new value is already persistent.
                     *
                     * "Put" the new value in the map by updating its owner and key
                     * fields to the appropriate values.  This is done with the same
                     * methods the PC itself would use if the application code
                     * modified the fields.  It should result in no actual database
                     * activity if the fields were already set to the right values.
                     */
                    if (ec != ec.getApiAdapter().getExecutionContext(newValue))
                    {
                        throw new NucleusUserException(Localiser.msg("RDBMS.SCO.Map.WriteValueInvalidWithDifferentPM"), ec.getApiAdapter().getIdForObject(newValue));
                    }

                    ObjectProvider vsm = ec.findObjectProvider(newValue);
                    
                    // Ensure the current owner field is loaded, and replace with new value
                    if (ownerFieldNumber >= 0)
                    {
                        vsm.isLoaded(ownerFieldNumber);
                        Object oldOwner = vsm.provideField(ownerFieldNumber);
                        vsm.replaceFieldMakeDirty(ownerFieldNumber, newOwner);
                        if (ec.getManageRelations())
                        {
                            ec.getRelationshipManager(vsm).relationChange(ownerFieldNumber, oldOwner, newOwner);
                        }
                    }
                    else
                    {
                        updateValueFk(op, newValue, newOwner);
                    }

                    // Ensure the current key field is loaded, and replace with new value
                    vsm.isLoaded(keyFieldNumber);
                    Object oldKey = vsm.provideField(keyFieldNumber);
                    vsm.replaceFieldMakeDirty(keyFieldNumber, newKey);
                    if (ec.getManageRelations())
                    {
                        ec.getRelationshipManager(vsm).relationChange(keyFieldNumber, oldKey, newKey);
                    }
                }
                else
                {                  
                    /*
                     * The new value is not yet persistent.
                     *
                     * Update its owner and key fields to the appropriate values and
                     * *then* make it persistent.  Making the changes before DB
                     * insertion avoids an unnecessary UPDATE allows the owner
                     * and/or key fields to be non-nullable.
                     */
                    ec.persistObjectInternal(newValue, new FieldValues()
                        {
                        public void fetchFields(ObjectProvider vsm)
                        {
                            if (ownerFieldNumber >= 0)
                            {
                                vsm.replaceFieldMakeDirty(ownerFieldNumber, newOwner);
                            }
                            vsm.replaceFieldMakeDirty(keyFieldNumber, newKey);

                            JavaTypeMapping externalFKMapping = valueTable.getExternalMapping(ownerMemberMetaData, MappingType.EXTERNAL_FK);
                            if (externalFKMapping != null)
                            {
                                // Set the owner in the value object where appropriate
                                vsm.setAssociatedValue(externalFKMapping, op.getObject());
                            }
                        }
                        public void fetchNonLoadedFields(ObjectProvider op)
                        {
                        }
                        public FetchPlan getFetchPlanForLoading()
                        {
                            return null;
                        }
                        }, ObjectProvider.PC);
                }
            }
            else
            {
                // Value is stored in the key
                final Object newOwner = op.getObject();

                if (ec.getApiAdapter().isPersistent(newKey))
                {
                    /*
                     * The new key is already persistent.
                     *
                     * "Put" the new key in the map by updating its owner and value
                     * fields to the appropriate values. This is done with the same
                     * methods the PC itself would use if the application code
                     * modified the fields. It should result in no actual database
                     * activity if the fields were already set to the right values.
                     */
                    if (ec != ec.getApiAdapter().getExecutionContext(newKey))
                    {
                        throw new NucleusUserException(Localiser.msg("056060"),
                            ec.getApiAdapter().getIdForObject(newKey));
                    }

                    ObjectProvider valOP = ec.findObjectProvider(newKey);

                    // Ensure the current owner field is loaded, and replace with new key
                    if (ownerFieldNumber >= 0)
                    {
                        valOP.isLoaded(ownerFieldNumber);
                        Object oldOwner = valOP.provideField(ownerFieldNumber);
                        valOP.replaceFieldMakeDirty(ownerFieldNumber, newOwner);
                        if (ec.getManageRelations())
                        {
                            ec.getRelationshipManager(valOP).relationChange(ownerFieldNumber, oldOwner, newOwner);
                        }
                    }
                    else
                    {
                        updateKeyFk(op, newKey, newOwner);
                    }

                    // Ensure the current value field is loaded, and replace with new value
                    valOP.isLoaded(valueFieldNumber);
                    oldValue = (V) valOP.provideField(valueFieldNumber); // TODO Should we update the local variable ?
                    valOP.replaceFieldMakeDirty(valueFieldNumber, newValue);
                    if (ec.getManageRelations())
                    {
                        ec.getRelationshipManager(valOP).relationChange(valueFieldNumber, oldValue, newValue);
                    }
                }
                else
                {
                    /*
                     * The new key is not yet persistent.
                     *
                     * Update its owner and key fields to the appropriate values and
                     * *then* make it persistent.  Making the changes before DB
                     * insertion avoids an unnecessary UPDATE allows the owner
                     * and/or key fields to be non-nullable.
                     */
                    final Object newValueObj = newValue;
                    ec.persistObjectInternal(newKey, new FieldValues()
                        {
                        public void fetchFields(ObjectProvider vsm)
                        {
                            if (ownerFieldNumber >= 0)
                            {
                                vsm.replaceFieldMakeDirty(ownerFieldNumber, newOwner);
                            }
                            vsm.replaceFieldMakeDirty(valueFieldNumber, newValueObj);

                            JavaTypeMapping externalFKMapping = valueTable.getExternalMapping(ownerMemberMetaData, MappingType.EXTERNAL_FK);
                            if (externalFKMapping != null)
                            {
                                // Set the owner in the value object where appropriate
                                vsm.setAssociatedValue(externalFKMapping, op.getObject());
                            }
                        }
                        public void fetchNonLoadedFields(ObjectProvider op)
                        {
                        }
                        public FetchPlan getFetchPlanForLoading()
                        {
                            return null;
                        }
                        }, ObjectProvider.PC
                    );

                    /*if (ownerFieldNumber < 0)
                    {
                        // TODO Think about removing this since we set the associated owner here
                        updateKeyFk(sm, newKey, newOwner);
                    }*/
                }
            }
        }

        // TODO Cater for key being PC and having delete-dependent
        if (ownerMemberMetaData.getMap().isDependentValue() && oldValue != null)
        {
            // Delete the old value if it is no longer contained and is dependent
            if (!containsValue(op, oldValue))
            {
                ec.deleteObjectInternal(oldValue);
            }
        }

        return oldValue;
    }

    /**
     * Method to remove an entry from the map.
     * @param op ObjectProvider for the map.
     * @param key Key of the entry to remove.
     * @return The value that was removed.
     */
    public V remove(ObjectProvider op, Object key)
    {
        if (!allowNulls && key == null)
        {
            // Just return
            return null;
        }
        Object oldValue = get(op, key);
        return remove(op, key, oldValue);
    }

    /**
     * Method to remove an entry from the map.
     * @param op ObjectProvider for the map.
     * @param key Key of the entry to remove.
     * @return The value that was removed.
     */
    public V remove(ObjectProvider op, Object key, Object oldValue)
    {
        ExecutionContext ec = op.getExecutionContext();
        if (keyFieldNumber >= 0)
        {
            // Key stored in value
            if (oldValue != null)
            {
                boolean deletingValue = false;
                ObjectProvider valueOP = ec.findObjectProvider(oldValue);
                if (ownerMemberMetaData.getMap().isDependentValue())
                {
                    // Delete the value if it is dependent
                    deletingValue = true;
                    ec.deleteObjectInternal(oldValue);
                    valueOP.flush();
                }
                else if (ownerMapping.isNullable())
                {
                    // Null the owner FK
                    if (ownerFieldNumber >= 0)
                    {
                        // Update the field in the value
                        Object oldOwner = valueOP.provideField(ownerFieldNumber);
                        valueOP.replaceFieldMakeDirty(ownerFieldNumber, null);
                        valueOP.flush();
                        if (ec.getManageRelations())
                        {
                            ec.getRelationshipManager(valueOP).relationChange(ownerFieldNumber, oldOwner, null);
                        }
                    }
                    else
                    {
                        // Update the external FK in the value in the datastore
                        updateValueFkInternal(op, oldValue, null);
                    }
                }
                else
                {
                    // Not nullable, so must delete since no other way of removing from map
                    deletingValue = true;
                    ec.deleteObjectInternal(oldValue);
                    valueOP.flush();
                }

                if (ownerMemberMetaData.getMap().isDependentKey())
                {
                    // Delete the key since it is dependent
                    if (!deletingValue)
                    {
                        // Null FK in value to key
                        if (keyMapping.isNullable())
                        {
                            valueOP.replaceFieldMakeDirty(keyFieldNumber, null);
                            valueOP.flush();
                            if (ec.getManageRelations())
                            {
                                ec.getRelationshipManager(valueOP).relationChange(keyFieldNumber, key, null);
                            }
                        }
                    }
                    ec.deleteObjectInternal(key);
                    ObjectProvider keyOP = ec.findObjectProvider(key);
                    keyOP.flush();
                }
            }
        }
        else
        {
            // Value stored in key
            if (key != null)
            {
                boolean deletingKey = false;
                ObjectProvider keyOP = ec.findObjectProvider(key);
                if (ownerMemberMetaData.getMap().isDependentKey())
                {
                    // Delete the key if it is dependent
                    deletingKey = true;
                    ec.deleteObjectInternal(key);
                    keyOP.flush();
                }
                else if (ownerMapping.isNullable())
                {
                    // Null the owner FK
                    if (ownerFieldNumber >= 0)
                    {
                        // Update the field in the key
                        Object oldOwner = keyOP.provideField(ownerFieldNumber);
                        keyOP.replaceFieldMakeDirty(ownerFieldNumber, null);
                        keyOP.flush();
                        if (ec.getManageRelations())
                        {
                            ec.getRelationshipManager(keyOP).relationChange(ownerFieldNumber, oldOwner, null);
                        }
                    }
                    else
                    {
                        // Update the external FK in the key in the datastore
                        updateKeyFkInternal(op, key, null);
                    }
                }
                else
                {
                    // Not nullable, so must delete since no other way of removing from map
                    deletingKey = true;
                    ec.deleteObjectInternal(key);
                    keyOP.flush();
                }

                if (ownerMemberMetaData.getMap().isDependentValue())
                {
                    // Delete the value since it is dependent
                    if (!deletingKey)
                    {
                        // Null FK in key to value
                        if (valueMapping.isNullable())
                        {
                            keyOP.replaceFieldMakeDirty(valueFieldNumber, null);
                            keyOP.flush();
                            if (ec.getManageRelations())
                            {
                                ec.getRelationshipManager(keyOP).relationChange(valueFieldNumber, oldValue, null);
                            }
                        }
                    }
                    ec.deleteObjectInternal(oldValue);
                    ObjectProvider valOP = ec.findObjectProvider(oldValue);
                    valOP.flush();
                }
            }
        }

        return (V) oldValue;
    }

    /**
     * Utility to remove a value from the Map.
     * @param op ObjectProvider for the map.
     * @param key Key of the object
     * @param oldValue Value to remove
     */
    private void removeValue(ObjectProvider op, Object key, Object oldValue)
    {
        ExecutionContext ec = op.getExecutionContext();
        
        // Null out the key and owner fields if they are nullable
        if (keyMapping.isNullable())
        {
            ObjectProvider vsm = ec.findObjectProvider(oldValue);
            
            // Null the key field
            vsm.replaceFieldMakeDirty(keyFieldNumber, null);
            if (ec.getManageRelations())
            {
                ec.getRelationshipManager(vsm).relationChange(keyFieldNumber, key, null);
            }
            
            // Null the owner field
            if (ownerFieldNumber >= 0)
            {
                Object oldOwner = vsm.provideField(ownerFieldNumber);
                vsm.replaceFieldMakeDirty(ownerFieldNumber, null);
                if (ec.getManageRelations())
                {
                    ec.getRelationshipManager(vsm).relationChange(ownerFieldNumber, oldOwner, null);
                }
            }
            else
            {
                updateValueFk(op, oldValue, null);
            }
        }
        // otherwise just delete the item
        else
        {
            ec.deleteObjectInternal(oldValue);
        }
    }

    /**
     * Method to clear the map of all values.
     * @param op ObjectProvider for the map.
     */
    public void clear(ObjectProvider op)
    {
        // TODO Fix this. Should not be retrieving objects only to remove them since they
        // may be cached in the SCO object. But we need to utilise delete-dependent correctly too
        Iterator iter = keySetStore().iterator(op);
        while (iter.hasNext())
        {
            Object key = iter.next();
            if (key == null && !allowNulls)
            {
                // Do nothing
            }
            else
            {
                remove(op, key);
            }
        }
    }

    /**
     * Utility to clear the key of a value from the Map.
     * If the key is non nullable, delete the value.
     * @param op ObjectProvider for the map.
     * @param key Key of the object
     * @param oldValue Value to remove
     */
    public void clearKeyOfValue(ObjectProvider op, Object key, Object oldValue)
    {
        ExecutionContext ec = op.getExecutionContext();

        if (keyMapping.isNullable())
        {
            // Null out the key and owner fields if they are nullable
            ObjectProvider vsm = ec.findObjectProvider(oldValue);

            // Check that the value hasn't already been deleted due to being removed from the map
            if (!ec.getApiAdapter().isDeleted(oldValue))
            {
                // Null the key field
                vsm.replaceFieldMakeDirty(keyFieldNumber, null);
                if (ec.getManageRelations())
                {
                    ec.getRelationshipManager(vsm).relationChange(keyFieldNumber, key, null);
                }
            }
        }
        else
        {
            // otherwise just delete the value
            ec.deleteObjectInternal(oldValue);
        }
    }

    /**
     * Accessor for the keys in the Map.
     * @return The keys
     */
    public synchronized SetStore keySetStore()
    {
        return new MapKeySetStore(valueTable, this, clr);
    }

    /**
     * Accessor for the values in the Map.
     * @return The values.
     */
    public synchronized CollectionStore valueCollectionStore()
    {
        return new MapValueCollectionStore(valueTable, this, clr);
    }

    /**
     * Accessor for the map entries in the Map.
     * @return The map entries.
     */
    public synchronized SetStore entrySetStore()
    {
        return new MapEntrySetStore(valueTable, this, clr);
    }

    /**
     * Generate statement for updating a Foreign Key from key/value to owner in an inverse 1-N.
     * <PRE>
     * UPDATE MAPTABLE SET FK_COL_1 = ?, FK_COL_2 = ?
     * WHERE ELEMENT_ID = ?
     * </PRE>
     * @return Statement for updating the FK in an inverse 1-N
     */
    private String getUpdateFkStmt()
    {
        StringBuilder stmt = new StringBuilder("UPDATE ");
        stmt.append(getMapTable().toString());
        stmt.append(" SET ");
        for (int i=0; i<ownerMapping.getNumberOfDatastoreMappings(); i++)
        {
            if (i > 0)
            {
                stmt.append(",");
            }
            stmt.append(ownerMapping.getDatastoreMapping(i).getColumn().getIdentifier().toString());
            stmt.append(" = ");
            stmt.append(((AbstractDatastoreMapping)ownerMapping.getDatastoreMapping(i)).getUpdateInputParameter());
        }
        stmt.append(" WHERE ");
        if (keyFieldNumber >= 0)
        {
            BackingStoreHelper.appendWhereClauseForMapping(stmt, valueMapping, null, true);
        }
        else
        {
            BackingStoreHelper.appendWhereClauseForMapping(stmt, keyMapping, null, true);
        }

        return stmt.toString();
    }

    protected boolean updateValueFkInternal(ObjectProvider op, Object value, Object owner)
    {
        boolean retval;
        ExecutionContext ec = op.getExecutionContext();
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, updateFkStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    if (owner == null)
                    {
                        if (ownerMemberMetaData != null)
                        {
                            ownerMapping.setObject(ec, ps, MappingHelper.getMappingIndices(1,ownerMapping), null, op, ownerMemberMetaData.getAbsoluteFieldNumber());
                        }
                        else
                        {
                            ownerMapping.setObject(ec, ps, MappingHelper.getMappingIndices(1,ownerMapping), null);
                        }
                        jdbcPosition += ownerMapping.getNumberOfDatastoreMappings();
                    }
                    else
                    {
                        jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                    }
                    jdbcPosition = BackingStoreHelper.populateValueInStatement(ec, ps, value, jdbcPosition, valueMapping);

                    sqlControl.executeStatementUpdate(ec, mconn, updateFkStmt, ps, true);
                    retval = true;
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
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("056027",updateFkStmt),e);
        }

        return retval;
    }

    protected boolean updateKeyFkInternal(ObjectProvider op, Object key, Object owner)
    {
        boolean retval;
        ExecutionContext ec = op.getExecutionContext();
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                PreparedStatement ps = sqlControl.getStatementForUpdate(mconn, updateFkStmt, false);
                try
                {
                    int jdbcPosition = 1;
                    if (owner == null)
                    {
                        if (ownerMemberMetaData != null)
                        {
                            ownerMapping.setObject(ec, ps, MappingHelper.getMappingIndices(1,ownerMapping), null, op, ownerMemberMetaData.getAbsoluteFieldNumber());
                        }
                        else
                        {
                            ownerMapping.setObject(ec, ps, MappingHelper.getMappingIndices(1,ownerMapping), null);
                        }
                        jdbcPosition += ownerMapping.getNumberOfDatastoreMappings();
                    }
                    else
                    {
                        jdbcPosition = BackingStoreHelper.populateOwnerInStatement(op, ec, ps, jdbcPosition, this);
                    }
                    jdbcPosition = BackingStoreHelper.populateKeyInStatement(ec, ps, key, jdbcPosition, keyMapping);

                    sqlControl.executeStatementUpdate(ec, mconn, updateFkStmt, ps, true);
                    retval = true;
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
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("056027",updateFkStmt),e);
        }

        return retval;
    }

    /**
     * Method to retrieve a value from the Map given the key.
     * @param ownerOP ObjectProvider for the owner of the map.
     * @param key The key to retrieve the value for.
     * @return The value for this key
     * @throws NoSuchElementException if the key was not found
     */
    protected V getValue(ObjectProvider ownerOP, Object key)
    throws NoSuchElementException
    {
        if (!validateKeyForReading(ownerOP, key))
        {
            return null;
        }

        ExecutionContext ec = ownerOP.getExecutionContext();
        if (getStmtLocked == null)
        {
            synchronized (this) // Make sure this completes in case another thread needs the same info
            {
                // Generate the statement, and statement mapping/parameter information
                SQLStatement sqlStmt = getSQLStatementForGet(ownerOP);
                getStmtUnlocked = sqlStmt.getSQLText().toSQL();
                sqlStmt.addExtension(SQLStatement.EXTENSION_LOCK_FOR_UPDATE, true);
                getStmtLocked = sqlStmt.getSQLText().toSQL();
            }
        }

        Transaction tx = ec.getTransaction();
        String stmt = (tx.getSerializeRead() != null && tx.getSerializeRead() ? getStmtLocked : getStmtUnlocked);
        Object value = null;
        try
        {
            ManagedConnection mconn = storeMgr.getConnection(ec);
            SQLController sqlControl = storeMgr.getSQLController();
            try
            {
                // Create the statement and supply owner/key params
                PreparedStatement ps = sqlControl.getStatementForQuery(mconn, stmt);
                StatementMappingIndex ownerIdx = getMappingParams.getMappingForParameter("owner");
                int numParams = ownerIdx.getNumberOfParameterOccurrences();
                for (int paramInstance=0;paramInstance<numParams;paramInstance++)
                {
                    ownerIdx.getMapping().setObject(ec, ps, ownerIdx.getParameterPositionsForOccurrence(paramInstance), ownerOP.getObject());
                }
                StatementMappingIndex keyIdx = getMappingParams.getMappingForParameter("key");
                numParams = keyIdx.getNumberOfParameterOccurrences();
                for (int paramInstance=0;paramInstance<numParams;paramInstance++)
                {
                    keyIdx.getMapping().setObject(ec, ps, keyIdx.getParameterPositionsForOccurrence(paramInstance), key);
                }

                try
                {
                    ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, stmt, ps);
                    try
                    {
                        boolean found = rs.next();
                        if (!found)
                        {
                            throw new NoSuchElementException();
                        }

                        if (valuesAreEmbedded || valuesAreSerialised)
                        {
                            int param[] = new int[valueMapping.getNumberOfDatastoreMappings()];
                            for (int i = 0; i < param.length; ++i)
                            {
                                param[i] = i + 1;
                            }

                            if (valueMapping instanceof SerialisedPCMapping ||
                                valueMapping instanceof SerialisedReferenceMapping ||
                                valueMapping instanceof EmbeddedKeyPCMapping)
                            {
                                // Value = Serialised
                                value = valueMapping.getObject(ec, rs, param, ownerOP, ((JoinTable)mapTable).getOwnerMemberMetaData().getAbsoluteFieldNumber());
                            }
                            else
                            {
                                // Value = Non-PC
                                value = valueMapping.getObject(ec, rs, param);
                            }
                        }
                        else if (valueMapping instanceof ReferenceMapping)
                        {
                            // Value = Reference (Interface/Object)
                            int param[] = new int[valueMapping.getNumberOfDatastoreMappings()];
                            for (int i = 0; i < param.length; ++i)
                            {
                                param[i] = i + 1;
                            }
                            value = valueMapping.getObject(ec, rs, param);
                        }
                        else
                        {
                            // Value = PC
                            ResultObjectFactory rof = new PersistentClassROF(storeMgr, valueCmd, getMappingDef, false, null, clr.classForName(valueType));
                            value = rof.getObject(ec, rs);
                        }

                        JDBCUtils.logWarnings(rs);
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
        catch (SQLException e)
        {
            throw new NucleusDataStoreException(Localiser.msg("056014", stmt), e);
        }
        return (V) value;
    }

    /**
     * Method to return an SQLStatement for retrieving the value for a key.
     * Selects the join table and optionally joins to the value table if it has its own table.
     * @param ownerOP ObjectProvider for the owning object
     * @return The SQLStatement
     */
    protected SelectStatement getSQLStatementForGet(ObjectProvider ownerOP)
    {
        SelectStatement sqlStmt = null;
        ExecutionContext ec = ownerOP.getExecutionContext();

        final ClassLoaderResolver clr = ownerOP.getExecutionContext().getClassLoaderResolver();
        final Class valueCls = clr.classForName(this.valueType);
        if (ownerMemberMetaData.getMap().getMapType() == MapType.MAP_TYPE_KEY_IN_VALUE)
        {
            getMappingDef = new StatementClassMapping();
            if (valueTable.getDiscriminatorMetaData() != null && valueTable.getDiscriminatorMetaData().getStrategy() != DiscriminatorStrategy.NONE)
            {
                // Value class has discriminator
                if (ClassUtils.isReferenceType(valueCls))
                {
                    String[] clsNames = storeMgr.getNucleusContext().getMetaDataManager().getClassesImplementingInterface(valueType, clr);
                    Class[] cls = new Class[clsNames.length];
                    for (int i=0; i<clsNames.length; i++)
                    {
                        cls[i] = clr.classForName(clsNames[i]);
                    }
                    sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr, cls, true, null, null).getStatement(ec);
                }
                else
                {
                    sqlStmt = new DiscriminatorStatementGenerator(storeMgr, clr, valueCls, true, null, null).getStatement(ec);
                }
                iterateUsingDiscriminator = true;
            }
            else
            {
                // Use union to resolve any subclasses of value
                UnionStatementGenerator stmtGen = new UnionStatementGenerator(storeMgr, clr, valueCls, true, null, null);
                stmtGen.setOption(SelectStatementGenerator.OPTION_SELECT_DN_TYPE);
                getMappingDef.setNucleusTypeColumnName(UnionStatementGenerator.DN_TYPE_COLUMN);
                sqlStmt = stmtGen.getStatement(ec);
            }

            // Select the value field(s)
            SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, getMappingDef, ec.getFetchPlan(), sqlStmt.getPrimaryTable(), valueCmd, 0);
        }
        else
        {
            // Value is in key table
            sqlStmt = new SelectStatement(storeMgr, mapTable, null, null);
            sqlStmt.setClassLoaderResolver(clr);

            if (valueCmd != null)
            {
                // Left outer join to value table (so we allow for null values)
                SQLTable valueSqlTbl = sqlStmt.join(JoinType.LEFT_OUTER_JOIN, sqlStmt.getPrimaryTable(), valueMapping, valueTable, null, valueTable.getIdMapping(), 
                    null, null, true);

                // Select the value field(s)
                SQLStatementHelper.selectFetchPlanOfSourceClassInStatement(sqlStmt, getMappingDef, ec.getFetchPlan(), valueSqlTbl, valueCmd, 0);
            }
            else
            {
                sqlStmt.select(sqlStmt.getPrimaryTable(), valueMapping, null);
            }
        }

        // Apply condition on owner field to filter by owner
        SQLExpressionFactory exprFactory = storeMgr.getSQLExpressionFactory();
        SQLTable ownerSqlTbl = SQLStatementHelper.getSQLTableForMappingOfTable(sqlStmt, sqlStmt.getPrimaryTable(), ownerMapping);
        SQLExpression ownerExpr = exprFactory.newExpression(sqlStmt, ownerSqlTbl, ownerMapping);
        SQLExpression ownerVal = exprFactory.newLiteralParameter(sqlStmt, ownerMapping, null, "OWNER");
        sqlStmt.whereAnd(ownerExpr.eq(ownerVal), true);

        // Apply condition on key
        if (keyMapping instanceof SerialisedMapping)
        {
            // if the keyMapping contains a BLOB column (or any other column not supported by the database
            // as primary key), uses like instead of the operator OP_EQ (=)
            // in future do not check if the keyMapping is of ObjectMapping, but use the database 
            // adapter to check the data types not supported as primary key
            // if object mapping (BLOB) use like
            SQLExpression keyExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), keyMapping);
            SQLExpression keyVal = exprFactory.newLiteralParameter(sqlStmt, keyMapping, null, "KEY");
            sqlStmt.whereAnd(new org.datanucleus.store.rdbms.sql.expression.BooleanExpression(keyExpr, Expression.OP_LIKE, keyVal), true);
        }
        else
        {
            SQLExpression keyExpr = exprFactory.newExpression(sqlStmt, sqlStmt.getPrimaryTable(), keyMapping);
            SQLExpression keyVal = exprFactory.newLiteralParameter(sqlStmt, keyMapping, null, "KEY");
            sqlStmt.whereAnd(keyExpr.eq(keyVal), true);
        }

        // Input parameter(s) - owner, key
        int inputParamNum = 1;
        StatementMappingIndex ownerIdx = new StatementMappingIndex(ownerMapping);
        StatementMappingIndex keyIdx = new StatementMappingIndex(keyMapping);
        if (sqlStmt.getNumberOfUnions() > 0)
        {
            // Add parameter occurrence for each union of statement
            for (int j=0;j<sqlStmt.getNumberOfUnions()+1;j++)
            {
                int[] ownerPositions = new int[ownerMapping.getNumberOfDatastoreMappings()];
                for (int k=0;k<ownerPositions.length;k++)
                {
                    ownerPositions[k] = inputParamNum++;
                }
                ownerIdx.addParameterOccurrence(ownerPositions);

                int[] keyPositions = new int[keyMapping.getNumberOfDatastoreMappings()];
                for (int k=0;k<keyPositions.length;k++)
                {
                    keyPositions[k] = inputParamNum++;
                }
                keyIdx.addParameterOccurrence(keyPositions);
            }
        }
        else
        {
            int[] ownerPositions = new int[ownerMapping.getNumberOfDatastoreMappings()];
            for (int k=0;k<ownerPositions.length;k++)
            {
                ownerPositions[k] = inputParamNum++;
            }
            ownerIdx.addParameterOccurrence(ownerPositions);

            int[] keyPositions = new int[keyMapping.getNumberOfDatastoreMappings()];
            for (int k=0;k<keyPositions.length;k++)
            {
                keyPositions[k] = inputParamNum++;
            }
            keyIdx.addParameterOccurrence(keyPositions);
        }
        getMappingParams = new StatementParameterMapping();
        getMappingParams.addMappingForParameter("owner", ownerIdx);
        getMappingParams.addMappingForParameter("key", keyIdx);

        return sqlStmt;
    }
}