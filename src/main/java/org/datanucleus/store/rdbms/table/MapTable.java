/**********************************************************************
Copyright (c) 2002 Kelly Grizzle and others. All rights reserved.
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
2002 Mike Martin - unknown changes
2003 Andy Jefferson - added localiser
2003 Andy Jefferson - replaced TableMetadata with identifier
2004 Marco Schulze - added advance-check via TypeManager.isSupportedType(...)
2005 Andy Jefferson - only create ADPT column when necessary
2005 Andy Jefferson - enabled ability to have embedded keys/values
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.ForeignKeyMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.metadata.KeyMetaData;
import org.datanucleus.metadata.MapMetaData;
import org.datanucleus.metadata.PrimaryKeyMetaData;
import org.datanucleus.metadata.UniqueMetaData;
import org.datanucleus.metadata.ValueMetaData;
import org.datanucleus.store.rdbms.exceptions.NoTableManagedException;
import org.datanucleus.store.rdbms.RDBMSPropertyNames;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedKeyPCMapping;
import org.datanucleus.store.rdbms.mapping.java.EmbeddedValuePCMapping;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Representation of a join table for a Map. A Map covers a wide range of possibilities
 * in terms of whether it allows duplicates or not, whether it allows nulls or not, whether it supports
 * ordering via indexes etc. Consequently the join table can vary depending on the required capabilities.
 * <h3>JoinTable Mappings</h3>
 * <p>
 * The join table consists of the following mappings :-
 * <ul>
 * <li><B>ownerMapping</B> linking back to the owning class with the Collection.</li>
 * <li><B>keyMapping</B> either being an FK link to the key table or being an embedded/serialised key 
 * stored wholely in this table.</li>
 * <li><B>valueMapping</B> either being an FK link to the value table or being an embedded/serialised value 
 * stored wholely in this table.</li>
 * <li><B>orderMapping</B> which may be null, or otherwise stores an index for the keys.
 * This is either to provide uniqueness or ordering (and part of the PK).</li>
 * </ul>
 */
public class MapTable extends JoinTable
{
    protected Table ownerTable;

    /** Mapping to the key object. */
    private JavaTypeMapping keyMapping;

    /** Mapping to the value object. */
    private JavaTypeMapping valueMapping;

    /**
     * Mapping to allow ordering (of keys) or to allow duplicates. Can be used when the key is not suitable
     * for use as part of the PK and a PK is required for this join table.
     */
    private JavaTypeMapping orderMapping;

    /**
     * Constructor.
     * @param ownerTable Table of the owner of this member
     * @param tableName Identifier name of the table
     * @param mmd MetaData for the member of the owner
     * @param storeMgr The Store Manager managing these tables.
     */
    public MapTable(Table ownerTable, DatastoreIdentifier tableName, AbstractMemberMetaData mmd, RDBMSStoreManager storeMgr)
    {
        super(ownerTable, tableName, mmd, storeMgr);
    }

    /**
     * Method to initialise the table definition.
     * @param clr The ClassLoaderResolver
     */
    public void initialize(ClassLoaderResolver clr)
    {
        assertIsUninitialized();

        MapMetaData mapmd = mmd.getMap();
        if (mapmd == null)
        {
            throw new NucleusUserException(Localiser.msg("057017",mmd));
        }

        PrimaryKeyMetaData pkmd = (mmd.getJoinMetaData() != null ? mmd.getJoinMetaData().getPrimaryKeyMetaData() : null);
        boolean pkColsSpecified = (pkmd != null && pkmd.getColumnMetaData() != null);
        boolean pkRequired = requiresPrimaryKey();

        // Add owner mapping
        ColumnMetaData[] ownerColmd = null;
        if (mmd.getJoinMetaData() != null && mmd.getJoinMetaData().getColumnMetaData() != null && mmd.getJoinMetaData().getColumnMetaData().length > 0)
        {
            // Column mappings defined at this side (1-N, M-N)
            // When specified at this side they use the <join> tag
            ownerColmd = mmd.getJoinMetaData().getColumnMetaData();
        }

        ownerMapping = ColumnCreator.createColumnsForJoinTables(clr.classForName(ownerType), mmd, ownerColmd, storeMgr, this, pkRequired, false, FieldRole.ROLE_OWNER, clr, null);
        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            logMapping(mmd.getFullFieldName()+".[OWNER]", ownerMapping);
        }

        String keyValueFieldName = (mmd.getKeyMetaData() != null ? mmd.getKeyMetaData().getMappedBy() : null);
        String valueKeyFieldName = (mmd.getValueMetaData() != null ? mmd.getValueMetaData().getMappedBy() : null);

        // Add key mapping
        boolean keyPC = (mmd.hasMap() && mmd.getMap().keyIsPersistent());
        Class keyCls = clr.classForName(mapmd.getKeyType());
        if (keyValueFieldName != null && isEmbeddedValuePC())
        {
            // Added in value code
        }
        else if (isSerialisedKey() || isEmbeddedKeyPC() || (isEmbeddedKey() && !keyPC) || ClassUtils.isReferenceType(keyCls))
        {
            // Key = PC(embedded), PC(serialised), Non-PC(serialised), Non-PC(embedded), Reference
            keyMapping = storeMgr.getMappingManager().getMapping(this, mmd, clr, FieldRole.ROLE_MAP_KEY);
            if (Boolean.TRUE.equals(mmd.getContainer().allowNulls()))
            {
                // Make all key col(s) nullable so we can store null elements
                for (int i=0;i<keyMapping.getNumberOfColumnMappings();i++)
                {
                    Column elementCol = keyMapping.getColumnMapping(i).getColumn();
                    elementCol.setNullable(true);
                }
            }
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                logMapping(mmd.getFullFieldName()+".[KEY]", keyMapping);
            }
            if (valueKeyFieldName != null && isEmbeddedKeyPC())
            {
                // Key (PC) is embedded and value is a field of the key
                EmbeddedKeyPCMapping embMapping = (EmbeddedKeyPCMapping)keyMapping;
                valueMapping = embMapping.getJavaTypeMapping(valueKeyFieldName);
            }
        }
        else
        {
            // Key = PC
            ColumnMetaData[] keyColmd = null;
            KeyMetaData keymd = mmd.getKeyMetaData();
            if (keymd != null && keymd.getColumnMetaData() != null && keymd.getColumnMetaData().length > 0)
            {
                // Column mappings defined at this side (1-N, M-N)
                keyColmd = keymd.getColumnMetaData();
            }

            keyMapping = ColumnCreator.createColumnsForJoinTables(keyCls, mmd, keyColmd, storeMgr, this, false, false, FieldRole.ROLE_MAP_KEY, clr, null);
            if (mmd.getContainer().allowNulls() == Boolean.TRUE)
            {
                // Make all key col(s) nullable so we can store null elements
                for (int i=0;i<keyMapping.getNumberOfColumnMappings();i++)
                {
                    Column elementCol = keyMapping.getColumnMapping(i).getColumn();
                    elementCol.setNullable(true);
                }
            }
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                logMapping(mmd.getFullFieldName()+".[KEY]", keyMapping);
            }
        }

        // Add value mapping
        boolean valuePC = (mmd.hasMap() && mmd.getMap().valueIsPersistent());
        Class valueCls = clr.classForName(mapmd.getValueType());
        if (valueKeyFieldName != null && isEmbeddedKeyPC())
        {
            // Added in key code
        }
        else if (isSerialisedValue() || isEmbeddedValuePC() || (isEmbeddedValue() && !valuePC) || ClassUtils.isReferenceType(valueCls))
        {
            // Value = PC(embedded), PC(serialised), Non-PC(serialised), Non-PC(embedded), Reference
            valueMapping = storeMgr.getMappingManager().getMapping(this, mmd, clr, FieldRole.ROLE_MAP_VALUE);
            if (mmd.getContainer().allowNulls() == Boolean.TRUE)
            {
                // Make all value col(s) nullable so we can store null elements
                for (int i=0;i<valueMapping.getNumberOfColumnMappings();i++)
                {
                    Column elementCol = valueMapping.getColumnMapping(i).getColumn();
                    elementCol.setNullable(true);
                }
            }
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                logMapping(mmd.getFullFieldName()+".[VALUE]", valueMapping);
            }
            if (keyValueFieldName != null && isEmbeddedValuePC())
            {
                // Value (PC) is embedded and key is a field of the value
                EmbeddedValuePCMapping embMapping = (EmbeddedValuePCMapping)valueMapping;
                keyMapping = embMapping.getJavaTypeMapping(keyValueFieldName);
            }
        }
        else
        {
            // Value = PC
            ColumnMetaData[] valueColmd = null;
            ValueMetaData valuemd = mmd.getValueMetaData();
            if (valuemd != null && valuemd.getColumnMetaData() != null && valuemd.getColumnMetaData().length > 0)
            {
                // Column mappings defined at this side (1-N, M-N)
                valueColmd = valuemd.getColumnMetaData();
            }
            valueMapping = ColumnCreator.createColumnsForJoinTables(clr.classForName(mapmd.getValueType()), mmd, valueColmd, storeMgr, this, false, true, FieldRole.ROLE_MAP_VALUE, clr, null);
            if (mmd.getContainer().allowNulls() == Boolean.TRUE)
            {
                // Make all value col(s) nullable so we can store null elements
                for (int i=0;i<valueMapping.getNumberOfColumnMappings();i++)
                {
                    Column elementCol = valueMapping.getColumnMapping(i).getColumn();
                    elementCol.setNullable(true);
                }
            }
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                logMapping(mmd.getFullFieldName()+".[VALUE]", valueMapping);
            }
        }

        // Add order mapping if required
        boolean orderRequired = false;
        if (mmd.getOrderMetaData() != null)
        {
            // User requested order column so add one
            orderRequired = true;
        }
        else if (requiresPrimaryKey() && !pkColsSpecified)
        {
            // PK is required so maybe need to add an index to form the PK
            if (isEmbeddedKeyPC())
            {
                if (mmd.hasExtension("surrogate-pk-column") && mmd.getValueForExtension("surrogate-pk-column").equalsIgnoreCase("true"))
                {
                    // Allow user to request surrogate pk column be added (for use with JPA)
                    orderRequired = true;
                }
                else if (storeMgr.getApiAdapter().getName().equalsIgnoreCase("JDO") &&
                    mmd.getMap().getKeyClassMetaData(clr).getIdentityType() != IdentityType.APPLICATION)
                {
                    // Embedded key PC with datastore id so we need an index to form the PK TODO It is arguable that we can just use all embedded key fields as part of PK here always
                    orderRequired = true;
                }
            }
            else if (isSerialisedKey())
            {
                // Serialised key, so need an index to form the PK
                orderRequired = true;
            }
            else if (keyMapping instanceof ReferenceMapping)
            {
                // ReferenceMapping, so have order if more than 1 implementation
                ReferenceMapping refMapping = (ReferenceMapping)keyMapping;
                if (refMapping.getJavaTypeMapping().length > 1)
                {
                    orderRequired = true;
                }
            }
            else if (!(keyMapping instanceof PersistableMapping))
            {
                // Non-PC, so depends if the key column can be used as part of a PK
                // TODO This assumes the keyMapping has a single column but what if it is Color with 4 cols?
                Column elementCol = keyMapping.getColumnMapping(0).getColumn();
                if (!storeMgr.getDatastoreAdapter().isValidPrimaryKeyType(elementCol.getJdbcType()))
                {
                    // Not possible to use this Non-PC type as part of the PK
                    orderRequired = true;
                }
            }
        }
        if (orderRequired)
        {
            // Order/Adapter (index) column is required (integer based)
            ColumnMetaData orderColmd = null;
            if (mmd.getOrderMetaData() != null && mmd.getOrderMetaData().getColumnMetaData() != null && mmd.getOrderMetaData().getColumnMetaData().length > 0)
            {
                // Specified "order" column info
                orderColmd = mmd.getOrderMetaData().getColumnMetaData()[0];
                if (orderColmd.getName() == null)
                {
                    // No column name so use default
                    orderColmd = new ColumnMetaData(orderColmd);
                    DatastoreIdentifier id = storeMgr.getIdentifierFactory().newIndexFieldIdentifier(mmd);
                    orderColmd.setName(id.getName());
                }
            }
            else
            {
                // No column name so use default
                DatastoreIdentifier id = storeMgr.getIdentifierFactory().newIndexFieldIdentifier(mmd);
                orderColmd = new ColumnMetaData();
                orderColmd.setName(id.getName());
            }
            orderMapping = storeMgr.getMappingManager().getMapping(int.class); // JDO2 spec [18.5] order column is assumed to be "int"
            ColumnCreator.createIndexColumn(orderMapping, storeMgr, clr, this, orderColmd, pkRequired && !pkColsSpecified);
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                logMapping(mmd.getFullFieldName()+".[ORDER]", orderMapping);
            }
        }

        // Define primary key of the join table (if any)
        if (pkRequired)
        {
            if (pkColsSpecified)
            {
                // Apply the users PK specification
                applyUserPrimaryKeySpecification(pkmd);
            }
            else
            {
                // Define PK using internal rules
                if (orderRequired)
                {
                    // Order column specified so owner+order are the PK
                    orderMapping.getColumnMapping(0).getColumn().setPrimaryKey();
                }
                else
                {
                    // No order column specified so owner+key are the PK
                    for (int i=0;i<keyMapping.getNumberOfColumnMappings();i++)
                    {
                        keyMapping.getColumnMapping(i).getColumn().setPrimaryKey();
                    }
                }
            }
        }

        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("057023", this));
        }
        storeMgr.registerTableInitialized(this);
        state = TABLE_STATE_INITIALIZED;
    }

    /**
     * Convenience method to apply the user specification of &lt;primary-key&gt; columns
     * @param pkmd MetaData for the primary key
     */
    protected void applyUserPrimaryKeySpecification(PrimaryKeyMetaData pkmd)
    {
        ColumnMetaData[] pkCols = pkmd.getColumnMetaData();
        for (int i=0;i<pkCols.length;i++)
        {
            String colName = storeMgr.getIdentifierFactory().getIdentifierInAdapterCase(pkCols[i].getName());
            boolean found = false;
            for (int j=0;j<ownerMapping.getNumberOfColumnMappings();j++)
            {
                if (ownerMapping.getColumnMapping(j).getColumn().getIdentifier().getName().equals(colName))
                {
                    ownerMapping.getColumnMapping(j).getColumn().setPrimaryKey();
                    found = true;
                }
            }
            
            if (!found)
            {
                for (int j=0;j<keyMapping.getNumberOfColumnMappings();j++)
                {
                    if (keyMapping.getColumnMapping(j).getColumn().getIdentifier().getName().equals(colName))
                    {
                        keyMapping.getColumnMapping(j).getColumn().setPrimaryKey();
                        found = true;
                    }
                }
            }
            
            if (!found)
            {
                for (int j=0;j<valueMapping.getNumberOfColumnMappings();j++)
                {
                    if (valueMapping.getColumnMapping(j).getColumn().getIdentifier().getName().equals(colName))
                    {
                        valueMapping.getColumnMapping(j).getColumn().setPrimaryKey();
                        found = true;
                    }
                }
            }
            
            if (!found)
            {
                throw new NucleusUserException(Localiser.msg("057040", toString(), colName));
            }
        }
    }

    /**
     * Accessor for whether the key is embedded into this table.
     * This can be an embedded persistable, or an embedded simple type.
     * @return Whether the key is embedded.
     */
    public boolean isEmbeddedKey()
    {
        if (mmd.getMap() != null && mmd.getMap().isSerializedKey())
        {
            // Serialized takes precedence over embedded
            return false;
        }
        else if (mmd.getMap() != null && mmd.getMap().isEmbeddedKey())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the key is serialised into this table.
     * This can be an serialised persistable, or a serialised simple type.
     * @return Whether the key is serialised.
     */
    public boolean isSerialisedKey()
    {
        if (mmd.getMap() != null && mmd.getMap().isSerializedKey())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the key is a persistable(serialised)
     * @return Whether the key is PC and is serialised
     */
    public boolean isSerialisedKeyPC()
    {
        if (mmd.getMap() != null && mmd.getMap().isSerializedKey() && mmd.getMap().keyIsPersistent())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the key is a persistable(embedded)
     * @return Whether the key is PC and is embedded
     */
    public boolean isEmbeddedKeyPC()
    {
        if (mmd.getMap() != null && mmd.getMap().isSerializedKey())
        {
            // Serialized takes precedence over embedded
            return false;
        }
        /*if (mmd.getMap().keyIsPersistent() && mmd.getMap().isEmbeddedKey())
        {
            // Persistable key, and marked as embedded key
            return true;
        }*/
        if (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getEmbeddedMetaData() != null)
        {
            // Embedded metadata provided for key
            return true;
        }
        if (mmd.getMap() != null)
        {
            AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(storeMgr.getNucleusContext().getClassLoaderResolver(null));
            if (keyCmd != null && keyCmd.isEmbeddedOnly())
            {
                // Key is persistable and is embedded only, so it is embedded PC
                return true;
            }
        }
        return false;
    }

    /**
     * Accessor for whether the value is embedded into this table.
     * This can be an embedded persistable, or an embedded simple type.
     * @return Whether the value is embedded.
     */
    public boolean isEmbeddedValue()
    {
        if (mmd.getMap() != null && mmd.getMap().isSerializedValue())
        {
            // Serialized takes precedence over embedded
            return false;
        }
        else if (mmd.getMap() != null && mmd.getMap().isEmbeddedValue())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the value is serialised into this table.
     * This can be an serialised persistable, or a serialised simple type.
     * @return Whether the value is serialised.
     */
    public boolean isSerialisedValue()
    {
        if (mmd.getMap() != null && mmd.getMap().isSerializedValue())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the value is a persistable(serialised)
     * @return Whether the value is PC and is serialised
     */
    public boolean isSerialisedValuePC()
    {
        if (mmd.getMap() != null && mmd.getMap().isSerializedValue() && mmd.getMap().valueIsPersistent())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the value is a persistable(embedded)
     * @return Whether the value is PC and is embedded
     */
    public boolean isEmbeddedValuePC()
    {
        if (mmd.getMap() != null && mmd.getMap().isSerializedValue())
        {
            // Serialized takes precedence over embedded
            return false;
        }
        /*if (mmd.getMap().valueIsPersistent() && mmd.getMap().isEmbeddedValue())
        {
            // Persistable value, and marked as embedded value
            return true;
        }*/
        if (mmd.getValueMetaData() != null && mmd.getValueMetaData().getEmbeddedMetaData() != null)
        {
            // Embedded metadata provided for value
            return true;
        }
        if (mmd.getMap() != null)
        {
            AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(storeMgr.getNucleusContext().getClassLoaderResolver(null));
            if (valCmd != null && valCmd.isEmbeddedOnly())
            {
                // Value is persistable and is embedded only, so it is embedded PC
                return true;
            }
        }
        return false;
    }

    /**
     * Accessor for the "key" mapping end of the relationship.
     * @return The column mapping for the element.
     **/
    public JavaTypeMapping getKeyMapping()
    {
        assertIsInitialized();

        return keyMapping;
    }

    /**
     * Accessor for the "value" mapping end of the relationship.
     * @return The column mapping for the element.
     **/
    public JavaTypeMapping getValueMapping()
    {
        assertIsInitialized();

        return valueMapping;
    }

    /**
     * Accessor for the key type for this Map.
     * @return Name of key type.
     */
    public String getKeyType()
    {
        return mmd.getMap().getKeyType();
    }

    /**
     * Accessor for the value type for this Map.
     * @return Name of value type.
     */
    public String getValueType()
    {
        return mmd.getMap().getValueType();
    }

    /**
     * Accessor for order mapping.
     * The columns in this mapping are part of the primary key.
     * @return The column mapping for objects without identity or not supported as part of the primary keys
     **/
    public JavaTypeMapping getOrderMapping()
    {
        assertIsInitialized();
        return orderMapping;
    }

    /**
     * Accessor for the expected foreign keys for this table.
     * @param clr The ClassLoaderResolver
     * @return The expected foreign keys.
     */
    public List getExpectedForeignKeys(ClassLoaderResolver clr)
    {
        assertIsInitialized();

        boolean autoMode = false;
        if (storeMgr.getStringProperty(RDBMSPropertyNames.PROPERTY_RDBMS_CONSTRAINT_CREATE_MODE).equals("DataNucleus"))
        {
            autoMode = true;
        }

        ArrayList foreignKeys = new ArrayList();
        try
        {
            // FK from join table to owner table
            DatastoreClass referencedTable = storeMgr.getDatastoreClass(ownerType, clr);
            if (referencedTable != null)
            {
                // Take <foreign-key> from <join>
                ForeignKeyMetaData fkmd = null;
                if (mmd.getJoinMetaData() != null)
                {
                    fkmd = mmd.getJoinMetaData().getForeignKeyMetaData();
                }
                if (fkmd != null || autoMode)
                {
                    ForeignKey fk = new ForeignKey(ownerMapping,dba,referencedTable, true);
                    fk.setForMetaData(fkmd);
                    foreignKeys.add(fk);
                }
            }

            if (!isSerialisedValuePC())
            {
                if (isEmbeddedValuePC())
                {
                    // Add any FKs for the fields of the (embedded) value
                    EmbeddedValuePCMapping embMapping = (EmbeddedValuePCMapping)valueMapping;
                    for (int i=0;i<embMapping.getNumberOfJavaTypeMappings();i++)
                    {
                        JavaTypeMapping embFieldMapping = embMapping.getJavaTypeMapping(i);
                        AbstractMemberMetaData embFmd = embFieldMapping.getMemberMetaData();
                        if (ClassUtils.isReferenceType(embFmd.getType()) &&
                            embFieldMapping instanceof ReferenceMapping)
                        {
                            // Field is a reference type, so add a FK to the table of the PC for each PC implementation
                            Collection fks = TableUtils.getForeignKeysForReferenceField(embFieldMapping, embFmd, 
                                autoMode, storeMgr, clr);
                            foreignKeys.addAll(fks);
                        }
                        else if (storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(embFmd.getType(), clr) != null &&
                                embFieldMapping.getNumberOfColumnMappings() > 0 &&
                                embFieldMapping instanceof PersistableMapping)
                        {
                            // Field is for a PC class with the FK at this side, so add a FK to the table of this PC
                            ForeignKey fk = TableUtils.getForeignKeyForPCField(embFieldMapping, embFmd, 
                                autoMode, storeMgr, clr);
                            if (fk != null)
                            {
                                foreignKeys.add(fk);
                            }
                        }
                    }
                }
                else if (mmd.getMap().valueIsPersistent())
                {
                    // FK from join table to value table
                    referencedTable = storeMgr.getDatastoreClass(mmd.getMap().getValueType(), clr);
                    if (referencedTable != null)
                    {
                        // Take <foreign-key> from <value>
                        ForeignKeyMetaData fkmd = null;
                        if (mmd.getValueMetaData() != null)
                        {
                            fkmd = mmd.getValueMetaData().getForeignKeyMetaData();
                        }
                        if (fkmd != null || autoMode)
                        {
                            ForeignKey fk = new ForeignKey(valueMapping, dba, referencedTable, true);
                            fk.setForMetaData(fkmd);
                            foreignKeys.add(fk);
                        }
                    }
                }
            }

            if (!isSerialisedKeyPC())
            {
                if (isEmbeddedKeyPC())
                {
                    // Add any FKs for the fields of the (embedded) key
                    EmbeddedKeyPCMapping embMapping = (EmbeddedKeyPCMapping)keyMapping;
                    for (int i=0;i<embMapping.getNumberOfJavaTypeMappings();i++)
                    {
                        JavaTypeMapping embFieldMapping = embMapping.getJavaTypeMapping(i);
                        AbstractMemberMetaData embFmd = embFieldMapping.getMemberMetaData();
                        if (ClassUtils.isReferenceType(embFmd.getType()) && embFieldMapping instanceof ReferenceMapping)
                        {
                            // Field is a reference type, so add a FK to the table of the PC for each PC implementation
                            Collection fks = TableUtils.getForeignKeysForReferenceField(embFieldMapping, embFmd, autoMode, storeMgr, clr);
                            foreignKeys.addAll(fks);
                        }
                        else if (storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(embFmd.getType(), clr) != null &&
                                embFieldMapping.getNumberOfColumnMappings() > 0 && embFieldMapping instanceof PersistableMapping)
                        {
                            // Field is for a PC class with the FK at this side, so add a FK to the table of this PC
                            ForeignKey fk = TableUtils.getForeignKeyForPCField(embFieldMapping, embFmd, autoMode, storeMgr, clr);
                            if (fk != null)
                            {
                                foreignKeys.add(fk);
                            }
                        }
                    }
                }
                else if (mmd.getMap().keyIsPersistent())
                {
                    // FK from join table to key table
                    referencedTable = storeMgr.getDatastoreClass(mmd.getMap().getKeyType(), clr);
                    if (referencedTable != null)
                    {
                        // Take <foreign-key> from <key>
                        ForeignKeyMetaData fkmd = null;
                        if (mmd.getKeyMetaData() != null)
                        {
                            fkmd = mmd.getKeyMetaData().getForeignKeyMetaData();
                        }
                        if (fkmd != null || autoMode)
                        {
                            ForeignKey fk = new ForeignKey(keyMapping, dba, referencedTable, true);
                            fk.setForMetaData(fkmd);
                            foreignKeys.add(fk);
                        }
                    }
                }
            }
        }
        catch (NoTableManagedException e)
        {
            // expected when no table exists
        }
        return foreignKeys;
    }

    /**
     * Accessor for the indices for this table. 
     * This includes both the user-defined indices (via MetaData), and the ones required by foreign keys.
     * @param clr The ClassLoaderResolver
     * @return The indices
     */
    protected Set getExpectedIndices(ClassLoaderResolver clr)
    {
        Set indices = new HashSet();

        // Index for FK back to owner
        if (mmd.getIndexMetaData() != null)
        {
            Index index = TableUtils.getIndexForField(this, mmd.getIndexMetaData(), ownerMapping);
            if (index != null)
            {
                indices.add(index);
            }
        }
        else if (mmd.getJoinMetaData() != null && mmd.getJoinMetaData().getIndexMetaData() != null)
        {
            Index index = TableUtils.getIndexForField(this, mmd.getJoinMetaData().getIndexMetaData(), ownerMapping);
            if (index != null)
            {
                indices.add(index);
            }
        }
        else
        {
            // Fallback to an index for the foreign-key to the owner
            Index index = TableUtils.getIndexForField(this, null, ownerMapping);
            if (index != null)
            {
                indices.add(index);
            }
        }

        // Index for the key FK (if required)
        if (keyMapping instanceof EmbeddedKeyPCMapping)
        {
            // Add all indices required by fields of the embedded key
            EmbeddedKeyPCMapping embMapping = (EmbeddedKeyPCMapping)keyMapping;
            for (int i=0;i<embMapping.getNumberOfJavaTypeMappings();i++)
            {
                JavaTypeMapping embFieldMapping = embMapping.getJavaTypeMapping(i);
                IndexMetaData imd = embFieldMapping.getMemberMetaData().getIndexMetaData();
                if (imd != null)
                {
                    Index index = TableUtils.getIndexForField(this, imd, embFieldMapping);
                    if (index != null)
                    {
                        indices.add(index);
                    }
                }
            }
        }
        else
        {
            KeyMetaData keymd = mmd.getKeyMetaData();
            if (keymd != null && keymd.getIndexMetaData() != null)
            {
                IndexMetaData idxmd = mmd.getKeyMetaData().getIndexMetaData();
                Index index = TableUtils.getIndexForField(this, idxmd, keyMapping);
                if (index != null)
                {
                    indices.add(index);
                }
            }
            else
            {
                // Fallback to an index for any foreign-key to the key
                if (keyMapping instanceof PersistableMapping)
                {
                    Index index = TableUtils.getIndexForField(this, null, keyMapping);
                    if (index != null)
                    {
                        indices.add(index);
                    }
                }
            }
        }

        // Index for the value FK (if required)
        if (valueMapping instanceof EmbeddedValuePCMapping)
        {
            // Add all indices required by fields of the embedded value
            EmbeddedValuePCMapping embMapping = (EmbeddedValuePCMapping)valueMapping;
            for (int i=0;i<embMapping.getNumberOfJavaTypeMappings();i++)
            {
                JavaTypeMapping embFieldMapping = embMapping.getJavaTypeMapping(i);
                IndexMetaData imd = embFieldMapping.getMemberMetaData().getIndexMetaData();
                if (imd != null)
                {
                    Index index = TableUtils.getIndexForField(this, imd, embFieldMapping);
                    if (index != null)
                    {
                        indices.add(index);
                    }
                }
            }
        }
        else
        {
            ValueMetaData valmd = mmd.getValueMetaData();
            if (valmd != null && valmd.getIndexMetaData() != null)
            {
                IndexMetaData idxmd = mmd.getValueMetaData().getIndexMetaData();
                Index index = TableUtils.getIndexForField(this, idxmd, valueMapping);
                if (index != null)
                {
                    indices.add(index);
                }
            }
            else
            {
                // Fallback to an index for any foreign-key to the value
                if (valueMapping instanceof PersistableMapping)
                {
                    Index index = TableUtils.getIndexForField(this, null, valueMapping);
                    if (index != null)
                    {
                        indices.add(index);
                    }
                }
            }
        }

        return indices;
    }

    /**
     * Accessor for the candidate keys for this table.
     * @return The indices
     */
    protected List getExpectedCandidateKeys()
    {
        // The indices required by foreign keys (BaseTable)
        List candidateKeys = super.getExpectedCandidateKeys();

        if (keyMapping instanceof EmbeddedKeyPCMapping)
        {
            // Add all candidate keys required by fields of the embedded key
            EmbeddedKeyPCMapping embMapping = (EmbeddedKeyPCMapping)keyMapping;
            for (int i=0;i<embMapping.getNumberOfJavaTypeMappings();i++)
            {
                JavaTypeMapping embFieldMapping = embMapping.getJavaTypeMapping(i);
                UniqueMetaData umd = embFieldMapping.getMemberMetaData().getUniqueMetaData();
                if (umd != null)
                {
                    CandidateKey ck = TableUtils.getCandidateKeyForField(this, umd, embFieldMapping);
                    if (ck != null)
                    {
                        candidateKeys.add(ck);
                    }
                }
            }
        }
        else if (mmd.getKeyMetaData() != null)
        {
            UniqueMetaData unimd = mmd.getKeyMetaData().getUniqueMetaData();
            if (unimd != null)
            {
                CandidateKey ck = TableUtils.getCandidateKeyForField(this, unimd, keyMapping);
                if (ck != null)
                {
                    candidateKeys.add(ck);
                }
            }
        }

        if (valueMapping instanceof EmbeddedValuePCMapping)
        {
            // Add all candidate keys required by fields of the embedded value
            EmbeddedValuePCMapping embMapping = (EmbeddedValuePCMapping)valueMapping;
            for (int i=0;i<embMapping.getNumberOfJavaTypeMappings();i++)
            {
                JavaTypeMapping embFieldMapping = embMapping.getJavaTypeMapping(i);
                UniqueMetaData umd = embFieldMapping.getMemberMetaData().getUniqueMetaData();
                if (umd != null)
                {
                    CandidateKey ck = TableUtils.getCandidateKeyForField(this, umd, embFieldMapping);
                    if (ck != null)
                    {
                        candidateKeys.add(ck);
                    }
                }
            }
        }
        else if (mmd.getValueMetaData() != null)
        {
            UniqueMetaData unimd = mmd.getValueMetaData().getUniqueMetaData();
            if (unimd != null)
            {
                CandidateKey ck = TableUtils.getCandidateKeyForField(this, unimd, valueMapping);
                if (ck != null)
                {
                    candidateKeys.add(ck);
                }
            }
        }

        return candidateKeys;
    }

    /**
     * Accessor the for the mapping for a field stored in this table
     * @param mmd MetaData for the field whose mapping we want
     * @return The mapping
     */
    public JavaTypeMapping getMemberMapping(AbstractMemberMetaData mmd)
    {
        return null;
    }
}