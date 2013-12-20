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
package org.datanucleus.store.rdbms.table;

import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ElementMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.PrimaryKeyMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.java.PersistableMapping;
import org.datanucleus.store.rdbms.mapping.java.ReferenceMapping;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;

/**
 * Representation of a join table for a Collection. A Collection covers a wide range of possibilities
 * in terms of whether it allows duplicates or not, whether it allows nulls or not, whether it supports
 * ordering via indexes, whether it supports ordering via a SELECT criteria, etc. Consequently the
 * join table can vary depending on the required capabilities.
 * <h3>JoinTable Mappings</h3>
 * <p>
 * The join table consists of the following mappings :-
 * <ul>
 * <li><B>ownerMapping</B> linking back to the owning class with the Collection.</li>
 * <li><B>elementMapping</B> either being an FK link to the element table or being an 
 * embedded/serialised element stored wholly in this table.</li>
 * <li><B>orderMapping</B> which may be null, or otherwise stores an index for the elements.
 * This is either to provide uniqueness or ordering in a List (and part of the PK).</li>
 * </ul>
 * Note that with an M-N relation there will be 2 instances of the CollectionTable - one represents the relation
 * from owner to element, and the other for the relation from element to owner.
 * </p>
 */
public class CollectionTable extends ElementContainerTable implements DatastoreElementContainer
{
    /**
     * Constructor.
     * @param tableName Identifier name of the table
     * @param mmd MetaData for the field of the owner
     * @param storeMgr The Store Manager managing these tables.
     */
    public CollectionTable(DatastoreIdentifier tableName, AbstractMemberMetaData mmd, RDBMSStoreManager storeMgr)
    {
        super(tableName, mmd, storeMgr);
    }

    /**
     * Method to initialise the table definition.
     * @param clr The ClassLoaderResolver
     */
    public void initialize(ClassLoaderResolver clr)
    {
        super.initialize(clr);

        PrimaryKeyMetaData pkmd = (mmd.getJoinMetaData() != null ? mmd.getJoinMetaData().getPrimaryKeyMetaData() : null);
        boolean pkColsSpecified = (pkmd != null ? pkmd.getColumnMetaData() != null : false);
        boolean pkRequired = requiresPrimaryKey();

        // Add column(s) for element
        boolean elementPC = (mmd.hasCollection() && mmd.getCollection().elementIsPersistent());
        Class elementClass = clr.classForName(getElementType());
        if (isSerialisedElement() || isEmbeddedElementPC() || (isEmbeddedElement() && !elementPC) ||
            ClassUtils.isReferenceType(elementClass))
        {
            // Element = PC(embedded), PC(serialised), Non-PC(serialised), Non-PC(embedded), Reference
            // Join table has : ownerMapping (PK), elementMapping, orderMapping (PK)
            elementMapping = storeMgr.getMappingManager().getMapping(this, mmd, clr, FieldRole.ROLE_COLLECTION_ELEMENT);
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                debugMapping(elementMapping);
            }
        }
        else
        {
            // Element = PC
            // Join table has : ownerMapping (PK), elementMapping, orderMapping (optional)
            ColumnMetaData[] elemColmd = null;
            AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
            ElementMetaData elemmd = mmd.getElementMetaData();
            if (elemmd != null && elemmd.getColumnMetaData() != null && elemmd.getColumnMetaData().length > 0)
            {
                // Column mappings defined at this side (1-N, M-N)
                elemColmd = elemmd.getColumnMetaData();
            }
            else if (relatedMmds != null && relatedMmds[0].getJoinMetaData() != null && 
                relatedMmds[0].getJoinMetaData().getColumnMetaData() != null &&
                relatedMmds[0].getJoinMetaData().getColumnMetaData().length > 0)
            {
                // Column mappings defined at other side (M-N) on <join>
                elemColmd = relatedMmds[0].getJoinMetaData().getColumnMetaData();
            }

            elementMapping = ColumnCreator.createColumnsForJoinTables(elementClass, mmd, 
                elemColmd, storeMgr, this, false, false, FieldRole.ROLE_COLLECTION_ELEMENT, clr);

            RelationType relationType = mmd.getRelationType(clr);
            if (Boolean.TRUE.equals(mmd.getContainer().allowNulls()) && relationType != RelationType.MANY_TO_MANY_BI)
            {
                // 1-N : Make all element col(s) nullable so we can store null elements
                for (int i=0;i<elementMapping.getNumberOfDatastoreMappings();i++)
                {
                    Column elementCol = elementMapping.getDatastoreMapping(i).getColumn();
                    elementCol.setNullable();
                }
            }
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                debugMapping(elementMapping);
            }
        }

        // Add order mapping if required
        boolean orderRequired = false;
        if (mmd.getOrderMetaData() != null)
        {
            if (mmd.getOrderMetaData().isIndexedList())
            {
                // Indexed Collection with <order>, so add index mapping
                orderRequired = true;
            }
        }
        else if (List.class.isAssignableFrom(mmd.getType()))
        {
            // Indexed List with no <order>, so has index mapping
            orderRequired = true;
        }
        else if (requiresPrimaryKey() && !pkColsSpecified)
        {
            // PK is required so maybe need to add an index to form the PK
            if (isEmbeddedElementPC())
            {
                if (mmd.getCollection().getElementClassMetaData(clr, 
                    storeMgr.getMetaDataManager()).getIdentityType() != IdentityType.APPLICATION)
                {
                    // Embedded PC with datastore id so we need an index to form the PK
                    orderRequired = true;
                }
            }
            else if (isSerialisedElement())
            {
                // Serialised element, so need an index to form the PK
                orderRequired = true;
            }
            else if (elementMapping instanceof ReferenceMapping)
            {
                // ReferenceMapping, so have order if more than 1 implementation
                ReferenceMapping refMapping = (ReferenceMapping)elementMapping;
                if (refMapping.getJavaTypeMapping().length > 1)
                {
                    orderRequired = true;
                }
            }
            else if (!(elementMapping instanceof PersistableMapping))
            {
                // Non-PC, so depends if the element column can be used as part of a PK
                // TODO This assumes the elementMapping has a single column but what if it is Color with 4 cols?
                Column elementCol = elementMapping.getDatastoreMapping(0).getColumn();
                if (!storeMgr.getDatastoreAdapter().isValidPrimaryKeyType(elementCol.getJdbcType()))
                {
                    // Not possible to use this Non-PC type as part of the PK
                    orderRequired = true;
                }
            }
        }
        if (orderRequired)
        {
            // Order (index) column is required (integer based)
            ColumnMetaData orderColmd = null;
            if (mmd.getOrderMetaData() != null &&
                mmd.getOrderMetaData().getColumnMetaData() != null &&
                mmd.getOrderMetaData().getColumnMetaData().length > 0)
            {
                // Specified "order" column info
                orderColmd = mmd.getOrderMetaData().getColumnMetaData()[0];
                if (orderColmd.getName() == null)
                {
                    orderColmd = new ColumnMetaData(orderColmd);
                    if (mmd.hasExtension("adapter-column-name"))
                    {
                        // Specified "extension" column name
                        // TODO Is this needed? The user can just specify <order column="...">
                        orderColmd.setName(mmd.getValueForExtension("adapter-column-name"));
                    }
                    else
                    {
                        // No column name so use default
                        DatastoreIdentifier id = storeMgr.getIdentifierFactory().newIndexFieldIdentifier(mmd);
                        orderColmd.setName(id.getIdentifierName());
                    }
                }
            }
            else
            {
                if (mmd.hasExtension("adapter-column-name"))
                {
                    // Specified "extension" column name
                    // TODO Is this needed? The user can just specify <order column="...">
                    orderColmd = new ColumnMetaData();
                    orderColmd.setName(mmd.getValueForExtension("adapter-column-name"));
                }
                else
                {
                    // No column name so use default
                    DatastoreIdentifier id = storeMgr.getIdentifierFactory().newIndexFieldIdentifier(mmd);
                    orderColmd = new ColumnMetaData();
                    orderColmd.setName(id.getIdentifierName());
                }
            }
            orderMapping = storeMgr.getMappingManager().getMapping(int.class); // JDO2 spec [18.5] order column is assumed to be "int"
            ColumnCreator.createIndexColumn(orderMapping, storeMgr, clr, this, orderColmd, pkRequired && !pkColsSpecified);
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                debugMapping(orderMapping);
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
                // Define PK
                if (orderRequired)
                {
                    // Order column specified so owner+order are the PK
                    orderMapping.getDatastoreMapping(0).getColumn().setAsPrimaryKey();
                }
                else
                {
                    // No order column specified so owner+element are the PK
                    for (int i=0;i<elementMapping.getNumberOfDatastoreMappings();i++)
                    {
                        elementMapping.getDatastoreMapping(i).getColumn().setAsPrimaryKey();
                    }
                }
            }
        }

        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("057023", this));
        }
        storeMgr.registerTableInitialized(this);
        state = TABLE_STATE_INITIALIZED;
    }

    /**
     * Accessor for the element type stored in this Collection/Set/List.
     * @return Name of element type.
     */
    public String getElementType()
    {
        return mmd.getCollection().getElementType();
    }

    /**
     * Accessor for whether the element is serialised into this table.
     * This can be a serialised PersistenceCapable, or a serialised simple type
     * @return Whether the element is serialised.
     */
    public boolean isSerialisedElement()
    {
        if (mmd.getCollection() != null && mmd.getCollection().isSerializedElement())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the element is embedded into this table.
     * This can be an embedded PersistenceCapable, or an embedded simple type
     * @return Whether the element is embedded.
     */
    public boolean isEmbeddedElement()
    {
        if (mmd.getCollection() != null && mmd.getCollection().isSerializedElement())
        {
            // Serialised takes precedence
            return false;
        }
        else if (mmd.getCollection() != null && mmd.getCollection().isEmbeddedElement())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the element is a PersistenceCapable(serialised)
     * @return Whether the element is PC and is serialised
     */
    public boolean isSerialisedElementPC()
    {
        if (mmd.getCollection() != null && mmd.getCollection().isSerializedElement() &&
            mmd.getCollection().elementIsPersistent())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the element is a PersistenceCapable(embedded).
     * Currently this only defines a PC element as embedded if the MetaData has an <embedded> block.
     * This may or may not be correct depending on how you interpret the JDO2 spec "embedded-element"
     * flag.
     * @return Whether the element is PC and is embedded
     */
    public boolean isEmbeddedElementPC()
    {
        if (mmd.getCollection() != null && mmd.getCollection().isSerializedElement())
        {
            // Serialisation takes precedence over embedding
            return false;
        }
        if (mmd.getElementMetaData() != null && mmd.getElementMetaData().getEmbeddedMetaData() != null)
        {
            return true;
        }
        return false;
    }

    /**
     * Convenience method for whether a PK is required for the join table.
     * Extends JoinTable allowing for "ordered List" case which do not require a primary key (so we can have duplicates).
     * @return Whether a PK is required
     */
    protected boolean requiresPrimaryKey()
    {
        if (mmd.getOrderMetaData() != null && !mmd.getOrderMetaData().isIndexedList())
        {
            // "Ordered Collection/List" so no PK applied, meaning that we can have duplicate elements in the List
            return false;
        }
        return super.requiresPrimaryKey();
    }
}