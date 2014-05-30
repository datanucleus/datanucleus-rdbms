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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ElementMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.PrimaryKeyMetaData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Representation of a join table for an array. An array requires ordering of elements so that they can be stored
 * and retrieved in the same order.
 * <h3>JoinTable Mappings</h3>
 * <p>
 * The join table consists of the following mappings :-
 * <ul>
 * <li><B>ownerMapping</B> linking back to the owning class with the Collection.</li>
 * <li><B>elementMapping</B> either being an FK link to the element table or being an 
 * embedded/serialised element stored wholely in this table.</li>
 * <li><B>orderMapping</B> providing the ordering of the elements.</li>
 * </ul>
 * The primary-key is formed from the ownerMapping and the orderMapping (unless overridden by the user).
 */
public class ArrayTable extends ElementContainerTable implements DatastoreElementContainer
{
    /**
     * Constructor.
     * @param tableName Identifier name of the table
     * @param mmd MetaData for the field of the owner
     * @param storeMgr The Store Manager managing these tables.
     */
    public ArrayTable(DatastoreIdentifier tableName, AbstractMemberMetaData mmd, RDBMSStoreManager storeMgr)
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

        // Add field(s) for element
        boolean elementPC = (mmd.hasArray() && mmd.getArray().elementIsPersistent());
        if (isSerialisedElementPC() || isEmbeddedElementPC() || (isEmbeddedElement() && !elementPC) ||
            ClassUtils.isReferenceType(mmd.getType().getComponentType()))
        {
            // Element = PC(embedded), PC(serialised), Non-PC(embedded), Reference
            elementMapping = storeMgr.getMappingManager().getMapping(this, mmd, clr, FieldRole.ROLE_ARRAY_ELEMENT);
            if (Boolean.TRUE.equals(mmd.getContainer().allowNulls()))
            {
                // Make all element col(s) nullable so we can store null elements
                for (int i=0;i<elementMapping.getNumberOfDatastoreMappings();i++)
                {
                    Column elementCol = elementMapping.getDatastoreMapping(i).getColumn();
                    elementCol.setNullable(true);
                }
            }
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                debugMapping(elementMapping);
            }
        }
        else
        {
            // Element = PC
            ColumnMetaData[] elemColmd = null;
            ElementMetaData elemmd = mmd.getElementMetaData();
            if (elemmd != null && elemmd.getColumnMetaData() != null && elemmd.getColumnMetaData().length > 0)
            {
                // Column mappings defined at this side (1-N, M-N)
                elemColmd = elemmd.getColumnMetaData();
            }
            elementMapping = ColumnCreator.createColumnsForJoinTables(mmd.getType().getComponentType(), mmd,
                elemColmd, storeMgr, this, false, true, FieldRole.ROLE_ARRAY_ELEMENT, clr);
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                debugMapping(elementMapping);
            }
        }

        // Add order mapping
        ColumnMetaData colmd = null;
        if (mmd.getOrderMetaData() != null && mmd.getOrderMetaData().getColumnMetaData() != null &&
            mmd.getOrderMetaData().getColumnMetaData().length > 0)
        {
            // Specified "order" column info
            colmd = mmd.getOrderMetaData().getColumnMetaData()[0];
        }
        else
        {
            // No column name so use default
            DatastoreIdentifier id = storeMgr.getIdentifierFactory().newIndexFieldIdentifier(mmd);
            colmd = new ColumnMetaData();
            colmd.setName(id.getIdentifierName());
        }
        orderMapping = storeMgr.getMappingManager().getMapping(int.class); // JDO2 spec [18.5] order column is assumed to be "int"
        ColumnCreator.createIndexColumn(orderMapping, storeMgr, clr, this, colmd, pkRequired && !pkColsSpecified);
        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            debugMapping(orderMapping);
        }

        // Define primary key of the join table (if any)
        if (pkRequired && pkColsSpecified)
        {
            // Apply the users PK specification
            applyUserPrimaryKeySpecification(pkmd);
        }

        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("057023", this));
        }
        storeMgr.registerTableInitialized(this);
        state = TABLE_STATE_INITIALIZED;
    }

    /**
     * Accessor for the element type stored in this array.
     * @return Name of element type.
     */
    public String getElementType()
    {
        return mmd.getType().getComponentType().getName();
    }

    /**
     * Accessor for whether the element is serialised into this table.
     * This can be a serialised persistable, or a serialised simple type
     * @return Whether the element is serialised.
     */
    public boolean isSerialisedElement()
    {
        if (mmd.getArray() != null && mmd.getArray().isSerializedElement())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the element is embedded into this table.
     * This can be an embedded persistable, or an embedded simple type
     * @return Whether the element is embedded.
     */
    public boolean isEmbeddedElement()
    {
        if (mmd.getArray() != null && mmd.getArray().isSerializedElement())
        {
            // Serialised takes precedence
            return false;
        }
        else if (mmd.getArray() != null && mmd.getArray().isEmbeddedElement())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the element is a persistable(serialised)
     * @return Whether the element is PC and is serialised
     */
    public boolean isSerialisedElementPC()
    {
        if (mmd.getArray() != null && mmd.getArray().isSerializedElement())
        {
            return true;
        }
        return false;
    }

    /**
     * Accessor for whether the element is a persistable(embedded)
     * @return Whether the element is PC and is embedded
     */
    public boolean isEmbeddedElementPC()
    {
        if (mmd.getArray() != null && mmd.getArray().isSerializedElement())
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
}