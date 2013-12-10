/**********************************************************************
Copyright (c) 2006 Andy Jefferson and others. All rights reserved.
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
***********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.store.rdbms.table.ColumnCreator;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.NucleusLogger;

/**
 * Mapping for a field that stores a PC object that uses "subclass-table" inheritance
 * and where this is mapped in the datastore as a separate FK for each subclass.
 */
public class SubclassPCMapping extends MultiPersistableMapping
{
    /**
     * Initialize this JavaTypeMapping with the given DatastoreAdapter for
     * the given FieldMetaData.
     * @param table The datastore container storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     * @param fmd FieldMetaData for the field to be mapped (if any)
     */
    public void initialize(AbstractMemberMetaData fmd, Table table, ClassLoaderResolver clr)
    {
		super.initialize(fmd, table, clr);

		prepareDatastoreMapping(clr);
    }

    /**
     * Convenience method to create a column for each implementation type of this reference.
     * @param clr The ClassLoaderResolver
     */
    protected void prepareDatastoreMapping(ClassLoaderResolver clr)
    {
        if (roleForMember == FieldRole.ROLE_ARRAY_ELEMENT)
        {
            // TODO Implement column creation for array[PC] with subclass-table
        }
        else if (roleForMember == FieldRole.ROLE_COLLECTION_ELEMENT)
        {
            // TODO Implement column creation for collection<PC> with subclass-table
        }
        else if (roleForMember == FieldRole.ROLE_MAP_KEY)
        {
            // TODO Implement column creation for map<PC, ..> with subclass-table
        }
        else if (roleForMember == FieldRole.ROLE_MAP_VALUE)
        {
            // TODO Implement column creation for map<.., PC> with subclass-table
        }
        else
        {
            // Create columns for each possible implementation type of the reference field
            AbstractClassMetaData refCmd = 
                storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            if (refCmd.getInheritanceMetaData().getStrategy() != InheritanceStrategy.SUBCLASS_TABLE)
            {
                throw new NucleusUserException(LOCALISER.msg("020185", mmd.getFullFieldName()));
            }
            AbstractClassMetaData[] subclassCmds = storeMgr.getClassesManagingTableForClass(refCmd, clr);

            boolean pk = false;
            if (subclassCmds.length > 1)
            {
                pk = false;
            }
            boolean nullable = true;
            if (subclassCmds.length > 1)
            {
                nullable = true;
            }

            int colPos = 0;
            for (int i=0; i<subclassCmds.length; i++)
            {
                Class type = clr.classForName(subclassCmds[i].getFullClassName());
                DatastoreClass dc = storeMgr.getDatastoreClass(subclassCmds[i].getFullClassName(), clr);
                JavaTypeMapping m = dc.getIdMapping();

                // obtain the column names for this type
                // TODO Fix this. The <column> elements have no way of being ordered to match the subclasses
                ColumnMetaData[] columnMetaDataForType = null;
                if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
                {
                    if (mmd.getColumnMetaData().length < colPos+m.getNumberOfDatastoreMappings())
                    {
                        throw new NucleusUserException(LOCALISER.msg("020186", 
                            mmd.getFullFieldName(), "" + mmd.getColumnMetaData().length, 
                            "" + (colPos + m.getNumberOfDatastoreMappings())));
                    }
                    columnMetaDataForType = new ColumnMetaData[m.getNumberOfDatastoreMappings()];
                    System.arraycopy(mmd.getColumnMetaData(), colPos, columnMetaDataForType, 0, columnMetaDataForType.length);
                    colPos += columnMetaDataForType.length;
                }

                // Create the FK columns for this subclass
                // TODO Remove the link to an RDBMS class
                ColumnCreator.createColumnsForField(type, this, table, storeMgr, mmd, pk, nullable, 
                    false, false, FieldRole.ROLE_FIELD, columnMetaDataForType, clr, true);

                if (NucleusLogger.DATASTORE.isInfoEnabled())
                {
                    NucleusLogger.DATASTORE.info(LOCALISER.msg("020187", type, mmd.getName()));
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#getJavaType()
     */
    public Class getJavaType()
    {
        return null;
    }
}