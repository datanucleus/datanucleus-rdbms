/**********************************************************************
Copyright (c) 2004 Andy Jefferson and others. All rights reserved. 
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
package org.datanucleus.store.rdbms;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ImplementsMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.table.ViewImpl;
import org.datanucleus.util.Localiser;

/**
 * Representation of a class (FCO) / field (SCO) that is persisted to an RDBMS table.
 * Extends the basic data to allow determination of whether it is a table or a view being represented.
 */
public class RDBMSStoreData extends StoreData
{
    protected String tableName = null;

    protected DatastoreIdentifier tableIdentifier = null;

    protected boolean tableOwner = true;

    // TODO Remove this when org.datanucleus.store.rdbms.table.Table implements org.datanucleus.store.schema.table.Table
    protected Table rdbmsTable = null;

    /**
     * Constructor. To be used when creating for the start mechanism.
     * @param name Name of the class/field
     * @param tableName Name of the table associated
     * @param tableOwner Whether this is the owner
     * @param type The type (FCO/SCO)
     * @param interfaceName if this class is an implementation of a persistent interface (multiple persistent interface names 
     *    are comma separated), otherwise is null.
     */
    public RDBMSStoreData(String name, String tableName, boolean tableOwner, int type, String interfaceName)
    {
        super(name, null, type, interfaceName);
        this.tableName = tableName;
        this.tableOwner = tableOwner;
    }

    /**
     * Constructor for FCO data.
     * @param cmd MetaData for the class.
     * @param table Table where the class is stored.
     * @param tableOwner Whether the class is the owner of the table.
     */
    public RDBMSStoreData(ClassMetaData cmd, Table table, boolean tableOwner)
    {
        super(cmd.getFullClassName(), cmd, FCO_TYPE, null);

        this.tableOwner = tableOwner;
        if (table != null)
        {
            this.rdbmsTable = table;
            this.tableName = table.toString();
            this.tableIdentifier = table.getIdentifier();
        }

        String interfaces = null;
        ImplementsMetaData[] implMds = cmd.getImplementsMetaData();
        if (implMds != null)
        {
            for (int i=0; i<cmd.getImplementsMetaData().length; i++)
            {
                if (interfaces == null)
                {
                    interfaces = "";
                }
                else
                {
                    interfaces += ",";
                }
                interfaces += cmd.getImplementsMetaData()[i].getName();
            }
            this.interfaceName = interfaces;
        }
    }

    /**
     * Constructor, taking the meta data for the field, and the table it is mapped to.
     * @param mmd MetaData for the field.
     * @param table Table definition
     */
    public RDBMSStoreData(AbstractMemberMetaData mmd, Table table)
    {
        super(mmd.getFullFieldName(), mmd, SCO_TYPE, null);

        if (table == null)
        {
            throw new NullPointerException("table should not be null");
        }
        this.rdbmsTable = table;
        this.tableName = table.toString();
        this.tableOwner = true;
        this.tableIdentifier = table.getIdentifier();

        String interfaceName = 
            (table.getStoreManager().getMetaDataManager().isPersistentInterface(mmd.getType().getName()) ? mmd.getType().getName() : null);
        if (interfaceName != null)
        {
            this.interfaceName = interfaceName;
        }
    }

    /**
     * Utility to return whether this table is a view.
     * @return Whether it is for a view.
     */
    public boolean mapsToView()
    {
        return rdbmsTable != null ? rdbmsTable instanceof ViewImpl : false;
    }

    /**
     * Accessor for tableName.
     * @return Returns the tableName.
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * Accessor for whether this class is the owner of the table.
     * @return Whether it owns the table
     */
    public boolean isTableOwner()
    {
        return tableOwner;
    }

    /**
     * Accessor for whether this has a table representation.
     * @return Whether it has a table
     */
    public boolean hasTable()
    {
        return tableName != null;
    }

    /**
     * Accessor for the Table details.
     * @return The Table
     */
    public Table getRDBMSTable()
    {
        return rdbmsTable;
    }

    /**
     * Accessor for the identifier for the table.
     * @return The table identifier
     */
    public DatastoreIdentifier getDatastoreIdentifier()
    {
        return tableIdentifier;
    }

    /**
     * Convenience to set the table. To be used in cases where the table isn't known until after the initial create
     * @param table The table
     */
    public void setDatastoreContainerObject(DatastoreClass table)
    {
        if (table != null)
        {
            this.rdbmsTable = table;
            this.tableName = table.toString();
            this.tableIdentifier = table.getIdentifier();
        }
    }

    /**
     * Method to return this class/field managed object as a string.
     * @return String version of this class/field managed object.
     */
    public String toString()
    {
        MetaData metadata = getMetaData();
        if (metadata instanceof ClassMetaData)
        {
            ClassMetaData cmd = (ClassMetaData)metadata;
            return Localiser.msg("035004", name, tableName != null ? tableName : "(none)", cmd.getInheritanceMetaData().getStrategy().toString());
        }
        else if (metadata instanceof AbstractMemberMetaData)
        {
            return Localiser.msg("035005", name, tableName);
        }
        else
        {
            return Localiser.msg("035004", name, tableName);
        }
    }
}