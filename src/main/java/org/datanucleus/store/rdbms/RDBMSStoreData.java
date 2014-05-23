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
        addProperty("table", tableName);
        addProperty("table-owner", tableOwner ? "true" : "false");
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

        addProperty("table", (table != null ? table.toString() : null));
        addProperty("table-owner", tableOwner ? "true" : "false");
        if (table != null)
        {
            addProperty("tableObject", table);
            addProperty("tableId", table.getIdentifier());
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
            addProperty("interface-name", interfaces);
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
        addProperty("table", table.toString());
        addProperty("table-owner", "true");
        addProperty("tableObject", table);
        addProperty("tableId", table.getIdentifier());

        String interfaceName = 
            (table.getStoreManager().getMetaDataManager().isPersistentInterface(mmd.getType().getName()) ? mmd.getType().getName() : null);
        if (interfaceName != null)
        {
            addProperty("interface-name", interfaceName);
        }
    }

    /**
     * Utility to return whether this table is a view.
     * @return Whether it is for a view.
     */
    public boolean mapsToView()
    {
        Table table = getTable();
        if (table == null)
        {
            return false;
        }
        return (table instanceof ViewImpl);
    }

    /**
     * Accessor for tableName.
     * @return Returns the tableName.
     */
    public String getTableName()
    {
        return (String)properties.get("table");
    }

    /**
     * Accessor for whether this class is the owner of the table.
     * @return Whether it owns the table
     */
    public boolean isTableOwner()
    {
        return ((String)properties.get("table-owner")).equals("true");
    }

    /**
     * Accessor for whether this has a table representation.
     * @return Whether it has a table
     */
    public boolean hasTable()
    {
        return properties.get("table") != null;
    }

    /**
     * Accessor for the Table details.
     * @return The Table
     */
    public Table getTable()
    {
        return (Table)properties.get("tableObject");
    }

    /**
     * Accessor for the identifier for the table.
     * @return The table identifier
     */
    public DatastoreIdentifier getDatastoreIdentifier()
    {
        return (DatastoreIdentifier)properties.get("tableId");
    }

    /**
     * Convenience to set the table. To be used in cases where the table isn't known
     * until after the initial create
     * @param table The table
     */
    public void setDatastoreContainerObject(DatastoreClass table)
    {
        if (table != null)
        {
            addProperty("table", table.toString());
            addProperty("tableObject", table);
            addProperty("tableId", table.getIdentifier());
        }
    }

    /**
     * Method to return this class/field managed object as a string.
     * @return String version of this class/field managed object.
     */
    public String toString()
    {
        String tableName = (String)properties.get("table");
        MetaData metadata = getMetaData();
        if (metadata instanceof ClassMetaData)
        {
            ClassMetaData cmd = (ClassMetaData)metadata;
            return Localiser.msg("035004", name, tableName != null ? tableName : "(none)",
                cmd.getInheritanceMetaData().getStrategy().toString());
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