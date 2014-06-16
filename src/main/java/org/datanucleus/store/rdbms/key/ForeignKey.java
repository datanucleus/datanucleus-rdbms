/**********************************************************************
Copyright (c) 2003 Mike Martin (TJDO) and others. All rights reserved.
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
2003 Andy Jefferson - equality operator
2004 Andy Jefferson - deleteAction, updateAction, MetaData interface
2008 Andy Jefferson - rewritten to use enums for actions
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.key;

import java.util.ArrayList;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.ForeignKeyAction;
import org.datanucleus.metadata.ForeignKeyMetaData;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.DatastoreClass;

/**
 * Representation of a foreign key to another table.
 */
public class ForeignKey extends Key
{
    /** Enum representing an action on the FK. */
    public static enum FKAction
    {
        CASCADE("CASCADE"),
        RESTRICT("RESTRICT"),
        NULL("SET NULL"),
        DEFAULT("SET DEFAULT");

        String keyword;
        private FKAction(String word)
        {
            this.keyword = word;
        }
        public String toString()
        {
            return keyword;
        }
    }

    private DatastoreAdapter dba;

    private boolean initiallyDeferred;
    private DatastoreClass refTable;
    private FKAction updateAction;
    private FKAction deleteAction;
    private ArrayList refColumns = new ArrayList();

    private String foreignKeyDefinition = null;

    /**
     * Constructor.
     * @param initiallyDeferred Whether the constraints are deferred
     */
    public ForeignKey(boolean initiallyDeferred)
    {
        super(null);

        this.initiallyDeferred = initiallyDeferred;
        this.refTable = null;
        this.dba = null;
    }

    /**
     * Constructor.
     * @param mapping The type mapping for this Foreign-key field
     * @param dba Datastore adapter
     * @param refTable Referred to table
     * @param initiallyDeferred Whether they are deferred
     */
    public ForeignKey(JavaTypeMapping mapping, DatastoreAdapter dba, DatastoreClass refTable, boolean initiallyDeferred)
    {
        super(mapping.getTable());
        this.initiallyDeferred = initiallyDeferred;
        this.refTable = refTable;
        this.dba = dba;

        if (refTable.getIdMapping() == null)
        {
            throw new NucleusException("ForeignKey ID mapping is not initilized for "+mapping+". Table referenced: " + refTable.toString()).setFatal();
        }

        for (int i=0; i<refTable.getIdMapping().getNumberOfDatastoreMappings(); i++)
        {
        	setColumn(i, mapping.getDatastoreMapping(i).getColumn(), refTable.getIdMapping().getDatastoreMapping(i).getColumn()); 
        }
    }

    /**
     * Convenience mutator for setting the specification based on MetaData
     * @param fkmd ForeignKey MetaData definition
     */
    public void setForMetaData(ForeignKeyMetaData fkmd)
    {
        if (fkmd == null)
        {
            return;
        }

        if (fkmd.getFkDefinitionApplies() && fkmd.getFkDefinition() != null)
        {
            foreignKeyDefinition = fkmd.getFkDefinition();
            name = fkmd.getName();
            refColumns = null;
            updateAction = null;
            deleteAction = null;
            refTable = null;
            refColumns = null;
        }
        else
        {
            if (fkmd.getName() != null)
            {
                setName(fkmd.getName());
            }

            ForeignKeyAction deleteAction = fkmd.getDeleteAction();
            if (deleteAction != null)
            {
                if (deleteAction.equals(ForeignKeyAction.CASCADE))
                {
                    setDeleteAction(FKAction.CASCADE);
                }
                else if (deleteAction.equals(ForeignKeyAction.RESTRICT))
                {
                    setDeleteAction(FKAction.RESTRICT);
                }
                else if (deleteAction.equals(ForeignKeyAction.NULL))
                {
                    setDeleteAction(FKAction.NULL);
                }
                else if (deleteAction.equals(ForeignKeyAction.DEFAULT))
                {
                    setDeleteAction(FKAction.DEFAULT);
                }
            }

            ForeignKeyAction updateAction = fkmd.getUpdateAction();
            if (updateAction != null)
            {
                if (updateAction.equals(ForeignKeyAction.CASCADE))
                {
                    setUpdateAction(FKAction.CASCADE);
                }
                else if (updateAction.equals(ForeignKeyAction.RESTRICT))
                {
                    setUpdateAction(FKAction.RESTRICT);
                }
                else if (updateAction.equals(ForeignKeyAction.NULL))
                {
                    setUpdateAction(FKAction.NULL);
                }
                else if (updateAction.equals(ForeignKeyAction.DEFAULT))
                {
                    setUpdateAction(FKAction.DEFAULT);
                }
            }

            if (fkmd.isDeferred())
            {
                initiallyDeferred = true;
            }
        }
    }

    /**
     * Mutator for deleteAction.
     * @param deleteAction The deleteAction to set.
     */
    public void setDeleteAction(FKAction deleteAction)
    {
        this.deleteAction = deleteAction;
    }

    /**
     * Mutator for updateAction.
     * @param updateAction The updateAction to set.
     */
    public void setUpdateAction(FKAction updateAction)
    {
        this.updateAction = updateAction;
    }

    /**
     * Method to add a Column.
     * @param col The column to add
     * @param refCol The column to reference 
     **/
    public void addColumn(Column col, Column refCol)
    {
        setColumn(columns.size(), col, refCol);
    }

    /**
     * Set the datastore field for the specified position <code>seq</code>
     * @param seq the specified position
     * @param col the datastore field
     * @param refCol the foreign (refered) datastore field
     */
    public void setColumn(int seq, Column col, Column refCol)
    {
        if (table == null)
        {
            table = col.getTable();
            refTable = (DatastoreClass) refCol.getTable();
            dba = table.getStoreManager().getDatastoreAdapter();
        }
        else
        {
            if (!table.equals(col.getTable()))
            {
                throw new NucleusException("Cannot add " + col + " as FK column for " + table).setFatal();
            }
            if (!refTable.equals(refCol.getTable()))
            {
                throw new NucleusException("Cannot add " + refCol + " as referenced FK column for " + refTable).setFatal();
            }
        }

        setMinSize(columns, seq + 1);
        setMinSize(refColumns, seq + 1);

        columns.set(seq, col);
        refColumns.set(seq, refCol);
    }

    public int hashCode()
    {
        if (foreignKeyDefinition != null)
        {
            return foreignKeyDefinition.hashCode();
        }
        return super.hashCode() ^ refColumns.hashCode();
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof ForeignKey))
        {
            return false;
        }

        ForeignKey fk = (ForeignKey)obj;
        if (refColumns != null && !refColumns.equals(fk.refColumns))
        {
            return false;
        }

        // TODO Enable this. Sadly some RDBMS (e.g HSQLDB) don't always return all info so we end up
        // For example "test.jdo.orm.application" RelationshipTest where we have
        // this  = FOREIGN KEY (BOILER_ID) REFERENCES HEATING_BOILER (BOILER_ID) ON DELETE SET NULL
        // other = FOREIGN KEY (BOILER_ID) REFERENCES HEATING_BOILER (BOILER_ID)
        // These should be equal, but the latter hasn't had all info returned
/*        if (!toString().equals(fk.toString()))
        {
            return false;
        }*/

        return super.equals(obj);
    }

    /**
     * Stringify method. Generates the foreign key statement ready for use in an SQL call.
     * @return String version of this object.
     **/
    public String toString()
    {
        if (foreignKeyDefinition != null)
        {
            // User-provided definition
            return foreignKeyDefinition;
        }

        StringBuilder s = new StringBuilder("FOREIGN KEY ");
        s.append(getColumnList(columns));

        // Referenced table
        if (refTable != null)
        {
            // Include the referenced column list because some RDBMS require it (e.g MySQL)
            s.append(" REFERENCES ");
            s.append(refTable.toString());
            s.append(" ").append(getColumnList(refColumns));
        }

        // Delete action
        if (deleteAction != null)
        {
            if ((deleteAction == FKAction.CASCADE && dba.supportsOption(DatastoreAdapter.FK_DELETE_ACTION_CASCADE)) ||
                    (deleteAction == FKAction.RESTRICT && dba.supportsOption(DatastoreAdapter.FK_DELETE_ACTION_RESTRICT)) ||
                    (deleteAction == FKAction.NULL && dba.supportsOption(DatastoreAdapter.FK_DELETE_ACTION_NULL)) ||
                    (deleteAction == FKAction.DEFAULT && dba.supportsOption(DatastoreAdapter.FK_DELETE_ACTION_DEFAULT)))
            {
                s.append(" ON DELETE ").append(deleteAction.toString());
            }
        }

        // Update action
        if (updateAction != null)
        {
            if ((updateAction == FKAction.CASCADE && dba.supportsOption(DatastoreAdapter.FK_UPDATE_ACTION_CASCADE)) ||
                    (updateAction == FKAction.RESTRICT && dba.supportsOption(DatastoreAdapter.FK_UPDATE_ACTION_RESTRICT)) ||
                    (updateAction == FKAction.NULL && dba.supportsOption(DatastoreAdapter.FK_UPDATE_ACTION_NULL)) ||
                    (updateAction == FKAction.DEFAULT && dba.supportsOption(DatastoreAdapter.FK_UPDATE_ACTION_DEFAULT)))
            {
                s.append(" ON UPDATE ").append(updateAction.toString());
            }
        }

        // Deferral of constraints
        if (initiallyDeferred && dba.supportsOption(DatastoreAdapter.DEFERRED_CONSTRAINTS))
        {
            s.append(" INITIALLY DEFERRED");
        }

        s.append(" ");

        return s.toString();
    }
}