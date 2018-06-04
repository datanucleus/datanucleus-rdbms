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
2005 Andy Jefferson - added capability to have String or long type discriminators
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.ClassNameConstants;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.mapping.MappingManager;
import org.datanucleus.store.rdbms.mapping.datastore.ColumnMapping;
import org.datanucleus.store.rdbms.table.Column;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.rdbms.RDBMSStoreManager;

/**
 * Mapping for a discriminator column in a table used in inheritance.
 * The discriminator column is, by default, a String type, typically VARCHAR.
 * It can however be "long" based if the user specifies INTEGER, BIGINT, or NUMERIC as the jdbc-type. 
 * In the latter case we make the necessary conversions between value types in this mapping class.
 * 
 * This class is for internal use only. It should not be used in user mappings nor extended.
 */
public class DiscriminatorMapping extends SingleFieldMapping
{
    private final JavaTypeMapping delegate;

    /**
     * Constructor.
     * @param table Datastore table
     * @param delegate The JavaTypeMapping to delegate storage
     * @param dismd Metadata for the discriminator
     */
    public DiscriminatorMapping(Table table, JavaTypeMapping delegate, DiscriminatorMetaData dismd)
    {
        initialize(table.getStoreManager(), delegate.getType());
        this.table = table;
        this.delegate = delegate;

        IdentifierFactory idFactory = table.getStoreManager().getIdentifierFactory();
        DatastoreIdentifier id = null;
        if (dismd.getColumnMetaData() == null)
        {
            // No column name so generate a default
            id = idFactory.newDiscriminatorFieldIdentifier();
            ColumnMetaData colmd = new ColumnMetaData();
            colmd.setName(id.getName());
            dismd.setColumnMetaData(colmd);
        }
        else
        {
            // Column metadata defined
            ColumnMetaData colmd = dismd.getColumnMetaData();
            if (colmd.getName() == null)
            {
                // No name defined so create one and set it
                id = idFactory.newDiscriminatorFieldIdentifier();
                colmd.setName(id.getName());
            }
            else
            {
                // Name defined so just generate identifier
                id = idFactory.newColumnIdentifier(colmd.getName());
            }
        }

        Column column = table.addColumn(getType(), id, this, dismd.getColumnMetaData());
        table.getStoreManager().getMappingManager().createColumnMapping(delegate, column, getType());
    }

    /**
     * Accessor for the type represented here, returning the class itself
     * @return This class.
     */
    public Class getJavaType()
    {
        return DiscriminatorMapping.class;
    }

    /**
     * Mutator for the object in this column
     * @param ec ExecutionContext
     * @param ps The statement
     * @param exprIndex The indexes
     * @param value The value to set it to
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
        Object valueObj = value;
        if (value instanceof java.lang.String)
        {
            if (getType().equals(ClassNameConstants.LONG) || getType().equals(ClassNameConstants.JAVA_LANG_LONG))
            {
                valueObj = Long.valueOf((String)value);
            }
        }
        delegate.setObject(ec, ps, exprIndex, valueObj);
    }

    /**
     * Accessor for the object in this column
     * @param ec ExecutionContext
     * @param resultSet The ResultSet to get the value from
     * @param exprIndex The indexes
     * @return The object
     */
    public Object getObject(ExecutionContext ec, ResultSet resultSet, int[] exprIndex)
    {
        Object value = delegate.getObject(ec, resultSet, exprIndex);
        Object valueObj = value;
        if (value instanceof java.lang.String)
        {
            if (getType().equals(ClassNameConstants.LONG) || getType().equals(ClassNameConstants.JAVA_LANG_LONG))
            {
                valueObj = Long.valueOf((String)value);
            }
        }
        return valueObj;
    }

    /**
     * Accessor for the number of datastore fields.
     * @return Number of datastore fields
     */
    public int getNumberOfColumnMappings()
    {
        return delegate.getNumberOfColumnMappings();
    }

    /**
     * Accessor for a datastore mapping
     * @param index Index of the mapping
     * @return The datastore mapping.
     */
    public ColumnMapping getColumnMapping(int index)
    {
        return delegate.getColumnMapping(index);
    }

    /**
     * Accessor for the datastore mappings for this java type.
     * @return The datastore mapping(s)
     */
    public ColumnMapping[] getColumnMappings()
    {
        return delegate.getColumnMappings();
    }

    /**
     * Mutator to add a datastore mapping
     * @param datastoreMapping Datastore mapping
     */
    public void addColumnMapping(ColumnMapping datastoreMapping)
    {
        delegate.addColumnMapping(datastoreMapping);
    }

    /**
     * Convenience method to create a discriminator mapping in the specified table, using the provided
     * discriminator metadata.
     * @param table The table
     * @param dismd The discriminator metadata
     * @return Discriminator mapping
     */
    public static DiscriminatorMapping createDiscriminatorMapping(Table table, DiscriminatorMetaData dismd)
    {
        RDBMSStoreManager storeMgr = table.getStoreManager();
        MappingManager mapMgr = storeMgr.getMappingManager();

        if (dismd.getStrategy() == DiscriminatorStrategy.CLASS_NAME)
        {
            return new DiscriminatorStringMapping(table, mapMgr.getMapping(String.class), dismd);
        }
        else if (dismd.getStrategy() == DiscriminatorStrategy.VALUE_MAP)
        {
            ColumnMetaData disColmd = dismd.getColumnMetaData();
            if (disColmd != null && disColmd.getJdbcType() != null)
            {
                if (MetaDataUtils.isJdbcTypeNumeric(disColmd.getJdbcType()))
                {
                    return new DiscriminatorLongMapping(table, mapMgr.getMapping(Long.class), dismd);
                }
                return new DiscriminatorStringMapping(table, mapMgr.getMapping(String.class), dismd);
            }
            return new DiscriminatorStringMapping(table, mapMgr.getMapping(String.class), dismd);
        }
        else if (dismd.getStrategy() == DiscriminatorStrategy.VALUE_MAP_ENTITY_NAME)
        {
            ColumnMetaData disColmd = dismd.getColumnMetaData();
            if (disColmd != null && disColmd.getJdbcType() != null)
            {
                if (MetaDataUtils.isJdbcTypeNumeric(disColmd.getJdbcType()))
                {
                    return new DiscriminatorLongMapping(table, mapMgr.getMapping(Long.class), dismd);
                }
                return new DiscriminatorStringMapping(table, mapMgr.getMapping(String.class), dismd);
            }
            return new DiscriminatorStringMapping(table, mapMgr.getMapping(String.class), dismd);
        }
        return null;
    }

    // These extend DiscriminatorMapping so that we can have different expression/literal for string/numeric discriminators.

    /**
     * Discriminator using a String delegate.
     */
    public static final class DiscriminatorStringMapping extends DiscriminatorMapping
    {
        /**
         * Constructor.
         * @param table Datastore table
         * @param delegate The JavaTypeMapping to delegate storage
         * @param dismd Metadata for the discriminator
         */
        public DiscriminatorStringMapping(Table table, JavaTypeMapping delegate, DiscriminatorMetaData dismd)
        {
            super(table, delegate, dismd);
        }
    }

    /**
     * Discriminator using a Long delegate.
     */
    public static final class DiscriminatorLongMapping extends DiscriminatorMapping
    {
        /**
         * Constructor.
         * @param table Datastore table
         * @param delegate The JavaTypeMapping to delegate storage
         * @param dismd Discriminator metadata
         */
        public DiscriminatorLongMapping(Table table, JavaTypeMapping delegate, DiscriminatorMetaData dismd)
        {
            super(table, delegate, dismd);
        }
    }
}