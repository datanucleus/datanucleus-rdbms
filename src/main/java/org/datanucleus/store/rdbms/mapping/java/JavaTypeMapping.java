/**********************************************************************
Copyright (c) 2004 Erik Bengtson and others. All rights reserved.
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
2009 Andy Jefferson - clean out many methods and simplify
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.mapping.datastore.DatastoreMapping;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.util.Localiser;

/**
 * Representation of the mapping of a Java type.
 * The java type maps to one or more datastore mappings. This means that a field/property
 * in a java class can be mapped to many columns in a table (in an RDBMS).
 * A JavaTypeMapping can exist in 2 forms
 * <ul>
 * <li>Constructed for a field/property managed by a datastore container, and so has metadata/container 
 * information</li>
 * <li>Constructed to represent a parameter in a query, so has no metadata and container information.</li>
 * </ul>
 */
public abstract class JavaTypeMapping
{
    /** MetaData for the field/property that we are mapping. Null when it applies to a query parameter. */
    protected AbstractMemberMetaData mmd;

    /**
     * Role of the mapping for the field. Whether it is for the field as a whole, or element of
     * a collection field (in a join table), or key/value of a map field (in a join table).
     */
    protected int roleForMember = FieldRole.ROLE_NONE;

    /** The Datastore mappings for this Java type. */
    protected DatastoreMapping[] datastoreMappings = new DatastoreMapping[0];

    /** The Table storing this mapping. Null when it applies to a query parameter. */
    protected Table table;

    /** StoreManager for the datastore being used. */
    protected RDBMSStoreManager storeMgr;

    /** Actual type being mapped */
    protected String type;

    /**
     * Mapping of the reference on the end of a bidirectional association.
     * Only used when this mapping doesn't have datastore fields, but the other side has.
     */
    protected JavaTypeMapping referenceMapping;

    /**
     * Create a new empty JavaTypeMapping.
     * The caller must call one of the initialize() methods to initialise the instance with the 
     * DatastoreAdapter and its type.
     * The combination of this empty constructor and one of the initialize() methods is used instead of 
     * parameterised constructors for efficiency purpose, both in execution time and code maintainability.
     * See MappingFactory for how they are used.
     * Concrete subclasses must have a publicly accessible empty constructor. 
     */
    protected JavaTypeMapping()
    {
    }

    /**
     * Initialise this JavaTypeMapping with the given StoreManager for the given type.
     * Used when the mapping is for a parameter in a query.
     * This will not set the "mmd" and "datastoreContainer" parameters. If these are required for
     * usage of the mapping then you should call
     * "setFieldInformation(AbstractMemberMetaData, DatastoreContainerObject)" below
     * Subclasses should override this method to perform any datastore initialization operations.
     * @param storeMgr The Datastore Adapter that this Mapping should use.
     * @param type The Class that this mapping maps to the database.
     */
    public void initialize(RDBMSStoreManager storeMgr, String type)
    {
        this.storeMgr = storeMgr;
        this.type = type;
    }

    /**
     * Initialize this JavaTypeMapping for the supplied table and field/property metadata.
     * Subclasses should override this method to perform any datastore initialization operations.
     * Assumes the "roleForMember" is already set
     * @param mmd MetaData for the field/property to be mapped (if any)
     * @param table The table storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     */
    public void initialize(AbstractMemberMetaData mmd, Table table, ClassLoaderResolver clr)
    {
        this.storeMgr = table.getStoreManager();
        this.mmd = mmd;
        this.table = table;

        // Set the type based on what role this mapping has for the field as a whole
        if (roleForMember == FieldRole.ROLE_ARRAY_ELEMENT)
        {
            this.type = mmd.getArray().getElementType();
        }
        else if (roleForMember == FieldRole.ROLE_COLLECTION_ELEMENT)
        {
            this.type = mmd.getCollection().getElementType();
        }
        else if (roleForMember == FieldRole.ROLE_MAP_KEY)
        {
            this.type = mmd.getMap().getKeyType();
        }
        else if (roleForMember == FieldRole.ROLE_MAP_VALUE)
        {
            this.type = mmd.getMap().getValueType();
        }
        else
        {
            this.type = mmd.getType().getName();
        }
    }

    /**
     * Hash code function.
     * @return The hash code for this object
     */
    public int hashCode()
    {
        return mmd == null || table == null ? super.hashCode() : (mmd.hashCode() ^ table.hashCode());
    }

    /**
     * Equality operator.
     * @param obj Object to compare against
     * @return Whether they are equal
     */
    public boolean equals(Object obj)
    {
        if (obj == null || !obj.getClass().equals(getClass()))
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }

        JavaTypeMapping other = (JavaTypeMapping)obj;
        return mmd.equals(other.mmd) && table.equals(other.table);
    }

    /**
     * Method to set the metadata of the member for which this mapping applies.
     * For use where the mapping was created for a particular type (using the 
     * initialize(StoreManager, String) and we now have the member that it applies for.
     * @param mmd Field/Property MetaData
     */
    public void setMemberMetaData(AbstractMemberMetaData mmd)
    {
        this.mmd = mmd;
    }

    /**
     * Accessor for the MetaData of the field/property being mapped. Will be null if this mapping is for a literal in a query.
     * @return Returns the metadata for the field or property
     */
    public AbstractMemberMetaData getMemberMetaData()
    {
        return mmd;
    }

    public RDBMSStoreManager getStoreManager()
    {
        return storeMgr;
    }

    public void setTable(Table table)
    {
        this.table = table;
    }

    /**
     * Accessor for the table. Will be null if this mapping is for a literal in a query.
     * @return The datastore class containing this mapped field.
     */
    public Table getTable()
    {
        return table;
    }

    /**
     * Accessor for the role of this mapping for the field/property.
     * @return Role of this mapping for the field/property
     */
    public int getRoleForMember()
    {
        return roleForMember;
    }

    /**
     * Method to set the role for the field/property.
     * Should be called before initialize().
     * @param role Role for field/property.
     */
    public void setRoleForMember(int role)
    {
        roleForMember = role;
    }

    /** Absolute field number for this mapping. Will match "mmd" unless this is an embedded field. */
    protected int absFieldNumber = -1;
    protected int getAbsoluteFieldNumber()
    {
        if (absFieldNumber < 0 && mmd != null)
        {
            absFieldNumber = mmd.getAbsoluteFieldNumber();
        }
        return absFieldNumber;
    }
    public void setAbsFieldNumber(int num)
    {
        absFieldNumber = num;
    }

    /**
     * Convenience method to return if the (part of the) field being represented by this mapping is serialised.
     * @return Whether to use Java serialisation
     */
    public boolean isSerialised()
    {
        if (roleForMember == FieldRole.ROLE_COLLECTION_ELEMENT)
        {
            if (mmd == null)
            {
                return false;
            }
            return (mmd.getCollection() != null ? mmd.getCollection().isSerializedElement() : false);
        }
        else if (roleForMember == FieldRole.ROLE_ARRAY_ELEMENT)
        { 
            if (mmd == null)
            {
                return false;
            }
            return (mmd.getArray() != null ? mmd.getArray().isSerializedElement() : false);
        }
        else if (roleForMember == FieldRole.ROLE_MAP_KEY)
        {
            if (mmd == null)
            {
                return false;
            }
            return (mmd.getMap() != null ? mmd.getMap().isSerializedKey() : false);
        }
        else if (roleForMember == FieldRole.ROLE_MAP_VALUE)
        {
            if (mmd == null)
            {
                return false;
            }
            return (mmd.getMap() != null ? mmd.getMap().isSerializedValue() : false);
        }
        else
        {
            return (mmd != null ? mmd.isSerialized() : false);
        }
    }

    /**
     * Accessor for whether this mapping is nullable
     * @return Whether it is nullable
     */
    public boolean isNullable()
    {
        for (int i=0; i<datastoreMappings.length; i++)
        {
            if (!datastoreMappings[i].isNullable())
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether the mapping has a simple (single column) datastore representation.
     * @return Whether it has a simple datastore representation (single column)
     */
    public boolean hasSimpleDatastoreRepresentation()
    {
        return true;
    }

    /**
     * The vast majority of types can be added to a (SQL) statement in their String form
     * and the statement would operate ok. Some types (e.g spatial types) cannot be used
     * in their String form in a statement, so have to be represented as parameters to the
     * statement.
     * @return Whether a literal of this type can be considered in its String form (otherwise
     *     has to be a parameter).
     */
    public boolean representableAsStringLiteralInStatement()
    {
        return true;
    }

    /**
     * Accessor for the datastore mappings for this java type
     * @return The datastore mapping(s)
     */
    public DatastoreMapping[] getDatastoreMappings()
    {
        return datastoreMappings;
    }

    /**
     * Accessor for a datastore mapping
     * @param index The id of the mapping
     * @return The datastore mapping
     */
    public DatastoreMapping getDatastoreMapping(int index)
    {
        return datastoreMappings[index];
    }

    /**
     * Method to add a datastore mapping
     * @param datastoreMapping The datastore mapping
     */
    public void addDatastoreMapping(DatastoreMapping datastoreMapping)
    {
        DatastoreMapping[] dm = datastoreMappings;
        datastoreMappings = new DatastoreMapping[datastoreMappings.length+1];
        System.arraycopy(dm, 0, datastoreMappings, 0, dm.length);
        datastoreMappings[dm.length] = datastoreMapping;
    }

    /**
     * Accessor for the number of datastore mappings.
     * This typically equates to the number of columns
     * @return the number of datastore mappings
     */
    public int getNumberOfDatastoreMappings()
    {
        return datastoreMappings.length;
    }

    /**
     * Method to return the value to be stored in the specified datastore index given the overall
     * value for this java type.
     * All multi-column mappings must override this.
     * @param nucleusCtx Context
     * @param index The datastore index
     * @param value The overall value for this java type
     * @return The value for this datastore index
     */
    public Object getValueForDatastoreMapping(NucleusContext nucleusCtx, int index, Object value)
    {
        return value;
    }

    /**
     * Accessor for the mapping at the other end of a relation when this field is
     * part of a 1-1, 1-N, M-N (bidirectional) relation. Will be null otherwise.
     * @return The mapping at the other end.
     */
    public JavaTypeMapping getReferenceMapping()
    {
        return referenceMapping; 
    }

    /**
     * Method to set the mapping at the other end of the relation.
     * Only used when part of a (bidirectional) relation.
     * @param referenceMapping The mapping at the other end
     */
    public void setReferenceMapping(JavaTypeMapping referenceMapping)
    {
        this.referenceMapping = referenceMapping; 
    }

    /**
     * Accessor for the java type being mapped.
     * This is the java type that the mapping represents. Some examples :
     * <ul>
     * <li>if the field is of type "MyClass" then the mapping will be OIDMapping (or subclass)
     * the javaType will be OID, and the type will be MyClass.</li>
     * <li>if the field is of type "int" then the mapping will be IntegerMapping, the javaType will
     * be Integer, and the type will be int.</li>
     * </ul>
     * The "java type" is the java-type name used in the plugin.xml mapping file
     * @return The java type
     */
    public abstract Class getJavaType();

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore
     * field. This java-type must have an entry in the datastore mappings.
     * The default implementation throws an UnsupportedOperationException.
     *
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForDatastoreMapping(int index)
    {
        throw new UnsupportedOperationException("Datastore type mapping is not supported by: "+getClass());
    }

    /**
     * Accessor for the class name of the object that is being mapped here.
     * There are mainly two situations: 
     * <ul>
     * <li>For a JavaTypeMapping that maps a PersistentCapable class field, this will return
     * the type of the field. For example with a field of type "MyClass" this will return "MyClass"</li>
     * <li>For a JavaTypeMapping that maps a variable or parameter in a query, this will return
     * the type declared in the query.</li>
     * </ul>
     * @return The actual type that this Mapping maps.
     */
    public String getType()
    {
        return type;
    }

    /**
     * Accessor for whether this mapping is to be included in any fetch statement.
     * @return Whether to include this mapping in a fetch statement
     */
    public boolean includeInFetchStatement()
    {
        return true;
    }

    /**
     * Accessor for whether this mapping is to be included in the update statement.
     * @return Whether to include in update statement
     */
    public boolean includeInUpdateStatement()
    {
        return true;
    }

    /**
     * Accessor for whether this mapping is to be included in the insert statement.
     * @return Whether to include in insert statement
     */
    public boolean includeInInsertStatement()
    {
        return true;
    }

    /**
     * Utility to output any error message.
     * @param method The method that failed.
     * @return the localised failure message
     */
    protected String failureMessage(String method)
    {
        return Localiser.msg("041004",getClass().getName(),method);
    }

    // ------------------- Accessors & Mutators for datastore access -----------------------

    /**
     * Sets a <code>value</code> into <code>datastoreStatement</code> 
     * at position specified by <code>exprIndex</code>.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param exprIndex the position of the value in the statement
     * @param value the value
     */
    public void setBoolean(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, boolean value)
    {
        throw new NucleusException(failureMessage("setBoolean")).setFatal();
    }

    /**
     * Obtains a value from <code>datastoreResults</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param ec ExecutionContext
     * @param rs ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    public boolean getBoolean(ExecutionContext ec, ResultSet rs, int[] exprIndex)
    {
        throw new NucleusException(failureMessage("setBoolean")).setFatal();
    }

    /**
     * Sets a <code>value</code> into <code>datastoreStatement</code> 
     * at position specified by <code>exprIndex</code>.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param exprIndex the position of the value in the statement
     * @param value the value
     */
    public void setChar(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, char value)
    {
        throw new NucleusException(failureMessage("setChar")).setFatal();
    }

    /**
     * Obtains a value from <code>datastoreResults</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param ec ExecutionContext
     * @param rs ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    public char getChar(ExecutionContext ec, ResultSet rs, int[] exprIndex)
    {
        throw new NucleusException(failureMessage("getChar")).setFatal();
    }

    /**
     * Sets a <code>value</code> into <code>datastoreStatement</code> 
     * at position specified by <code>exprIndex</code>.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param exprIndex the position of the value in the statement
     * @param value the value
     */
    public void setByte(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, byte value)
    {
        throw new NucleusException(failureMessage("setByte")).setFatal();
    }

    /**
     * Obtains a value from <code>datastoreResults</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param ec ExecutionContext
     * @param rs ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    public byte getByte(ExecutionContext ec, ResultSet rs, int[] exprIndex)
    {
        throw new NucleusException(failureMessage("getByte")).setFatal();
    }

    /**
     * Sets a <code>value</code> into <code>datastoreStatement</code> 
     * at position specified by <code>exprIndex</code>.
     * @param ec execution context
     * @param ps PreparedStatement
     * @param exprIndex the position of the value in the statement
     * @param value the value
     */
    public void setShort(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, short value)
    {
        throw new NucleusException(failureMessage("setShort")).setFatal();
    }

    /**
     * Obtains a value from <code>datastoreResults</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param ec ExecutionContext
     * @param rs ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    public short getShort(ExecutionContext ec, ResultSet rs, int[] exprIndex)
    {
        throw new NucleusException(failureMessage("getShort")).setFatal();
    }

    /**
     * Sets a <code>value</code> into <code>datastoreStatement</code> 
     * at position specified by <code>exprIndex</code>.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param exprIndex the position of the value in the statement
     * @param value the value
     */
    public void setInt(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, int value)
    {
        throw new NucleusException(failureMessage("setInt")).setFatal();
    }

    /**
     * Obtains a value from <code>datastoreResults</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param ec ExecutionContext
     * @param rs ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    public int getInt(ExecutionContext ec, ResultSet rs, int[] exprIndex)
    {
        throw new NucleusException(failureMessage("getInt")).setFatal();
    }

    /**
     * Sets a <code>value</code> into <code>datastoreStatement</code> 
     * at position specified by <code>exprIndex</code>.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param exprIndex the position of the value in the statement
     * @param value the value
     */
    public void setLong(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, long value)
    {
        throw new NucleusException(failureMessage("setLong")).setFatal();
    }

    /**
     * Obtains a value from <code>datastoreResults</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param ec ExecutionContext
     * @param rs ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    public long getLong(ExecutionContext ec, ResultSet rs, int[] exprIndex)
    {
        throw new NucleusException(failureMessage("getLong")).setFatal();
    }

    /**
     * Sets a <code>value</code> into <code>datastoreStatement</code> 
     * at position specified by <code>exprIndex</code>.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param exprIndex the position of the value in the statement
     * @param value the value
     */
    public void setFloat(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, float value)
    {
        throw new NucleusException(failureMessage("setFloat")).setFatal();
    }

    /**
     * Obtains a value from <code>datastoreResults</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param ec ExecutionContext
     * @param rs ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    public float getFloat(ExecutionContext ec, ResultSet rs, int[] exprIndex)
    {
        throw new NucleusException(failureMessage("getFloat")).setFatal();
    }

    /**
     * Sets a <code>value</code> into <code>datastoreStatement</code> 
     * at position specified by <code>exprIndex</code>.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param exprIndex the position of the value in the statement
     * @param value the value
     */
    public void setDouble(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, double value)
    {
        throw new NucleusException(failureMessage("setDouble")).setFatal();
    }

    /**
     * Obtains a value from <code>datastoreResults</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param ec ExecutionContext
     * @param rs ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    public double getDouble(ExecutionContext ec, ResultSet rs, int[] exprIndex)
    {
        throw new NucleusException(failureMessage("getDouble")).setFatal();
    }

    /**
     * Sets a <code>value</code> into <code>datastoreStatement</code> 
     * at position specified by <code>exprIndex</code>.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param exprIndex the position of the value in the statement
     * @param value the value
     */
    public void setString(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, String value)
    {
        throw new NucleusException(failureMessage("setString")).setFatal();
    }

    /**
     * Obtains a value from <code>datastoreResults</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param ec ExecutionContext
     * @param rs ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    public String getString(ExecutionContext ec, ResultSet rs, int[] exprIndex)
    {
        throw new NucleusException(failureMessage("getString")).setFatal();
    }

    /**
     * Sets a <code>value</code> into <code>datastoreStatement</code> 
     * at position specified by <code>exprIndex</code>.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param exprIndex the position of the value in the statement
     * @param value the value
     * @param ownerOP the owner ObjectProvider
     * @param ownerFieldNumber the owner absolute field number
     */    
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value, ObjectProvider ownerOP, int ownerFieldNumber)
    {
        throw new NucleusException(failureMessage("setObject")).setFatal();
    }

    /**
     * Sets a <code>value</code> into <code>datastoreStatement</code> 
     * at position specified by <code>exprIndex</code>.
     * @param ec ExecutionContext
     * @param ps PreparedStatement
     * @param exprIndex the position of the value in the statement
     * @param value the value
     */
    public void setObject(ExecutionContext ec, PreparedStatement ps, int[] exprIndex, Object value)
    {
        throw new NucleusException(failureMessage("setObject")).setFatal();
    }

    /**
     * Obtains a value from <code>datastoreResults</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param ec ExecutionContext
     * @param rs an object returned from the datastore with values 
     * @param exprIndex the position of the value in the result
     * @param ownerOP the owner ObjectProvider
     * @param ownerFieldNumber the owner absolute field number
     * @return the value
     */
    public Object getObject(ExecutionContext ec, ResultSet rs, int[] exprIndex, ObjectProvider ownerOP, int ownerFieldNumber)
    {
        throw new NucleusException(failureMessage("getObject")).setFatal();
    }

    /**
     * Obtains a value from <code>datastoreResults</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param ec ExecutionContext
     * @param rs ResultSet
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    public Object getObject(ExecutionContext ec, ResultSet rs, int[] exprIndex)
    {
        throw new NucleusException(failureMessage("getObject")).setFatal();
    }

    /**
     * Convenience method to return the ColumnMetaData appropriate for this mapping.
     * If the mapping is in a join table then picks the correct component for the column definition.
     * @param mmd Metadata for the member
     * @param role The role this mapping plays for the specified member
     * @return The column metadata (if any)
     */
    protected static ColumnMetaData[] getColumnMetaDataForMember(AbstractMemberMetaData mmd, int role)
    {
        if (mmd == null)
        {
            return null;
        }

        ColumnMetaData[] colmds = null;
        if (role == FieldRole.ROLE_COLLECTION_ELEMENT || role == FieldRole.ROLE_ARRAY_ELEMENT)
        {
            if (mmd.getJoinMetaData() != null)
            {
                // Mapping for field in a join table column
                if (mmd.getElementMetaData() != null && mmd.getElementMetaData().getColumnMetaData() != null)
                {
                    colmds = mmd.getElementMetaData().getColumnMetaData();
                }
            }
        }
        else if (role == FieldRole.ROLE_MAP_KEY)
        {
            if (mmd.getJoinMetaData() != null)
            {
                // Mapping for field in a join table column
                if (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getColumnMetaData() != null)
                {
                    colmds = mmd.getKeyMetaData().getColumnMetaData();
                }
            }
        }
        else if (role == FieldRole.ROLE_MAP_VALUE)
        {
            if (mmd.getJoinMetaData() != null)
            {
                // Mapping for field in a join table column
                if (mmd.getValueMetaData() != null && mmd.getValueMetaData().getColumnMetaData() != null)
                {
                    colmds = mmd.getValueMetaData().getColumnMetaData();
                }
            }
        }
        else
        {
            if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
            {
                colmds = mmd.getColumnMetaData();
            }
        }
        return colmds;
    }
}