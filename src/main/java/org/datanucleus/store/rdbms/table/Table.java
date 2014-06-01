/**********************************************************************
Copyright (c) 2003 Mike Martin and others. All rights reserved.
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
2003 Andy Jefferson - Added getCatalogName()
2004 Andy Jefferson - Moved state constants here
2004 Andy Jefferson - Added documentation
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;

/**
 * Representation of a table in an RDBMS.
 * <P>
 * There are 2 aspects to a table. The first is the internal representation, provided here.
 * This has a state. The second aspect to the table is its external (datastore) representation.
 * This reflects whether it exists, or whether it has been deleted, etc.
 * </P>
 * <P>
 * This interface provides some methods for mapping from the internal representation to
 * the external representation. These are the methods
 * <UL>
 * <LI>create() - to create the table in the datastore</LI>
 * <LI>drop() - to drop the table from the datastore</LI>
 * <LI>validate() - to compare the internal and external representations.</LI>
 * <LI>exists() - whether the external representation exists</LI>
 * </UL>
 */
public interface Table extends org.datanucleus.store.schema.table.Table
{
    RDBMSStoreManager getStoreManager();

    /**
     * Accessor for the identifier for this object.
     * @return The identifier.
     */
    DatastoreIdentifier getIdentifier();

    /**
     * Method to add a new column to the internal representation.
     * @param storedJavaType The type of the Java field to store
     * @param name The name of the column
     * @param mapping The type mapping for this column
     * @param colmd The column MetaData
     * @return The new Column
     */
    Column addColumn(String storedJavaType, DatastoreIdentifier name, JavaTypeMapping mapping, ColumnMetaData colmd);
    
    /**
     * Checks if there is a column for the identifier
     * @param identifier the identifier of the column
     * @return true if the column exists for the identifier
     */
    boolean hasColumn(DatastoreIdentifier identifier);    

    /**
     * Accessor for the Datastore field with the specified identifier.
     * Returns null if has no column of this name.
     * @param identifier The name of the column
     * @return The column
     */
    Column getColumn(DatastoreIdentifier identifier);

    /**
     * Accessor for the ID mapping of this container object.
     * @return The ID Mapping (if present)
     */
    JavaTypeMapping getIdMapping();

    /**
     * Accessor for the mapping for the specified FieldMetaData. 
     * A datastore container object may store many fields.
     * @param mmd Metadata for the field/property
     * @return The Mapping for the member, or null if the FieldMetaData cannot be found
     */
    JavaTypeMapping getMemberMapping(AbstractMemberMetaData mmd);

    /**
     * Accessor for Discriminator MetaData.
     * @return Returns the Discriminator MetaData.
     */
    DiscriminatorMetaData getDiscriminatorMetaData();

    /**
     * Accessor for the discriminator mapping specified.
     * @param allowSuperclasses Whether we should return just the mapping from this table
     *     or whether we should return it when this table has none and the supertable has
     * @return The discriminator mapping
     */
    JavaTypeMapping getDiscriminatorMapping(boolean allowSuperclasses);

    /**
     * Accessor for the multi-tenancy mapping (if any).
     * @return The multi-tenancy mapping
     */
    JavaTypeMapping getMultitenancyMapping();

    /**
     * Accessor for the Version MetaData.
     * @return Returns the Version MetaData.
     */
    VersionMetaData getVersionMetaData();

    /**
     * Accessor for the version mapping.
     * @param allowSuperclasses Whether we should return just the mapping from this table
     *     or whether we should return it when this table has none and the supertable has
     * @return The version mapping.
     */
    JavaTypeMapping getVersionMapping(boolean allowSuperclasses);

    /**
     * Pre-initialize method; for things that must be initialized right after construction.
     * @param clr the ClassLoaderResolver
     */
    void preInitialize(ClassLoaderResolver clr);

    /**
     * Method to initialise the table.
     * @param clr The ClassLoaderResolver
     */
    void initialize(ClassLoaderResolver clr);

    /**
     * Post-initialize; for things that must be set after all classes have been initialized.
     * @param clr the ClassLoaderResolver
     */
    void postInitialize(ClassLoaderResolver clr);
    
    /**
     * Accessor for whether the table has been initialised.
     * @return Whether it is initialised.
     */
    boolean isInitialized();

    /**
     * Accessor for whether the table has been modified after being initialised.
     * @return Whether it has been modified after being initialised.
     */
    boolean isInitializedModified();

    /**
     * Method to validate the table against what is in the datastore.
     * @param conn The connection
     * @param validateColumnStructure Whether to validate down to the column structure, or just the existence
     * @param autoCreate Whether to update the table to fix any errors.
     * @param autoCreateErrors Errors found during the auto-create process
     * @return Whether it  validates successfully
     * @throws SQLException Thrown if an error occurrs in the validation
     */
    boolean validate(Connection conn, boolean validateColumnStructure, boolean autoCreate, Collection autoCreateErrors) 
    throws SQLException;

    /**
     * Accessor for whether the table is validated.
     * @return Whether it is validated.
     */
    boolean isValidated();

    /**
     * Accessor for whether the table exists in the datastore. 
     * Will throw a MissingTableException if the table doesn't exist.
     * @param conn The connecton to use to verify it
     * @param create Whether to create it if it doesn't exist
     * @return Whether the table was added.
     * @throws SQLException Thrown if an error occurs in the check
     */
    boolean exists(Connection conn, boolean create) 
    throws SQLException;

    /**
     * Method to create the table in the datastore representation.
     * @param conn The connection to use
     * @return true if the table was created
     * @throws SQLException Thrown if an error occurs creating the table.
     */
    boolean create(Connection conn)
    throws SQLException;
    
    /**
     * Method to drop the table from the datastore representation.
     * @param conn The connection to use
     * @throws SQLException Thrown if an error occurs
     */
    void drop(Connection conn)
    throws SQLException;
}