/**********************************************************************
Copyright (c) 2002 Kelly Grizzle (TJDO) and others. All rights reserved.
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
2003 Andy Jefferson - replaced TableMetadata with identifier and java name
2003 Andy Jefferson - coding standards
2004 Andy Jefferson - merged with JDOView
2004 Andy Jefferson - split out CorrespondentColumnsMapping
2005 Andy Jefferson - reworked to implement DatastoreClass and generate mappings correctly
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.store.rdbms.exceptions.NoSuchPersistentFieldException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.store.rdbms.exceptions.PersistentSuperclassNotAllowedException;
import org.datanucleus.store.rdbms.exceptions.ViewDefinitionException;
import org.datanucleus.store.rdbms.identifier.DatastoreIdentifier;
import org.datanucleus.store.rdbms.mapping.MappingConsumer;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.MacroString;
import org.datanucleus.util.NucleusLogger;

/**
 * Representation of an SQL View for a Class.
 * Requires that the class use "nondurable" identity.
 * Since a view is read-only, many methods throw exceptions that the operation is
 * not supported.
 */
public class ClassView extends ViewImpl implements DatastoreClass
{
    /** Class MetaData for the class mapping to this view. */
    private final ClassMetaData cmd;

    /** Definition of the view. */
    private final MacroString viewDef;

    /** DDL statement for creating the view **/
    private String createStatementDDL;
    
    /** Mappings for the fields of this class to map to the View. */
    private JavaTypeMapping[] fieldMappings;

    /**
     * Constructor for class view.
     * @param tableName The name of the view.
     * @param storeMgr The RDBMS manager managing this view
     * @param cmd The metadata for the class represented by this view.
     */
    public ClassView(final DatastoreIdentifier tableName, final RDBMSStoreManager storeMgr, final ClassMetaData cmd)
    {
        super(tableName, storeMgr);
        this.cmd = cmd;

        if (cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            // In all version until 5.1.0.M3 this required NONDURABLE to continue here
            NucleusLogger.DATASTORE_SCHEMA.debug("Mapping VIEW " + tableName + " to class " + cmd.getFullClassName() + " which uses " + cmd.getIdentityType());
        }

        // We expect a flat class here to map to a view.
        if (cmd.getPersistableSuperclass() != null)
        {
            throw new PersistentSuperclassNotAllowedException(cmd.getFullClassName());
        }

        // Extract the view definition from MetaData
        String viewImpStr = cmd.getValueForExtension(MetaData.EXTENSION_CLASS_VIEW_IMPORTS);
        String viewDefStr = null;
        if (dba.getVendorID() != null)
        {
            viewDefStr = cmd.getValueForExtension(MetaData.EXTENSION_CLASS_VIEW_DEFINITION + '-' + dba.getVendorID());
        }
        if (viewDefStr == null)
        {
            viewDefStr = cmd.getValueForExtension(MetaData.EXTENSION_CLASS_VIEW_DEFINITION);
        }
        if (viewDefStr == null)
        {
            throw new ViewDefinitionException(cmd.getFullClassName(), null);
        }

        viewDef = new MacroString(cmd.getFullClassName(), viewImpStr, viewDefStr);
    }
   
    /**
     * Method to initialise the view. Generates the mappings for all fields in
     * the class to map to this view.
     * @param clr The ClassLoaderResolver
     */
    public void initialize(final ClassLoaderResolver clr)
    {
        assertIsUninitialized();

        int fieldCount = cmd.getNoOfManagedMembers();
        fieldMappings = new JavaTypeMapping[fieldCount];
        for (int fieldNumber = 0; fieldNumber < fieldCount; ++fieldNumber)
        {
            AbstractMemberMetaData fmd = cmd.getMetaDataForManagedMemberAtRelativePosition(fieldNumber);
            if (fmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
            {
                fieldMappings[fieldNumber] = storeMgr.getMappingManager().getMapping(this, fmd, clr, FieldRole.ROLE_FIELD);
            }
            else if (fmd.getPersistenceModifier() != FieldPersistenceModifier.TRANSACTIONAL)
            {
                throw new NucleusException(Localiser.msg("031006", 
                    cmd.getFullClassName(), fmd.getName(), fmd.getPersistenceModifier())).setFatal();
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
     * Post initilize. For things that must be set after all classes have been initialized before 
     * @param clr the ClassLoaderResolver
     */
    public void postInitialize(final ClassLoaderResolver clr)
    {
        assertIsInitialized();

        createStatementDDL = viewDef.substituteMacros(new MacroString.MacroHandler()
            {
                public void onIdentifierMacro(MacroString.IdentifierMacro im)
                {
                    storeMgr.resolveIdentifierMacro(im, clr);
                }

                public void onParameterMacro(MacroString.ParameterMacro pm)
                {
                    throw new NucleusUserException(Localiser.msg("031009", cmd.getFullClassName(), pm));
                }
            }, clr
        );
    }
    
    /**
     * Accessor for a mapping for the ID. A view row doesn't have an id as such.
     * @return The ID mapping.
     */
    public JavaTypeMapping getIdMapping()
    {
        // Just return the first mapping that we have (since we have no "id" and this is used for "count(this)" queries so doesnt matter)
        // If there are other situations that would come through here then we would need to cater for those better
        for (int i=0;i<fieldMappings.length;i++)
        {
            if (fieldMappings[i] != null)
            {
                return fieldMappings[i];
            }
        }
        return null;
    }

    /**
     * Accessor for the base datastore class (table) managing the given field.
     * Returns null since we dont manage things the same with views.
     * @param mmd MetaData for the field
     * @return The base table.
     */
    public DatastoreClass getBaseDatastoreClassWithMember(AbstractMemberMetaData mmd)
    {
        return null;
    }

    /**
     * Accessor for the supertable for this table.
     * @return The supertable
     **/
    public DatastoreClass getSuperDatastoreClass()
    {
        return null;
    }

    /**
     * Accessor whether the supplied DatastoreClass is a supertable of this table.
     * @param table The DatastoreClass to check
     * @return Whether it is a supertable (somewhere up the inheritance tree)
     */
    public boolean isSuperDatastoreClass(DatastoreClass table)
    {
        return false;
    }

    /**
     * Accessor for any secondary tables for this table.
     * @return Secondary tables (if any)
     */
    public Collection getSecondaryDatastoreClasses()
    {
        return null;
    }

    /**
     * Accessor for the datastore identity id mapping.
     * Returns null since we dont use datastore identity for views.
     * @return Datastore identity ID mapping
     */
    public JavaTypeMapping getDatastoreIdMapping()
    {
        return null;
    }

    /**
     * Accessor for whether this table manages the specified class
     * @param className Name of the class
     * @return Whether it is managed by this table
     */
    public boolean managesClass(String className)
    {
        // We don't manage classes as such so just return false
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.DatastoreClass#getManagedClasses()
     */
    public String[] getManagedClasses()
    {
        // We don't manage classes as such so just return null
        return null;
    }

    /**
     * Convenience method to return if this table manages the columns for the supplied mapping.
     * @param mapping The mapping
     * @return Whether the mapping is managed in this table
     */
    public boolean managesMapping(JavaTypeMapping mapping)
    {
        // We don't manage mappings as such so just return false
        return false;
    }

    /**
     * Accessor for the MetaData for the named field
     * @param fieldName Name of the field
     * @return MetaData for the field
     */
    public AbstractMemberMetaData getFieldMetaData(String fieldName)
    {
        return cmd.getMetaDataForMember(fieldName);
    }

    /**
     * Accessor for the identity type in use.
     * @return The identity type
     */
    public IdentityType getIdentityType()
    {
        return cmd.getIdentityType();
    }

    /**
     * Accessor for whether this is a base datastore class (root in a hierarchy).
     * Returns true since we dont use inheritance in views.
     * @return Whether this is the base datastore class (table)
     */
    public boolean isBaseDatastoreClass()
    {
        return true;
    }

    public DatastoreClass getBaseDatastoreClass()
    {
        return this;
    }

    /**
     * Accessor for whether the object ID is attributed in the datastore.
     * Returns false since we dont use such things on views.
     * @return Whether it is attributed in the datastore.
     */
    public boolean isObjectIdDatastoreAttributed()
    {
        return false;
    }

    public void provideDatastoreIdMappings(MappingConsumer consumer)
    {
    }

    public void provideDiscriminatorMappings(MappingConsumer consumer)
    {
    }

    public void provideMultitenancyMapping(MappingConsumer consumer)
    {
    }

    public void provideMappingsForMembers(MappingConsumer consumer, AbstractMemberMetaData[] fieldNumbers, boolean includeSecondaryTables)
    {
    }

    public void provideNonPrimaryKeyMappings(MappingConsumer consumer)
    {
    }

    public void providePrimaryKeyMappings(MappingConsumer consumer)
    {
    }

    public void provideVersionMappings(MappingConsumer consumer)
    {
    }

    public void provideExternalMappings(MappingConsumer consumer, int mappingType)
    {
    }

    /**
     * Method to provide all unmapped datastore fields (columns) to the consumer.
     * @param consumer Consumer of information
     */
    public void provideUnmappedColumns(MappingConsumer consumer)
    {
    }

    /**
     * Accessor for the type of the class being represented by this view.
     * @return The name of the class being represented here
     */
    public String getType()
    {
        return cmd.getFullClassName();
    }

    /**
     * Accessor for the mapping for the specified field/property.
     * @param mmd Metadata for the field/property
     * @return The Mapping for the field.
     */
    public JavaTypeMapping getMemberMapping(AbstractMemberMetaData mmd)
    {
        assertIsInitialized();

        JavaTypeMapping m = fieldMappings[mmd.getAbsoluteFieldNumber()];
        if (m == null)
        {
            throw new NoSuchPersistentFieldException(cmd.getFullClassName(), mmd.getAbsoluteFieldNumber());
        }

        return m;
    }

    /**
     * Accessor for the mapping for the specified field only in this datastore class.
     * @param mmd Metadata of the field/property
     * @return The Mapping for the field (or null if not present here)
     */
    public JavaTypeMapping getMemberMappingInDatastoreClass(AbstractMemberMetaData mmd)
    {
        return getMemberMapping(mmd);
    }

    /**
     * Accessor for the field mapping for the specified field name.
     * @param fieldName Name of the field
     * @return The Java type mapping for the field
     */
    public JavaTypeMapping getMemberMapping(String fieldName)
    {
        assertIsInitialized();

        int rfn = cmd.getRelativePositionOfMember(fieldName);
        if (rfn < 0)
        {
            throw new NoSuchPersistentFieldException(cmd.getFullClassName(), fieldName);
        }

        return getMemberMapping(cmd.getMetaDataForManagedMemberAtRelativePosition(rfn));
    }

    /**
     * Method to return the necessary SQL create statements for this table.
     * @param props Properties for controlling the creation of views
     * @return The SQL create statements.
     */
    protected List<String> getSQLCreateStatements(Properties props)
    {
        assertIsInitialized();

        List<String> stmts = new ArrayList<>();
        StringTokenizer tokens = new StringTokenizer(createStatementDDL, ";");

        while (tokens.hasMoreTokens())
        {
            String token = tokens.nextToken();
            if (token.startsWith("--"))
            {
                // Ignore comment lines because some RDBMS will not handle these
                continue;
            }
            stmts.add(token);
        }

        return stmts;
    }
    
    /**
     * Accessor for Discriminator MetaData
     * @return Returns the Discriminator MetaData.
     */
    public final DiscriminatorMetaData getDiscriminatorMetaData()
    {
        return null; // No discriminators for Views
    }
    
    /**
     * Accessor for the discriminator mapping specified .
     * @return The mapping for the discriminator datastore field
     **/
    public JavaTypeMapping getDiscriminatorMapping(boolean allowSuperclasses)
    {
        return null; // No discriminators for Views
    }

    /**
     * Accessor for Version MetaData
     * @return Returns the Version MetaData.
     */
    public final VersionMetaData getVersionMetaData()
    {
        return null; // No versions for Views
    }

    /**
     * Accessor for the version mapping specified .
     * @return The mapping for the version datastore field
     **/
    public JavaTypeMapping getVersionMapping(boolean allowSuperclasses)
    {
        return null; // No versions for Views
    }

    public JavaTypeMapping getExternalMapping(AbstractMemberMetaData mmd, int mappingType)
    {
        // We do not support "external mappings" with a view. The mapping must be present in the view definition
        return null;
    }

    public AbstractMemberMetaData getMetaDataForExternalMapping(JavaTypeMapping mapping, int mappingType)
    {
        // We do not support "external mappings" with a view. The mapping must be present in the view definition
        return null;
    }
}