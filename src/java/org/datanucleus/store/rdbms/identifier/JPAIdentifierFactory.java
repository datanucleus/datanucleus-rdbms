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
**********************************************************************/
package org.datanucleus.store.rdbms.identifier;

import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.schema.naming.NamingCase;

/**
 * Factory that creates immutable instances of DatastoreIdentifier for mapped datastores with JPA.
 * Includes the JPA naming strategy, naming as follows
 * <ul>
 * <li>Class called "MyClass" will generate table name of "MYCLASS"</li>
 * <li>Field called "myField" will generate column name of "MYFIELD"</li>
 * <li>Join table will be named after the ownerClass and the otherClass so "MyClass" joining to "MyOtherClass"
 * will have a join table called "MYCLASS_MYOTHERCLASS"</li>
 * <li>Datastore-identity column for class "MyClass" will be "MYCLASS_ID" (not part of JPA)</li>
 * <li>1-N uni between "MyClass" (field="myField") and "MyElement" will have FK in "MYELEMENT" of MYFIELD_MYCLASS_ID</li>
 * <li>1-N bi between "MyClass" (field="myField") and "MyElement" (field="myClassRef") will have FK in "MYELEMENT"
 * of name "MYCLASSREF_MYCLASS_ID".</li>
 * <li>1-1 uni between "MyClass" (field="myField") and "MyElement" will have FK in "MYCLASS" of name "MYFIELD_MYELEMENT_ID"</li>
 * <li>Discriminator field columns will, by default, be called "DTYPE"</li>
 * <li>Version field columns will, by default, be called "VERSION"</li>
 * <li>Index (ordering) field columns will, for field "myField", be called "MYFIELD_ORDER"</li>
 * <li>Adapter index field columns will, by default, be called "IDX"</li>
 * </ul>
 */
public class JPAIdentifierFactory extends AbstractIdentifierFactory
{
    /**
     * Constructor.
     * The properties accepted are
     * <ul>
     * <li>RequiredCase : what case the identifiers should be in</li>
     * <li>DefaultCatalog : default catalog to use (if any)</li>
     * <li>DefaultSchema : default schema to use (if any)</li>
     * </ul>
     * @param dba Datastore adapter
     * @param clr ClassLoader resolver
     * @param props Any properties controlling identifier generation
     */
    public JPAIdentifierFactory(DatastoreAdapter dba, ClassLoaderResolver clr, Map props)
    {
        super(dba, clr, props);
    }

    /**
     * Method to return a Table identifier for the join table of the specified field/property.
     * @param mmd Meta data for the field/property
     * @return The identifier for the table
     */
    public DatastoreIdentifier newTableIdentifier(AbstractMemberMetaData mmd)
    {
        String identifierName = null;
        String schemaName = null;
        String catalogName = null;

        // Assign an identifier name based on the user-specified table/column name (if any)

        // SCO table for this field
        AbstractMemberMetaData[] relatedMmds = null;
        if (mmd.getColumnMetaData().length > 0 && mmd.getColumnMetaData()[0].getName() != null)
        {
            // Name the table based on the column
            identifierName = mmd.getColumnMetaData()[0].getName();
        }
        else if (mmd.hasContainer())
        {
            // Check for a specified join table name
            if (mmd.getTable() != null)
            {
                // Join table name specified at this side
                String specifiedName = mmd.getTable();
                String[] parts = getIdentifierNamePartsFromName(specifiedName);
                if (parts != null)
                {
                    catalogName = parts[0];
                    schemaName = parts[1];
                    identifierName = parts[2];
                }
                if (catalogName == null)
                {
                    catalogName = mmd.getCatalog();
                }
                if (schemaName == null)
                {
                    schemaName = mmd.getSchema();
                }
            }
            else
            {
                relatedMmds = mmd.getRelatedMemberMetaData(clr);
                if (relatedMmds != null && relatedMmds[0].getTable() != null)
                {
                    String specifiedName = relatedMmds[0].getTable();
                    String[] parts = getIdentifierNamePartsFromName(specifiedName);
                    if (parts != null)
                    {
                        catalogName = parts[0];
                        schemaName = parts[1];
                        identifierName = parts[2];
                    }
                    if (catalogName == null)
                    {
                        catalogName = relatedMmds[0].getCatalog();
                    }
                    if (schemaName == null)
                    {
                        schemaName = relatedMmds[0].getSchema();
                    }
                }
            }
        }

        // No schema/catalog specified in the MetaData table "name" so try alternative sources
        // Note that we treat these as pairs (they both come from the same source)
        if (schemaName == null && catalogName == null)
        {
            // Check the <class schema="..." catalog="..." >
            if (mmd.getParent() instanceof AbstractClassMetaData)
            {
                AbstractClassMetaData ownerCmd = (AbstractClassMetaData)mmd.getParent();
                if (dba.supportsOption(DatastoreAdapter.CATALOGS_IN_TABLE_DEFINITIONS))
                {
                    catalogName = ownerCmd.getCatalog();
                }
                if (dba.supportsOption(DatastoreAdapter.SCHEMAS_IN_TABLE_DEFINITIONS))
                {
                    schemaName = ownerCmd.getSchema();
                }
            }

            if (schemaName == null && catalogName == null)
            {
                // Still no values, so try the PMF settings.
                if (dba.supportsOption(DatastoreAdapter.CATALOGS_IN_TABLE_DEFINITIONS))
                {
                    catalogName = this.defaultCatalogName;
                }
                if (dba.supportsOption(DatastoreAdapter.SCHEMAS_IN_TABLE_DEFINITIONS))
                {
                    schemaName = this.defaultSchemaName;
                }
            }
        }

        // No user-specified name, so generate a default using the previously created fallback
        if (identifierName == null)
        {
            // Generate a fallback name, based on the ownerClass/otherClass names
            String ownerClass = mmd.getClassName(false);
            String otherClass = mmd.getTypeName();
            if (mmd.hasCollection())
            {
                otherClass = mmd.getCollection().getElementType();
            }
            else if (mmd.hasArray())
            {
                otherClass = mmd.getArray().getElementType();
            }
            else if (mmd.hasMap())
            {
                otherClass = mmd.getMap().getValueType();
            }

            if (mmd.hasCollection() && relatedMmds != null && relatedMmds[0].hasCollection() && mmd.getMappedBy() != null)
            {
                // M-N collection and the owner is the other side
                ownerClass = relatedMmds[0].getClassName(false);
                otherClass = relatedMmds[0].getCollection().getElementType();
            }

            otherClass = otherClass.substring(otherClass.lastIndexOf('.')+1);
            String unique_name = ownerClass + getWordSeparator() + otherClass;

            identifierName = unique_name;
        }

        // Generate the table identifier now that we have the identifier name
        DatastoreIdentifier identifier = newTableIdentifier(identifierName);
        if (schemaName != null)
        {
            identifier.setSchemaName(schemaName);
        }
        if (catalogName != null)
        {
            identifier.setCatalogName(catalogName);
        }

        return identifier;
    }

    /**
     * Method to return a Table identifier for the specified class.
     * @param cmd Meta data for the class
     * @return The identifier for the table
     **/
    public DatastoreIdentifier newTableIdentifier(AbstractClassMetaData cmd)
    {
        String identifierName = null;
        String schemaName = null;
        String catalogName = null;

        // Assign an identifier name based on the user-specified table/column name (if any)
        String specifiedName = cmd.getTable();
        String[] parts = getIdentifierNamePartsFromName(specifiedName);
        if (parts != null)
        {
            catalogName = parts[0];
            schemaName = parts[1];
            identifierName = parts[2];
        }

        // No schema/catalog specified in the MetaData table "name" so try alternative sources
        // Note that we treat these as pairs (they both come from the same source)
        if (schemaName == null && catalogName == null)
        {
            // Check the <class schema="..." catalog="..." >
            if (dba.supportsOption(DatastoreAdapter.CATALOGS_IN_TABLE_DEFINITIONS))
            {
                catalogName = cmd.getCatalog();
            }
            if (dba.supportsOption(DatastoreAdapter.SCHEMAS_IN_TABLE_DEFINITIONS))
            {
                schemaName = cmd.getSchema();
            }

            if (schemaName == null && catalogName == null)
            {
                // Still no values, so try the PMF settings.
                if (dba.supportsOption(DatastoreAdapter.CATALOGS_IN_TABLE_DEFINITIONS))
                {
                    catalogName = this.defaultCatalogName;
                }
                if (dba.supportsOption(DatastoreAdapter.SCHEMAS_IN_TABLE_DEFINITIONS))
                {
                    schemaName = this.defaultSchemaName;
                }
            }
        }

        // No user-specified name, so generate a default using the previously created fallback
        if (identifierName == null)
        {
            // Generate a fallback name, based on the last part of the class name ("MyClass" becomes "MYCLASS")
            String unique_name = cmd.getFullClassName().substring(cmd.getFullClassName().lastIndexOf('.')+1);

            identifierName = unique_name;
        }

        // Generate the table identifier now that we have the identifier name
        DatastoreIdentifier identifier = newTableIdentifier(identifierName);
        if (schemaName != null)
        {
            identifier.setSchemaName(schemaName);
        }
        if (catalogName != null)
        {
            identifier.setCatalogName(catalogName);
        }

        return identifier;
    }

    /**
     * Method to generate an identifier name for reference field, based on the metadata for the
     * field, and the ClassMetaData for the implementation.
     * @param refMetaData the metadata for the reference field
     * @param implMetaData the AbstractClassMetaData for this implementation
     * @param implIdentifier PK identifier for the implementation
     * @param embedded Whether the identifier is for a field embedded
     * @param fieldRole The role to be performed by this column e.g FK, collection element ?
     * @return The DatastoreIdentifier
     */
    public DatastoreIdentifier newReferenceFieldIdentifier(AbstractMemberMetaData refMetaData, 
            AbstractClassMetaData implMetaData, DatastoreIdentifier implIdentifier, boolean embedded, int fieldRole)
    {
        String key = "[" + refMetaData.getFullFieldName() + "][" + implMetaData.getFullClassName() + "][" + implIdentifier.getIdentifierName() + "]";
        DatastoreIdentifier identifier = references.get(key);
        if (identifier == null)
        {
            // use a simple naming for now : <reference-name>_<impl_name>_<impl_type>
            String referenceName = refMetaData.getName();
            String implementationName = implMetaData.getFullClassName();
            int dot = implementationName.lastIndexOf('.');
            if (dot > -1)
            {
                implementationName = implementationName.substring(dot+1);
            }
            String name = referenceName + "." + implementationName + "." + implIdentifier.getIdentifierName();

            // Set the SQL identifier adding any truncation as necessary
            String datastoreID = generateIdentifierNameForJavaName(name);
            String baseID = truncate(datastoreID, dba.getDatastoreIdentifierMaxLength(IdentifierType.COLUMN));
            identifier = new ColumnIdentifier(this, baseID);
            references.put(key, identifier);
        }
        return identifier;
    }

    /**
     * Method to generate a join-table identifier. The identifier could be for a foreign-key to another 
     * table (if the destinationId is provided), or could be for a simple column in the join table.
     * @param ownerFmd MetaData for the owner field
     * @param destinationId Identifier for the identity field of the destination (if FK)
     * @param embedded Whether the identifier is for a field embedded
     * @param fieldRole The role to be performed by this column e.g FK, collection element ?
     * @return The identifier.
     */
    public DatastoreIdentifier newJoinTableFieldIdentifier(AbstractMemberMetaData ownerFmd, 
            AbstractMemberMetaData relatedFmd, DatastoreIdentifier destinationId, boolean embedded, int fieldRole)
    {
        DatastoreIdentifier identifier = null;

        if (relatedFmd != null)
        {
            // Bidirectional
            if (fieldRole == FieldRole.ROLE_OWNER)
            {
                identifier = newColumnIdentifier(relatedFmd.getName() + getWordSeparator() + destinationId.getIdentifierName());
            }
            else if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT ||
                fieldRole == FieldRole.ROLE_ARRAY_ELEMENT ||
                fieldRole == FieldRole.ROLE_MAP_KEY ||
                fieldRole == FieldRole.ROLE_MAP_VALUE)
            {
                if (destinationId != null)
                {
                    // FK to other table
                    identifier = newColumnIdentifier(ownerFmd.getName() + getWordSeparator() + destinationId.getIdentifierName());
                }
                else
                {
                    // Column in join table
                    if (fieldRole == FieldRole.ROLE_ARRAY_ELEMENT || fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT)
                    {
                        identifier = newColumnIdentifier(ownerFmd.getName() + getWordSeparator() + "ELEMENT");
                    }
                    else if (fieldRole == FieldRole.ROLE_MAP_KEY)
                    {
                        identifier = newColumnIdentifier(ownerFmd.getName() + getWordSeparator() + "KEY");
                    }
                    else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
                    {
                        identifier = newColumnIdentifier(ownerFmd.getName() + getWordSeparator() + "VALUE");
                    }
                }
            }
            else
            {
                // Not a known role for a join table so use JPOX-style naming
                identifier = newColumnIdentifier(destinationId.getIdentifierName(), embedded, fieldRole);
            }
        }
        else
        {
            // Unidirectional
            if (fieldRole == FieldRole.ROLE_OWNER)
            {
                identifier = newColumnIdentifier(ownerFmd.getClassName(false) + getWordSeparator() + destinationId.getIdentifierName());
            }
            else if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT ||
                fieldRole == FieldRole.ROLE_ARRAY_ELEMENT ||
                fieldRole == FieldRole.ROLE_MAP_KEY ||
                fieldRole == FieldRole.ROLE_MAP_VALUE)
            {
                if (destinationId != null)
                {
                    // FK to other table
                    identifier = newColumnIdentifier(ownerFmd.getName() + getWordSeparator() + destinationId.getIdentifierName());
                }
                else
                {
                    // Column in join table
                    if (fieldRole == FieldRole.ROLE_ARRAY_ELEMENT || fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT)
                    {
                        identifier = newColumnIdentifier(ownerFmd.getName() + getWordSeparator() + "ELEMENT");
                    }
                    else if (fieldRole == FieldRole.ROLE_MAP_KEY)
                    {
                        identifier = newColumnIdentifier(ownerFmd.getName() + getWordSeparator() + "KEY");
                    }
                    else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
                    {
                        identifier = newColumnIdentifier(ownerFmd.getName() + getWordSeparator() + "VALUE");
                    }
                }
            }
            else
            {
                // Not a known role for a join table so use JPOX-style naming
                identifier = newColumnIdentifier(destinationId.getIdentifierName(), embedded, fieldRole);
            }
        }

        return identifier;
    }

    /**
     * Method to generate a FK/FK-index field identifier. 
     * The identifier could be for the FK field itself, or for a related index for the FK.
     * @param ownerFmd MetaData for the owner field
     * @param relatedFmd MetaData for the related field
     * @param destinationId Identifier for the identity field of the destination table (if strict FK)
     * @param embedded Whether the identifier is for a field embedded
     * @param fieldRole The role to be performed by this column e.g ROLE_OWNER, ROLE_INDEX
     * @return The identifier
     */
    public DatastoreIdentifier newForeignKeyFieldIdentifier(AbstractMemberMetaData ownerFmd, AbstractMemberMetaData relatedFmd,
            DatastoreIdentifier destinationId, boolean embedded, int fieldRole)
    {
        if (relatedFmd != null)
        {
            // Bidirectional
            if (fieldRole == FieldRole.ROLE_OWNER)
            {
                return newColumnIdentifier(relatedFmd.getName() + "." + destinationId.getIdentifierName(), embedded, fieldRole);
            }
            else if (fieldRole == FieldRole.ROLE_INDEX)
            {
                return newColumnIdentifier(relatedFmd.getName() + "." + destinationId.getIdentifierName(), embedded, fieldRole);
            }
            else
            {
                throw new NucleusException("Column role " + fieldRole + " not supported by this method").setFatal();
            }
        }
        else
        {
            if (fieldRole == FieldRole.ROLE_OWNER)
            {
                // FK field (FK collection/array/list/map)
                return newColumnIdentifier(ownerFmd.getName() + "." + destinationId.getIdentifierName(), embedded, fieldRole);
            }
            else if (fieldRole == FieldRole.ROLE_INDEX)
            {
                // Order field for FK (FK list)
                return newColumnIdentifier(ownerFmd.getName() + ".IDX", embedded, fieldRole);
            }
            else
            {
                throw new NucleusException("Column role " + fieldRole + " not supported by this method").setFatal();
            }
        }
    }

    /**
     * Method to return an identifier for a discriminator column.
     * Returns an identifier "DTYPE"
     * @return The discriminator column identifier
     */
    public DatastoreIdentifier newDiscriminatorFieldIdentifier()
    {
        String name = "DTYPE"; // JPA1 spec [9.1.30] discriminator column defaults to "DTYPE"
        DatastoreIdentifier identifier = columns.get(name);
        if (identifier == null)
        {
            identifier = new ColumnIdentifier(this, name);
            columns.put(name, identifier);
        }
        return identifier;
    }

    /**
     * Method to return an identifier for a version datastore field.
     * @return The version datastore field identifier
     */
    public DatastoreIdentifier newVersionFieldIdentifier()
    {
        String name = "VERSION";
        DatastoreIdentifier identifier = columns.get(name);
        if (identifier == null)
        {
            identifier = new ColumnIdentifier(this, name);
            columns.put(name, identifier);
        }
        return identifier;
    }

    /**
     * Method to return an identifier for an index (ordering) datastore field.
     * @param mmd MetaData for the field/property
     * @return The index datastore field identifier
     */
    public DatastoreIdentifier newIndexFieldIdentifier(AbstractMemberMetaData mmd)
    {
        String name = mmd.getName() + getWordSeparator() + "ORDER";
        DatastoreIdentifier identifier = columns.get(name);
        if (identifier == null)
        {
            identifier = new ColumnIdentifier(this, name);
            columns.put(name, identifier);
        }
        return identifier;
    }

    /**
     * Method to return an identifier for an adapter index datastore field.
     * An "adapter index" is a column added to be part of a primary key when some other
     * column cant perform that role.
     * @return The index datastore field identifier
     */
    public DatastoreIdentifier newAdapterIndexFieldIdentifier()
    {
        String name = "IDX"; // All index fields are called IDX in this factory
        DatastoreIdentifier identifier = columns.get(name);
        if (identifier == null)
        {
            identifier = new ColumnIdentifier(this, name);
            columns.put(name, identifier);
        }
        return identifier;
    }

    /**
     * Generate a datastore identifier from a Java identifier.
     *
     * <p>Conversion consists of breaking the identifier into words, converting
     * each word to upper-case, and separating each one with an underscore "_".
     * Words are identified by a leading upper-case character.
     * Any leading or trailing underscores are removed.</p>
     *
     * @param javaName the Java identifier.
     * @return The datastore identifier
     */
    public String generateIdentifierNameForJavaName(String javaName)
    {
        if (javaName == null)
        {
            return null;
        }

        StringBuilder s = new StringBuilder();

        for (int i = 0; i < javaName.length(); ++i)
        {
            char c = javaName.charAt(i);

            if (c >= 'A' && c <= 'Z' && 
                (namingCase != NamingCase.MIXED_CASE && namingCase != NamingCase.MIXED_CASE_QUOTED))
            {
                s.append(c);
            }
            else if (c >= 'A' && c <= 'Z' &&
                (namingCase == NamingCase.MIXED_CASE || namingCase == NamingCase.MIXED_CASE_QUOTED))
            {
                s.append(c);
            }
            else if (c >= 'a' && c <= 'z' &&
                (namingCase == NamingCase.MIXED_CASE || namingCase == NamingCase.MIXED_CASE_QUOTED))
            {
                s.append(c);
            }
            else if (c >= 'a' && c <= 'z' &&
                (namingCase != NamingCase.MIXED_CASE && namingCase != NamingCase.MIXED_CASE_QUOTED))
            {
                s.append((char)(c - ('a' - 'A')));
            }
            else if (c >= '0' && c <= '9' || c=='_')
            {
                s.append(c);
            }
            else if (c == '.')
            {
                s.append(getWordSeparator());
            }
            else
            {
                String cval = "000" + Integer.toHexString(c);

                s.append(cval.substring(cval.length() - (c > 0xff ? 4 : 2)));
            }
        }

        // Remove leading and trailing underscores
        while (s.length() > 0 && s.charAt(0) == '_')
        {
            s.deleteCharAt(0);
        }
        if (s.length() == 0)
        {
            throw new IllegalArgumentException("Illegal Java identifier: " + javaName);
        }

        return s.toString();
    }

    /**
     * Accessor for the suffix to add to any column identifier, based on the role type.
     * @param role Datastore field role
     * @param embedded Whether the column is stored embedded
     * @return The suffix (e.g _ID for id columns).
     **/
    protected String getColumnIdentifierSuffix(int role, boolean embedded)
    {
        String suffix = "";
        if (role == FieldRole.ROLE_NONE)
        {
            // JPA doesnt allow datastore identity so we can do as we like here. Lets add an "_ID" to match JPOX default
            suffix = !embedded ? "_ID" : "";
        }

        return suffix;
    }
}