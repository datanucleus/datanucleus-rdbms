/**********************************************************************
Copyright (c) 2005 Erik Bengtson and others. All rights reserved.
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
2006 Andy Jefferson - moved catalog/schema setting code into newTableIdentifierForMetaData
2006 Andy Jefferson - changed to implement IdentifierFactory for RDBMS stores
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.identifier;

import java.util.ArrayList;
import java.util.ListIterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.schema.naming.NamingCase;

/**
 * Factory that creates immutable instances of DatastoreIdentifier for mapped datastores.
 * Includes a "JPOX" naming strategy, naming as follows
 * <ul>
 * <li>Class called "MyClass" will generate table name of "MYCLASS"</li>
 * <li>Field called "myField" will generate column name of "MY_FIELD"</li>
 * <li>Datastore id field for class "MyClass" will have the PK field "MYCLASS_ID"</li>
 * <li>Join table will be named after the class and field, so "MyClass" with field "myField" will become
 * a table with name "MYCLASS_MYFIELD".</li>
 * <li>Columns of a join table will be named after the PK fields of the owner and element. So something
 * like "MYCLASS_ID_OID" and "MYELEMENT_ID_EID"</li>
 * <li>Discriminator field columns will, by default, be called "DISCRIMINATOR"</li>
 * <li>Index field columns will, by default, be called "INTEGER_IDX"</li>
 * <li>Version field columns will, by default, be called "OPT_VERSION"</li>
 * <li>Adapter index field columns will, by default, be called "ADPT_PK_IDX"</li>
 * </ul>
 * Strictly speaking the naming of the table is inconsistent with the naming of the column since
 * the table doesn't use the word separator whereas the column does, but JPOX 1.0/1.1/1.2 were like this
 * so provided for compatibility.
 */
public class DNIdentifierFactory extends AbstractIdentifierFactory
{
    /** Prefix for all generated table names. */
    protected String tablePrefix = null;

    /** Suffix for all generated table names. */
    protected String tableSuffix = null;

    /**
     * Constructor.
     * The properties accepted are
     * <ul>
     * <li>RequiredCase : what case the identifiers should be in</li>
     * <li>DefaultCatalog : default catalog to use (if any)</li>
     * <li>DefaultSchema : default schema to use (if any)</li>
     * <li>WordSeparator : separator character(s) between identifier words</li>
     * <li>TablePrefix : Prefix to prepend to all table identifiers</li>
     * <li>TableSuffix : Suffix to append to all table identifiers</li>
     * </ul>
     * @param dba Database adapter
     * @param clr ClassLoader resolver
     * @param props Any properties controlling identifier generation
     */
    public DNIdentifierFactory(DatastoreAdapter dba, ClassLoaderResolver clr, Map props)
    {
        super(dba, clr, props);

        if (props.containsKey("WordSeparator"))
        {
            this.wordSeparator = (String)props.get("WordSeparator");
        }
        this.tablePrefix = (String)props.get("TablePrefix");
        this.tableSuffix = (String)props.get("TableSuffix");
    }

    /**
     * Method to return a Table identifier for the join table of the specified field.
     * @param mmd Meta data for the field
     * @return The identifier for the table
     **/
    public DatastoreIdentifier newTableIdentifier(AbstractMemberMetaData mmd)
    {
        String identifierName = null;
        String schemaName = null;
        String catalogName = null;

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
                    // Join table name specified at other side (1-N or M-N)
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
                        catalogName = mmd.getCatalog();
                    }
                    if (schemaName == null)
                    {
                        schemaName = mmd.getSchema();
                    }
                }
            }
        }
        else
        {
            // Check for a specified join table name (1-1/N-1 UNI join table case)
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
        if (catalogName != null)
        {
            catalogName = getIdentifierInAdapterCase(catalogName);
        }
        if (schemaName != null)
        {
            schemaName = getIdentifierInAdapterCase(schemaName);
        }

        // No user-specified name, so generate a default using the previously created fallback
        if (identifierName == null)
        {
            // Use this field as the basis for the fallback name (CLASS_FIELD) unless
            // this is a bidirectional and the other side is the owner (has mapped-by) in which case OTHERCLASS_FIELD
            String fieldNameBasis = mmd.getFullFieldName();
            if (relatedMmds != null && relatedMmds[0].getMappedBy() != null)
            {
                // Other field has mapped-by so use that
                fieldNameBasis = relatedMmds[0].getFullFieldName();
            }

            // Generate a fallback name, based on the class/field name (CLASS_FIELD)
            ArrayList name_parts = new ArrayList();
            StringTokenizer tokens = new StringTokenizer(fieldNameBasis, ".");
            while (tokens.hasMoreTokens())
            {
                name_parts.add(tokens.nextToken());
            }
            ListIterator li = name_parts.listIterator(name_parts.size());
            String unique_name = (String)li.previous();
            String full_name = (li.hasPrevious() ? (li.previous() + getWordSeparator()) : "") + unique_name;

            identifierName = "";
            if (tablePrefix != null && tablePrefix.length() > 0)
            {
                identifierName = tablePrefix;
            }

            identifierName += full_name;

            if (tableSuffix != null && tableSuffix.length() > 0)
            {
                identifierName += tableSuffix;
            }
        }

        // Generate the table identifier now that we have the identifier name
        DatastoreIdentifier identifier = newTableIdentifier(identifierName, catalogName, schemaName);
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

            identifierName = "";
            if (tablePrefix != null && tablePrefix.length() > 0)
            {
                identifierName = tablePrefix;
            }
            identifierName += unique_name;
            if (tableSuffix != null && tableSuffix.length() > 0)
            {
                identifierName += tableSuffix;
            }
        }
        if (catalogName != null)
        {
            catalogName = getIdentifierInAdapterCase(catalogName);
        }
        if (schemaName != null)
        {
            schemaName = getIdentifierInAdapterCase(schemaName);
        }

        // Generate the table identifier now that we have the identifier name
        DatastoreIdentifier identifier = newTableIdentifier(identifierName, catalogName, schemaName);
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
            AbstractClassMetaData implMetaData, DatastoreIdentifier implIdentifier, boolean embedded, FieldRole fieldRole)
    {
        DatastoreIdentifier identifier = null;
        String key = "[" + refMetaData.getFullFieldName() + "][" + implMetaData.getFullClassName() + "][" + implIdentifier.getName() + "]";
        identifier = references.get(key);
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
            String name = referenceName + "." + implementationName + "." + implIdentifier.getName();

            // Set the SQL identifier adding any truncation as necessary
            String suffix = getColumnIdentifierSuffix(fieldRole, embedded);
            String datastoreID = generateIdentifierNameForJavaName(name);
            String baseID = truncate(datastoreID, 
                dba.getDatastoreIdentifierMaxLength(IdentifierType.COLUMN) - suffix.length());
            identifier = new ColumnIdentifier(this, baseID + suffix);
            references.put(key, identifier);
        }
        return identifier;
    }

    /**
     * Method to generate a join-table identifier. The identifier could be for a foreign-key
     * to another table (if the destinationId is provided), or could be for a simple column
     * in the join table.
     * @param ownerFmd MetaData for the owner field
     * @param relatedFmd MetaData for the related field
     * @param destinationId Identifier for the identity field of the destination (if FK)
     * @param embedded Whether the identifier is for a field embedded
     * @param fieldRole The role to be performed by this column e.g FK, collection element ?
     * @return The identifier.
     */
    public DatastoreIdentifier newJoinTableFieldIdentifier(AbstractMemberMetaData ownerFmd, AbstractMemberMetaData relatedFmd,
            DatastoreIdentifier destinationId, boolean embedded, FieldRole fieldRole)
    {
        if (destinationId != null)
        {
            RelationType relType = ownerFmd.getRelationType(clr);
            if (relType == RelationType.MANY_TO_MANY_BI && ownerFmd.hasCollection() && ownerFmd.getMappedBy() != null)
            {
                // M-N join table at non-owner side : need to swap the terminations that are assigned by fieldRole
                if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT)
                {
                    fieldRole = FieldRole.ROLE_OWNER;
                }
                else if (fieldRole == FieldRole.ROLE_OWNER)
                {
                    fieldRole = FieldRole.ROLE_COLLECTION_ELEMENT;
                }
            }
            return newColumnIdentifier(destinationId.getName(), embedded, fieldRole, false);
        }

        String baseName = null;
        if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT)
        {
            String elementType = ownerFmd.getCollection().getElementType();
            baseName = elementType.substring(elementType.lastIndexOf('.') + 1);
        }
        else if (fieldRole == FieldRole.ROLE_ARRAY_ELEMENT)
        {
            String elementType = ownerFmd.getArray().getElementType();
            baseName = elementType.substring(elementType.lastIndexOf('.') + 1);
        }
        else if (fieldRole == FieldRole.ROLE_MAP_KEY)
        {
            String keyType = ownerFmd.getMap().getKeyType();
            baseName = keyType.substring(keyType.lastIndexOf('.') + 1);
        }
        else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
        {
            String valueType = ownerFmd.getMap().getValueType();
            baseName = valueType.substring(valueType.lastIndexOf('.') + 1);
        }
        else
        {
            baseName = "UNKNOWN";
        }
        return newColumnIdentifier(baseName, embedded, fieldRole, false);
    }

    /**
     * Method to generate a FK/FK-index field identifier. 
     * The identifier could be for the FK field itself, or for a related index for the FK.
     * @param ownerFmd MetaData for the owner field
     * @param relatedFmd MetaData for the related field
     * @param destinationId Identifier for the identity field of the destination table (if strict FK)
     * @param embedded Whether the identifier is for a field embedded
     * @param fieldRole The role to be performed by this column e.g owner, index ?
     * @return The identifier
     */
    public DatastoreIdentifier newForeignKeyFieldIdentifier(AbstractMemberMetaData ownerFmd, AbstractMemberMetaData relatedFmd,
            DatastoreIdentifier destinationId, boolean embedded, FieldRole fieldRole)
    {
        if (relatedFmd != null)
        {
            // Bidirectional
            if (fieldRole == FieldRole.ROLE_OWNER)
            {
                return newColumnIdentifier(relatedFmd.getName() + "." + destinationId.getName(), embedded, fieldRole, false);
            }
            else if (fieldRole == FieldRole.ROLE_INDEX)
            {
                return newColumnIdentifier(relatedFmd.getName() + "." + destinationId.getName(), embedded, fieldRole, false);
            }
            else
            {
                throw new NucleusException("Column role " + fieldRole + " not supported by this method").setFatal();
            }
        }

        if (fieldRole == FieldRole.ROLE_OWNER)
        {
            // FK field (FK collection/array/list/map)
            return newColumnIdentifier(ownerFmd.getName() + "." + destinationId.getName(), embedded, fieldRole, false);
        }
        else if (fieldRole == FieldRole.ROLE_INDEX)
        {
            // Order field for FK (FK list)
            return newColumnIdentifier(ownerFmd.getName() + "." + "INTEGER", embedded, fieldRole, false);
        }
        else
        {
            throw new NucleusException("Column role " + fieldRole + " not supported by this method").setFatal();
        }
    }

    /**
     * Method to return an identifier for a discriminator column.
     * Returns an identifier "DISCRIMINATOR"
     * @return The discriminator column identifier
     */
    public DatastoreIdentifier newDiscriminatorFieldIdentifier()
    {
        String name = "DISCRIMINATOR";
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
        String name = "OPT_VERSION";
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
        String name = "ADPT_PK_IDX";
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
     * @param mmd MetaData for the field/property - not used here
     * @return The index datastore field identifier
     */
    public DatastoreIdentifier newIndexFieldIdentifier(AbstractMemberMetaData mmd)
    {
        String name = "INTEGER_IDX";
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
     * each word to upper-case, and separating each one with a word separator.
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
        char prev = '\0';

        for (int i = 0; i < javaName.length(); ++i)
        {
            char c = javaName.charAt(i);

            if (c >= 'A' && c <= 'Z' &&
                (namingCase != NamingCase.MIXED_CASE && namingCase != NamingCase.MIXED_CASE_QUOTED))
            {
                if (prev >= 'a' && prev <= 'z')
                {
                    s.append(wordSeparator);
                }

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
                s.append(wordSeparator);
            }
            else
            {
                String cval = "000" + Integer.toHexString(c);

                s.append(cval.substring(cval.length() - (c > 0xff ? 4 : 2)));
            }

            prev = c;
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
    protected String getColumnIdentifierSuffix(FieldRole role, boolean embedded)
    {
        String suffix;

        if (role == FieldRole.ROLE_OWNER)
        {
            suffix = !embedded ? "_OID" : "_OWN";
        }
        else if (role == FieldRole.ROLE_FIELD || role == FieldRole.ROLE_COLLECTION_ELEMENT || role == FieldRole.ROLE_ARRAY_ELEMENT)
        {
            suffix = !embedded ? "_EID" : "_ELE";
        }
        else if (role == FieldRole.ROLE_MAP_KEY)
        {
            suffix = !embedded ? "_KID" : "_KEY";
        }
        else if (role == FieldRole.ROLE_MAP_VALUE)
        {
            suffix = !embedded ? "_VID" : "_VAL";
        }
        else if (role == FieldRole.ROLE_INDEX)
        {
            suffix = !embedded ? "_XID" : "_IDX";
        }
        else
        {
            suffix = !embedded ? "_ID" : "";
        }

        return suffix;
    }
}