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
2006 Andy Jefferson - changed to interface
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.identifier;

import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.naming.NamingCase;

/**
 * Factory that creates immutable instances of DatastoreIdentifier.
 * Identifiers are of a particular type. Each datastore could invent its own  particular types
 * as required, just that the ones here should be the principal types required.
 */
public interface IdentifierFactory
{
    public static final String PROPERTY_DEFAULT_CATALOG = "DefaultCatalog";
    public static final String PROPERTY_DEFAULT_SCHEMA = "DefaultSchema";
    public static final String PROPERTY_REQUIRED_CASE = "RequiredCase";
    public static final String PROPERTY_TABLE_PREFIX = "TablePrefix";
    public static final String PROPERTY_TABLE_SUFFIX = "TableSuffix";
    public static final String PROPERTY_WORD_SEPARATOR = "WordSeparator";
    public static final String PROPERTY_NAMING_FACTORY = "NamingFactory";

    /**
     * Accessor for the datastore adapter that we are creating identifiers for.
     * @return The datastore adapter
     */
    DatastoreAdapter getDatastoreAdapter();

    /**
     * Accessor for the identifier case being used.
     * @return The identifier case
     */
    NamingCase getNamingCase();

    /**
     * Accessor for an identifier for use in the datastore adapter
     * @param identifier The identifier name
     * @return Identifier name for use with the datastore adapter
     */
    String getIdentifierInAdapterCase(String identifier);

    /**
     * Accessor for an identifier for use in the datastore adapter, but omitting any quoting.
     * @param identifier The identifier name
     * @return Identifier name for use with the datastore adapter
     */
    String getIdentifierInAdapterCaseUnquoted(String identifier);

    /**
     * Method to truncate the provided identifier as required to the datastore adapter column length (if required)
     * @param identifier The identifier
     * @return The truncated variant (if the length was longer than the max column identifier length);
     */
    String getIdentifierTruncatedToAdapterColumnLength(String identifier);

    /**
     * To be called when we want an identifier name creating based on the
     * identifier. Creates identifier for COLUMN, FOREIGN KEY, INDEX and TABLE
     * @param identifierType the type of identifier to be created
     * @param identifierName The identifier name
     * @return The DatastoreIdentifier
     */
    DatastoreIdentifier newIdentifier(IdentifierType identifierType, String identifierName);

    /**
     * Method to use to generate an identifier for a table with the supplied name in the default catalog/schema.
     * The passed name will not be changed (other than in its case) although it may
     * be truncated to fit the maximum length permitted for a table identifier.
     * @param identifierName The identifier name
     * @return The DatastoreIdentifier for the table
     */
    DatastoreIdentifier newTableIdentifier(String identifierName);

    /**
     * Method to use to generate an identifier for a table with the supplied name.
     * The passed name will not be changed (other than in its case) although it may
     * be truncated to fit the maximum length permitted for a table identifier.
     * @param identifierName The identifier name for the table
     * @param catalogName Optional catalog name
     * @param schemaName Optional schema name
     * @return The DatastoreIdentifier for the table
     */
    DatastoreIdentifier newTableIdentifier(String identifierName, String catalogName, String schemaName);

    /**
     * Method to return a Table identifier for the specified class.
     * @param md Meta data for the class
     * @return The identifier for the table
     */
    DatastoreIdentifier newTableIdentifier(AbstractClassMetaData md);

    /**
     * Method to return a Table identifier for the specified field.
     * @param fmd Meta data for the field
     * @return The identifier for the table
     */
    DatastoreIdentifier newTableIdentifier(AbstractMemberMetaData fmd);

    /**
     * Method to use to generate an identifier for a column with the supplied name.
     * The passed name will not be changed (other than in its case) although it may
     * be truncated to fit the maximum length permitted for a column identifier.
     * @param identifierName The identifier name
     * @return The DatastoreIdentifier
     */
    DatastoreIdentifier newColumnIdentifier(String identifierName);

    /**
     * Method to create an identifier for a column where we want the
     * name based on the supplied java name, and the field has a particular
     * role (and so could have its naming set according to the role).
     * @param javaName The java field name
     * @param embedded Whether the identifier is for a field embedded
     * @param fieldRole The role to be performed by this column e.g FK, Index ?
     * @param custom Whether this has a user-defined name
     * @return The DatastoreIdentifier
     */
    DatastoreIdentifier newColumnIdentifier(String javaName, boolean embedded, FieldRole fieldRole, boolean custom);

    /**
     * Method to generate an identifier name for reference field, based on the metadata for the
     * field, and the ClassMetaData for the implementation.
     * @param refMetaData the MetaData for the reference field
     * @param implMetaData the AbstractClassMetaData for this implementation
     * @param implIdentifier PK identifier for the implementation
     * @param embedded Whether the identifier is for a field embedded
     * @param fieldRole The role to be performed by this column e.g FK, collection element ?
     * @return The DatastoreIdentifier
     */
    DatastoreIdentifier newReferenceFieldIdentifier(AbstractMemberMetaData refMetaData, 
            AbstractClassMetaData implMetaData, DatastoreIdentifier implIdentifier, boolean embedded, FieldRole fieldRole);

    /**
     * Method to return an identifier for a discriminator column.
     * @return The discriminator column identifier
     */
    DatastoreIdentifier newDiscriminatorFieldIdentifier();

    /**
     * Method to return an identifier for a version column.
     * @return The version column identifier
     */
    DatastoreIdentifier newVersionFieldIdentifier();

    /**
     * Method to return a new Identifier based on the passed identifier, but adding on the passed suffix
     * @param identifier The current identifier
     * @param suffix The suffix
     * @return The new identifier
     */
    DatastoreIdentifier newIdentifier(DatastoreIdentifier identifier, String suffix);

    // RDBMS types of identifiers

    /**
     * Method to generate a join-table identifier. The identifier could be for a foreign-key
     * to another table (if the destinationId is provided), or could be for a simple column
     * in the join table.
     * @param ownerFmd MetaData for the owner field
     * @param relatedFmd MetaData for the related field (if bidirectional)
     * @param destinationId Identifier for the identity field of the destination table
     * @param embedded Whether the identifier is for a field embedded
     * @param fieldRole The role to be performed by this column e.g FK, collection element ?
     * @return The identifier.
     */
    DatastoreIdentifier newJoinTableFieldIdentifier(AbstractMemberMetaData ownerFmd, AbstractMemberMetaData relatedFmd,
            DatastoreIdentifier destinationId, boolean embedded, FieldRole fieldRole);

    /**
     * Method to generate a FK/FK-index field identifier. 
     * The identifier could be for the FK field itself, or for a related index for the FK.
     * @param ownerFmd MetaData for the owner field
     * @param relatedFmd MetaData for the related field (if bidirectional)
     * @param destinationId Identifier for the identity field of the destination table (if strict FK)
     * @param embedded Whether the identifier is for a field embedded
     * @param fieldRole The role to be performed by this column e.g owner, index ?
     * @return The identifier
     */
    DatastoreIdentifier newForeignKeyFieldIdentifier(AbstractMemberMetaData ownerFmd, AbstractMemberMetaData relatedFmd,
            DatastoreIdentifier destinationId, boolean embedded, FieldRole fieldRole);

    /**
     * Method to return an identifier for an index (ordering) column.
     * @param mmd Metadata for the field/property that we require to add an index(order) column for
     * @return The index column identifier
     */
    DatastoreIdentifier newIndexFieldIdentifier(AbstractMemberMetaData mmd);

    /**
     * Method to return an identifier for an adapter index column.
     * An "adapter index" is a column added to be part of a primary key when some other
     * column cant perform that role.
     * @return The index column identifier
     */
    DatastoreIdentifier newAdapterIndexFieldIdentifier();

    /**
     * Method to generate an identifier for a sequence using the passed name.
     * @param sequenceName the name of the sequence to use
     * @return The DatastoreIdentifier
     */
    DatastoreIdentifier newSequenceIdentifier(String sequenceName);

    /**
     * Method to generate an identifier for a primary key.
     * @param table the table
     * @return The DatastoreIdentifier
     */
    DatastoreIdentifier newPrimaryKeyIdentifier(Table table);

    /**
     * Method to generate an identifier for an index.
     * @param table the table
     * @param isUnique if the index is unique
     * @param seq the sequential number
     * @return The DatastoreIdentifier
     */
    DatastoreIdentifier newIndexIdentifier(Table table, boolean isUnique, int seq);

    /**
     * Method to generate an identifier for a candidate key.
     * @param table the table
     * @param seq Sequence number
     * @return The DatastoreIdentifier
     */
    DatastoreIdentifier newCandidateKeyIdentifier(Table table, int seq);

    /**
     * Method to create an identifier for a foreign key.
     * @param table the table
     * @param seq the sequential number
     * @return The DatastoreIdentifier
     */
    DatastoreIdentifier newForeignKeyIdentifier(Table table, int seq);
}