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
import java.util.WeakHashMap;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.exceptions.TooManyForeignKeysException;
import org.datanucleus.store.rdbms.exceptions.TooManyIndicesException;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.store.schema.naming.NamingCase;
import org.datanucleus.store.schema.naming.NamingFactory;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Abstract representation of an identifier factory for ORM datastores.
 * To be extended to generate the identifiers.
 */
public abstract class AbstractIdentifierFactory implements IdentifierFactory
{
    public static final int CASE_PRESERVE = 1;
    public static final int CASE_UPPER = 2;
    public static final int CASE_LOWER = 3;

    /** The number of characters used to build the hash. */
    private static final int HASH_LENGTH = 4;

    /**
     * Range to use for creating hashed ending when truncating identifiers. The actual hashes have a value
     * between 0 and <code>HASH_RANGE</code> - 1.
     */
    private static final int HASH_RANGE = calculateHashMax();
    private static final int calculateHashMax()
    {
        int hm = 1;
        for (int i = 0; i < HASH_LENGTH; ++i)
        {
            hm *= Character.MAX_RADIX;
        }

        return hm;
    }

    // TODO Make use of this in namings
    protected NamingFactory namingFactory;

    protected DatastoreAdapter dba;

    protected ClassLoaderResolver clr;

    /** Case to use for identifiers. */
    protected NamingCase namingCase;

    protected String quoteString;

    /** Separator to use for words in the identifiers. */
    protected String wordSeparator = "_";

    protected Map<String, DatastoreIdentifier> tables = new WeakHashMap();
    protected Map<String, DatastoreIdentifier> columns = new WeakHashMap();
    protected Map<String, DatastoreIdentifier> foreignkeys = new WeakHashMap();
    protected Map<String, DatastoreIdentifier> indexes = new WeakHashMap();
    protected Map<String, DatastoreIdentifier> candidates = new WeakHashMap();
    protected Map<String, DatastoreIdentifier> primarykeys = new WeakHashMap();
    protected Map<String, DatastoreIdentifier> sequences = new WeakHashMap();
    protected Map<String, DatastoreIdentifier> references = new WeakHashMap();

    /** Default catalog name for any created identifiers. */
    protected String defaultCatalogName = null;

    /** Default schema name for any created identifiers. */
    protected String defaultSchemaName = null;

    /**
     * Constructor.
     * The properties accepted are
     * <ul>
     * <li>RequiredCase : what case the identifiers should be in</li>
     * <li>DefaultCatalog : default catalog to use (if any)</li>
     * <li>DefaultSchema : default schema to use (if any)</li>
     * </ul>
     * @param dba Database adapter
     * @param clr ClassLoader resolver
     * @param props Any properties controlling identifier generation
     */
    public AbstractIdentifierFactory(DatastoreAdapter dba, ClassLoaderResolver clr, Map props)
    {
        this.dba = dba;
        this.clr = clr;
        this.quoteString = dba.getIdentifierQuoteString();

        // Set the identifier case to be used based on what the user has requested and what the datastore supports
        int userIdentifierCase = CASE_UPPER;
        if (props.containsKey("RequiredCase"))
        {
            String requiredCase = (String)props.get("RequiredCase");
            if (requiredCase.equalsIgnoreCase("UPPERCASE"))
            {
                userIdentifierCase = CASE_UPPER;
            }
            else if (requiredCase.equalsIgnoreCase("lowercase"))
            {
                userIdentifierCase = CASE_LOWER;
            }
            else if (requiredCase.equalsIgnoreCase("MixedCase"))
            {
                userIdentifierCase = CASE_PRESERVE;
            }
        }

        if (userIdentifierCase == CASE_UPPER)
        {
            if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_UPPERCASE))
            {
                namingCase = NamingCase.UPPER_CASE;
            }
            else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_UPPERCASE_QUOTED))
            {
                namingCase = NamingCase.UPPER_CASE_QUOTED;
            }
            else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE) ||
                dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_SENSITIVE))
            {
                namingCase = NamingCase.UPPER_CASE;
            }
            else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_QUOTED) ||
                dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_QUOTED_SENSITIVE))
            {
                namingCase = NamingCase.UPPER_CASE_QUOTED;
            }
            else
            {
                if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_LOWERCASE))
                {
                    namingCase = NamingCase.LOWER_CASE;
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("039001","UPPERCASE", "LOWERCASE"));
                }
                else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_LOWERCASE_QUOTED))
                {
                    namingCase = NamingCase.LOWER_CASE_QUOTED;
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("039001","UPPERCASE", "LOWERCASEQUOTED"));
                }
                else
                {
                    // Should never happen since we've tried all options
                    throw new NucleusUserException(Localiser.msg("039002", "UPPERCASE")).setFatal();
                }
            }
        }
        else if (userIdentifierCase == CASE_LOWER)
        {
            if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_LOWERCASE))
            {
                namingCase = NamingCase.LOWER_CASE;
            }
            else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_LOWERCASE_QUOTED))
            {
                namingCase = NamingCase.LOWER_CASE_QUOTED;
            }
            else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE) ||
                dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_SENSITIVE))
            {
                namingCase = NamingCase.LOWER_CASE;
            }
            else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_QUOTED) ||
                dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_QUOTED_SENSITIVE))
            {
                namingCase = NamingCase.LOWER_CASE_QUOTED;
            }
            else
            {
                if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_UPPERCASE))
                {
                    namingCase = NamingCase.UPPER_CASE;
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("039001","LOWERCASE", "UPPERCASE"));
                }
                else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_UPPERCASE_QUOTED))
                {
                    namingCase = NamingCase.UPPER_CASE_QUOTED;
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("039001","LOWERCASE", "UPPERCASEQUOTED"));
                }
                else
                {
                    // Should never happen since we've tried all options
                    throw new NucleusUserException(Localiser.msg("039002", "LOWERCASE")).setFatal();
                }
            }
        }
        else if (userIdentifierCase == CASE_PRESERVE)
        {
            if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE) ||
                dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_SENSITIVE))
            {
                namingCase = NamingCase.MIXED_CASE;
            }
            else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_QUOTED) ||
                dba.supportsOption(DatastoreAdapter.IDENTIFIERS_MIXEDCASE_QUOTED_SENSITIVE))
            {
                namingCase = NamingCase.MIXED_CASE_QUOTED;
            }
            else
            {
                if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_LOWERCASE))
                {
                    namingCase = NamingCase.LOWER_CASE;
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("039001","MIXEDCASE", "LOWERCASE"));
                }
                else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_LOWERCASE_QUOTED))
                {
                    namingCase = NamingCase.LOWER_CASE_QUOTED;
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("039001", "MIXEDCASE", "LOWERCASEQUOTED"));
                }
                else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_UPPERCASE))
                {
                    namingCase = NamingCase.UPPER_CASE;
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("039001", "MIXEDCASE", "UPPERCASE"));
                }
                else if (dba.supportsOption(DatastoreAdapter.IDENTIFIERS_UPPERCASE_QUOTED))
                {
                    namingCase = NamingCase.UPPER_CASE_QUOTED;
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("039001", "MIXEDCASE", "UPPERCASEQUOTED"));
                }
                else
                {
                    // Should never happen since we've tried all options
                    throw new NucleusUserException(Localiser.msg("039002", "MIXEDCASE")).setFatal();
                }
            }
        }
        else
        {
            // Case not supported
            throw new NucleusUserException(Localiser.msg("039000", userIdentifierCase)).setFatal();
        }

        // Save the default catalog/schema - in a valid case (may be user input)
        if (props.containsKey("DefaultCatalog"))
        {
            this.defaultCatalogName = getIdentifierInAdapterCase((String)props.get("DefaultCatalog"));
        }
        if (props.containsKey("DefaultSchema"))
        {
            this.defaultSchemaName = getIdentifierInAdapterCase((String)props.get("DefaultSchema"));
        }
        if (props.containsKey("NamingFactory"))
        {
            this.namingFactory = (NamingFactory)props.get("NamingFactory");
        }
    }

    /**
     * Accessor for the datastore adapter that we are creating identifiers for.
     * @return The datastore adapter
     */
    public DatastoreAdapter getDatastoreAdapter()
    {
        return dba;
    }

    /**
     * Accessor for the identifier case being used.
     * @return The identifier case
     */
    public NamingCase getNamingCase()
    {
        return namingCase;
    }

    /**
     * Accessor for the word separator for identifiers.
     * @return The word separator
     */
    public String getWordSeparator()
    {
        return wordSeparator;
    }

    /**
     * Method to truncate an identifier to fit within the specified identifier length.
     * If truncation is necessary will use a 4 char hashcode (defined by {@link #HASH_LENGTH}) (at the end) to attempt to create uniqueness.
     * @param identifier The identifier
     * @param length The (max) length to use
     * @return The truncated identifier.
     */
    protected String truncate(String identifier, int length)
    {
        // return namingFactory.truncate(identifier, length);
        if (length < 0) // Special case of no truncation
        {
            return identifier;
        }
        if (identifier.length() > length)
        {
            if (length < HASH_LENGTH)
                throw new IllegalArgumentException("The length argument (=" + length + ") is less than HASH_LENGTH(=" + HASH_LENGTH + ")!");

            // Truncation is necessary so cut down to "maxlength-HASH_LENGTH" and add HASH_LENGTH chars hashcode
            int tailIndex = length - HASH_LENGTH;
            int tailHash = identifier.hashCode();

            // We have to scale down the hash anyway, so we can simply ignore the sign
            if (tailHash < 0)
                tailHash *= -1;

            // Scale the hash code down to the range 0 ... (HASH_RANGE - 1)
            tailHash %= HASH_RANGE;

            String suffix = Integer.toString(tailHash, Character.MAX_RADIX);
            if (suffix.length() > HASH_LENGTH)
                throw new IllegalStateException("Calculated hash \"" + suffix + "\" has more characters than defined by HASH_LENGTH (=" + HASH_LENGTH + ")! This should never happen!");

            // we add prefix "0", if it's necessary
            if (suffix.length() < HASH_LENGTH)
            {
                StringBuilder sb = new StringBuilder(HASH_LENGTH);
                sb.append(suffix);
                while (sb.length() < HASH_LENGTH)
                    sb.insert(0, '0');

                suffix = sb.toString();
            }

            return identifier.substring(0, tailIndex) + suffix;
        }
        else
        {
            return identifier;
        }
    }

    /**
     * Convenience method to convert the passed identifier into an identifier
     * in the correct case, and with any required quoting for the datastore adapter.
     * If the identifier is already quoted and needs quotes then none are added.
     * @param identifier The identifier
     * @return The updated identifier in the correct case
     */
    public String getIdentifierInAdapterCase(String identifier)
    {
        // return namingFactory.getNameInRequiredCase(identifier);
        if (identifier == null)
        {
            return null;
        }
        StringBuilder id = new StringBuilder();
        if (namingCase == NamingCase.LOWER_CASE_QUOTED ||
            namingCase == NamingCase.MIXED_CASE_QUOTED ||
            namingCase == NamingCase.UPPER_CASE_QUOTED)
        {
            if (!identifier.startsWith(quoteString))
            {
                id.append(quoteString);
            }
        }

        if (namingCase == NamingCase.LOWER_CASE || namingCase == NamingCase.LOWER_CASE_QUOTED)
        {
            id.append(identifier.toLowerCase());
        }
        else if (namingCase == NamingCase.UPPER_CASE || namingCase == NamingCase.UPPER_CASE_QUOTED)
        {
            id.append(identifier.toUpperCase());
        }
        else
        {
            id.append(identifier);
        }

        if (namingCase == NamingCase.LOWER_CASE_QUOTED ||
            namingCase == NamingCase.MIXED_CASE_QUOTED ||
            namingCase == NamingCase.UPPER_CASE_QUOTED)
        {
            if (!identifier.endsWith(quoteString))
            {
                id.append(quoteString);
            }
        }
        return id.toString();
    }

    /**
     * Method to generate an identifier based on the supplied name for the requested type of identifier.
     * @param identifierType the type of identifier to be created
     * @param name The Java or SQL identifier name
     * @return The DatastoreIdentifier
     */
    public DatastoreIdentifier newIdentifier(IdentifierType identifierType, String name)
    {
        DatastoreIdentifier identifier = null;
        String key = name.replace(quoteString, ""); // Remove any user/JDBC supplied quotes
        if (identifierType == IdentifierType.TABLE)
        {
            identifier = tables.get(key);
            if (identifier == null)
            {
                String sqlIdentifier = generateIdentifierNameForJavaName(key);
                sqlIdentifier = truncate(sqlIdentifier, dba.getDatastoreIdentifierMaxLength(identifierType));
                identifier = new TableIdentifier(this, sqlIdentifier);
                setCatalogSchemaForTable((TableIdentifier)identifier);
                tables.put(key, identifier);
            }
        }
        else if (identifierType == IdentifierType.COLUMN)
        {
            identifier = columns.get(key);
            if (identifier == null)
            {
                String sqlIdentifier = generateIdentifierNameForJavaName(key);
                sqlIdentifier = truncate(sqlIdentifier, dba.getDatastoreIdentifierMaxLength(identifierType));
                identifier = new ColumnIdentifier(this, sqlIdentifier);
                columns.put(key, identifier);
            }
        }
        else if (identifierType == IdentifierType.FOREIGN_KEY)
        {
            identifier = foreignkeys.get(key);
            if (identifier == null)
            {
                String sqlIdentifier = generateIdentifierNameForJavaName(key);
                sqlIdentifier = truncate(sqlIdentifier, dba.getDatastoreIdentifierMaxLength(identifierType));
                identifier = new ForeignKeyIdentifier(this, sqlIdentifier);
                foreignkeys.put(key, identifier);
            }
        }
        else if (identifierType == IdentifierType.INDEX)
        {
            identifier = indexes.get(key);
            if (identifier == null)
            {
                String sqlIdentifier = generateIdentifierNameForJavaName(key);
                sqlIdentifier = truncate(sqlIdentifier, dba.getDatastoreIdentifierMaxLength(identifierType));
                identifier = new IndexIdentifier(this, sqlIdentifier);
                indexes.put(key, identifier);
            }
        }
        else if (identifierType == IdentifierType.CANDIDATE_KEY)
        {
            identifier = candidates.get(key);
            if (identifier == null)
            {
                String sqlIdentifier = generateIdentifierNameForJavaName(key);
                sqlIdentifier = truncate(sqlIdentifier, dba.getDatastoreIdentifierMaxLength(identifierType));
                identifier = new CandidateKeyIdentifier(this, sqlIdentifier);
                candidates.put(key, identifier);
            }
        }
        else if (identifierType == IdentifierType.PRIMARY_KEY)
        {
            identifier = primarykeys.get(key);
            if (identifier == null)
            {
                String sqlIdentifier = generateIdentifierNameForJavaName(key);
                sqlIdentifier = truncate(sqlIdentifier, dba.getDatastoreIdentifierMaxLength(identifierType));
                identifier = new PrimaryKeyIdentifier(this, sqlIdentifier);
                primarykeys.put(key, identifier);
            }
        }
        else if (identifierType == IdentifierType.SEQUENCE)
        {
            identifier = sequences.get(key);
            if (identifier == null)
            {
                String sqlIdentifier = generateIdentifierNameForJavaName(key);
                sqlIdentifier = truncate(sqlIdentifier, dba.getDatastoreIdentifierMaxLength(identifierType));
                identifier = new SequenceIdentifier(this, sqlIdentifier);
                sequences.put(key, identifier);
            }
        }
        else
        {
            throw new NucleusException("identifier type " + identifierType + " not supported by this factory method").setFatal();
        }
        return identifier;
    }

    /**
     * Method to return a new Identifier based on the passed identifier, but adding on the passed suffix
     * @param identifier The current identifier
     * @param suffix The suffix
     * @return The new identifier
     */
    public DatastoreIdentifier newIdentifier(DatastoreIdentifier identifier, String suffix)
    {
        String newId = identifier.getIdentifierName() + getWordSeparator() + suffix;
        if (identifier instanceof TableIdentifier)
        {
            newId = truncate(newId, dba.getDatastoreIdentifierMaxLength(IdentifierType.TABLE));
            TableIdentifier tableIdentifier = new TableIdentifier(this, newId);
            setCatalogSchemaForTable(tableIdentifier);
            return tableIdentifier;
        }
        else if (identifier instanceof ColumnIdentifier)
        {
            newId = truncate(newId, dba.getDatastoreIdentifierMaxLength(IdentifierType.COLUMN));
            return new ColumnIdentifier(this, newId);
        }
        else if (identifier instanceof ForeignKeyIdentifier)
        {
            newId = truncate(newId, dba.getDatastoreIdentifierMaxLength(IdentifierType.FOREIGN_KEY));
            return new ForeignKeyIdentifier(this, newId);
        }
        else if (identifier instanceof IndexIdentifier)
        {
            newId = truncate(newId, dba.getDatastoreIdentifierMaxLength(IdentifierType.INDEX));
            return new IndexIdentifier(this, newId);
        }
        else if (identifier instanceof CandidateKeyIdentifier)
        {
            newId = truncate(newId, dba.getDatastoreIdentifierMaxLength(IdentifierType.CANDIDATE_KEY));
            return new CandidateKeyIdentifier(this, newId);
        }
        else if (identifier instanceof PrimaryKeyIdentifier)
        {
            newId = truncate(newId, dba.getDatastoreIdentifierMaxLength(IdentifierType.PRIMARY_KEY));
            return new PrimaryKeyIdentifier(this, newId);
        }
        else if (identifier instanceof SequenceIdentifier)
        {
            newId = truncate(newId, dba.getDatastoreIdentifierMaxLength(IdentifierType.SEQUENCE));
            return new SequenceIdentifier(this, newId);
        }
        return null;
    }

    /**
     * Method to use to generate an identifier for a datastore field.
     * The passed name will not be changed (other than in its case) although it may
     * be truncated to fit the maximum length permitted for a datastore field identifier.
     * @param identifierName The identifier name
     * @return The DatastoreIdentifier for the table
     */
    public DatastoreIdentifier newTableIdentifier(String identifierName)
    {
        String key = identifierName.replace(quoteString, ""); // Allow for quotes on input name
        DatastoreIdentifier identifier = tables.get(key);
        if (identifier == null)
        {
            String baseID = truncate(key, dba.getDatastoreIdentifierMaxLength(IdentifierType.TABLE));
            identifier = new TableIdentifier(this, baseID);
            setCatalogSchemaForTable((TableIdentifier)identifier);
            tables.put(key, identifier);
        }
        return identifier;
    }

    /**
     * Method to use to generate an identifier for a datastore field.
     * The passed name will not be changed (other than in its case) although it may
     * be truncated to fit the maximum length permitted for a datastore field identifier.
     * @param identifierName The identifier name
     * @return The DatastoreIdentifier
     */
    public DatastoreIdentifier newColumnIdentifier(String identifierName)
    {
        String key = identifierName.replace(quoteString, ""); // Allow for quotes on input names
        DatastoreIdentifier identifier = columns.get(key);
        if (identifier == null)
        {
            String baseID = truncate(key, dba.getDatastoreIdentifierMaxLength(IdentifierType.COLUMN));
            identifier = new ColumnIdentifier(this, baseID);
            columns.put(key, identifier);
        }
        return identifier;
    }

    /**
     * Method to create an identifier for a datastore field where we want the
     * name based on the supplied java name, and the field has a particular
     * role (and so could have its naming set according to the role).
     * @param javaName The java field name
     * @param embedded Whether the identifier is for a field embedded
     * @param fieldRole The role to be performed by this column e.g FK, Index ?
     * @return The DatastoreIdentifier
     */
    public DatastoreIdentifier newColumnIdentifier(String javaName, boolean embedded, int fieldRole)
    {
        DatastoreIdentifier identifier = null;
        String key = "[" + (javaName == null ? "" : javaName) + "][" + embedded + "][" + fieldRole; // TODO Change this to a string form of fieldRole
        identifier = columns.get(key);
        if (identifier == null)
        {
            if (fieldRole == FieldRole.ROLE_CUSTOM)
            {
                // If the user has provided a name (CUSTOM) so dont need to generate it and dont need a suffix
                String baseID = truncate(javaName, dba.getDatastoreIdentifierMaxLength(IdentifierType.COLUMN));
                identifier = new ColumnIdentifier(this, baseID);
            }
            else
            {
                String suffix = getColumnIdentifierSuffix(fieldRole, embedded);
                String datastoreID = generateIdentifierNameForJavaName(javaName);
                String baseID = truncate(datastoreID, dba.getDatastoreIdentifierMaxLength(IdentifierType.COLUMN) - suffix.length());
                identifier = new ColumnIdentifier(this, baseID + suffix);
            }
            columns.put(key, identifier);
        }
        return identifier;
    }

    /**
     * Method to generate an identifier for a sequence using the passed name.
     * @param sequenceName the name of the sequence to use
     * @return The DatastoreIdentifier
     */
    public DatastoreIdentifier newSequenceIdentifier(String sequenceName)
    {
        String key = sequenceName;
        DatastoreIdentifier identifier = sequences.get(key);
        if (identifier == null)
        {
            String baseID = truncate(sequenceName, dba.getDatastoreIdentifierMaxLength(IdentifierType.SEQUENCE));
            identifier = new ColumnIdentifier(this, baseID);
            sequences.put(key, identifier);
        }
        return identifier;
    }

    /**
     * Method to generate an identifier for a primary key for the supplied table.
     * @param table the table
     * @return The DatastoreIdentifier
     */
    public DatastoreIdentifier newPrimaryKeyIdentifier(Table table)
    {
        DatastoreIdentifier identifier = null;
        String key = table.getIdentifier().toString();
        identifier = primarykeys.get(key);
        if (identifier == null)
        {
            String suffix = getWordSeparator() + "PK";
            int maxLength = dba.getDatastoreIdentifierMaxLength(IdentifierType.PRIMARY_KEY);
            String baseID = truncate(table.getIdentifier().getIdentifierName(), maxLength - suffix.length());
            identifier = new PrimaryKeyIdentifier(this, baseID + suffix);
            primarykeys.put(key, identifier);
        }
        return identifier;
    }

    /**
     * Method to generate an identifier for a candidate key in the supplied table.
     * @param table the table
     * @param seq the sequential number
     * @return The DatastoreIdentifier
     */
    public DatastoreIdentifier newCandidateKeyIdentifier(Table table, int seq)
    {
        DatastoreIdentifier identifier = null;
        String key = "[" + table.getIdentifier().toString() + "][" + seq + "]";
        identifier = candidates.get(key);
        if (identifier == null)
        {
            String suffix = getWordSeparator() + "U" + seq;
            int maxLength = dba.getDatastoreIdentifierMaxLength(IdentifierType.CANDIDATE_KEY);
            String baseID = truncate(table.getIdentifier().getIdentifierName(), maxLength - suffix.length());
            identifier = new CandidateKeyIdentifier(this, baseID + suffix);
            candidates.put(key, identifier);
        }
        return identifier;
    }

    /**
     * Method to create a new identifier for a foreign key in the supplied table.
     * @param table the table
     * @param seq the sequential number
     * @return The DatastoreIdentifier
     */
    public DatastoreIdentifier newForeignKeyIdentifier(Table table, int seq)
    {
        DatastoreIdentifier identifier = null;
        String key = "[" + table.getIdentifier().toString() + "][" + seq + "]";
        identifier = foreignkeys.get(key);
        if (identifier == null)
        {
            String suffix = getWordSeparator() + "FK";
            if (seq < 10)
            {
                suffix += "" + (char)('0' + seq);
            }
            else if (seq < dba.getMaxForeignKeys())
            {
                suffix += Integer.toHexString('A' + seq);
            }
            else
            {
                throw new TooManyForeignKeysException(dba, table.toString());
            }
            int maxLength = dba.getDatastoreIdentifierMaxLength(IdentifierType.FOREIGN_KEY);
            String baseID = truncate(table.getIdentifier().getIdentifierName(), maxLength - suffix.length());
            identifier = new ForeignKeyIdentifier(this, baseID + suffix);
            foreignkeys.put(key, identifier);
        }
        return identifier;
    }

    /**
     * Method to create an identifier for an Index in the supplied table.
     * @param table the table
     * @param isUnique if the index is unique
     * @param seq the sequential number
     * @return The DatastoreIdentifier
     */
    public DatastoreIdentifier newIndexIdentifier(Table table, boolean isUnique, int seq)
    {
        DatastoreIdentifier identifier = null;
        String key = "[" + table.getIdentifier().toString() + "][" + isUnique + "][" + seq + "]";
        identifier = indexes.get(key);
        if (identifier == null)
        {
            String suffix = getWordSeparator() + (isUnique ? "U" : "N");
            if (seq < dba.getMaxIndexes())
            {
                suffix += String.valueOf('0' + seq);
            }
            else
            {
                throw new TooManyIndicesException(dba, table.toString());
            }
            int maxLength = dba.getDatastoreIdentifierMaxLength(IdentifierType.INDEX);
            String baseID = truncate(table.getIdentifier().getIdentifierName(), maxLength - suffix.length());
            identifier = new IndexIdentifier(this, baseID + suffix);
            indexes.put(key, identifier);
        }
        return identifier;
    }

    /**
     * Accessor for the suffix to add to any column identifier, based on the role type.
     * @param role Datastore field role
     * @param embedded Whether the column is stored embedded
     * @return The suffix (e.g _ID for id columns).
     **/
    protected abstract String getColumnIdentifierSuffix(int role, boolean embedded);

    /**
     * Generate a datastore identifier from a Java identifier.
     * Embodies the naming rules for the factory.
     * @param javaName the Java identifier.
     * @return The datastore identifier
     */
    protected abstract String generateIdentifierNameForJavaName(String javaName);

    /**
     * Convenience method to set the catalog/schema on the passed TableIdentifier.
     * @param identifier The TableIdentifier
     */
    protected void setCatalogSchemaForTable(TableIdentifier identifier)
    {
        String catalogName = identifier.getCatalogName();
        String schemaName = identifier.getSchemaName();
        if (schemaName == null && catalogName == null)
        {
            // Still no values, so try the PMF settings.
            if (dba.supportsOption(DatastoreAdapter.CATALOGS_IN_TABLE_DEFINITIONS))
            {
                identifier.setCatalogName(this.defaultCatalogName);
            }
            if (dba.supportsOption(DatastoreAdapter.SCHEMAS_IN_TABLE_DEFINITIONS))
            {
                identifier.setSchemaName(this.defaultSchemaName);
            }
        }
    }

    /**
     * Convenience method to split a fully-specified identifier name (inc catalog/schema)
     * into its constituent parts. Returns a String array with 3 elements. The first is the
     * catalog, second the schema, and third the identifier.
     * @param name Name
     * @return The parts
     */
    protected String[] getIdentifierNamePartsFromName(String name)
    {
        if (name != null)
        {
            String[] names = new String[3];
            if (name.indexOf('.') < 0)
            {
                names[0] = null;
                names[1] = null;
                names[2] = name;
            }
            else
            {
                String[] specifiedNameParts = StringUtils.split(name, ".");
                int currentPartIndex = specifiedNameParts.length-1;
                names[2] = specifiedNameParts[currentPartIndex--];
                if (dba.supportsOption(DatastoreAdapter.SCHEMAS_IN_TABLE_DEFINITIONS) && currentPartIndex >= 0)
                {
                    names[1] = specifiedNameParts[currentPartIndex--];
                }
                if (dba.supportsOption(DatastoreAdapter.CATALOGS_IN_TABLE_DEFINITIONS) && currentPartIndex >= 0)
                {
                    names[0] = specifiedNameParts[currentPartIndex--];
                }
            }
            return names;
        }
        return null;
    }
}