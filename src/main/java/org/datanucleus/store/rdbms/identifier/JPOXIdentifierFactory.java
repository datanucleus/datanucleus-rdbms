package org.datanucleus.store.rdbms.identifier;

import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;

public class JPOXIdentifierFactory extends DNIdentifierFactory
{

    public JPOXIdentifierFactory(DatastoreAdapter dba, ClassLoaderResolver clr, Map props)
    {
        super(dba, clr, props);
        // TODO Auto-generated constructor stub
    }

    /** Range to use for creating hased ending when truncating identifiers. */
    private static final int HASH_RANGE = Character.MAX_RADIX * Character.MAX_RADIX / 2;
    
    /**
     * Method to truncate an identifier to fit within the specified identifier length.
     * If truncation is necessary will use a 2 char hashcode (at the end) to attempt to create uniqueness.
     * @param identifier The identifier
     * @param length The (max) length to use
     * @return The truncated identifier.
     */
    protected String truncate(String identifier, int length)
    {
        if (identifier.length() > length)
        {
            // Truncation is necessary so cut down to "maxlength-2" and add 2 char hashcode
            int tailIndex = length - 2;
            int tailHash = identifier.substring(tailIndex).hashCode();

            // Scale the hash code down to the range 0 - 1295
            if (tailHash < 0)
            {
                tailHash = tailHash % HASH_RANGE + (HASH_RANGE - 1);
            }
            else
            {
                tailHash = tailHash % HASH_RANGE + HASH_RANGE;
            }

            String suffix = "0" + Integer.toString(tailHash, Character.MAX_RADIX);

            return identifier.substring(0, tailIndex) + suffix.substring(suffix.length() - 2);
        }
        return identifier;
    }
}
