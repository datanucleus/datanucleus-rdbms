/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved.
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
import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;

/**
 * Factory that creates immutable instances of DatastoreIdentifier for mapped datastores.
 * Provides the "JPOX" naming strategy like with DNIdentifierFactory except this uses a consistent hashing process to match JPOX.
 */
public class JPOXIdentifierFactory extends DNIdentifierFactory
{
    public JPOXIdentifierFactory(DatastoreAdapter dba, ClassLoaderResolver clr, Map props)
    {
        super(dba, clr, props);
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
