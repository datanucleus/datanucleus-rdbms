/**********************************************************************
Copyright (c) 2002 Mike Martin and others. All rights reserved.
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
    Andy Jefferson - coding standards
    Erik Bengtson - renamed to NoDatastoreMappingException
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.exceptions;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.util.Localiser;

/**
 * A <tt>NoDatastoreMappingException</tt> is thrown if an operation is performed that
 * assumes that a particular persistent field is stored in a single datastore
 * field when it is not (such as if the field is a Collection or a Map).
 */
public class NoDatastoreMappingException extends NucleusUserException
{
    private static final long serialVersionUID = -4514927315711485556L;

    /**
     * Constructs a no datastore mapping exception.
     *
     * @param fieldName The name of the field.
     */
    public NoDatastoreMappingException(String fieldName)
    {
        super(Localiser.msg("020001",fieldName));
    }
}