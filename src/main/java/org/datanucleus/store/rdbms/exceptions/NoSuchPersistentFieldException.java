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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.exceptions;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.util.Localiser;

/**
 * A <i>NoSuchPersistentFieldException</i> is thrown if a reference is made
 * somewhere, such as in a query filter string, to a field that either doesn't
 * exist or is not persistent.
 */
public class NoSuchPersistentFieldException extends NucleusUserException
{
    private static final long serialVersionUID = 2364470194200034650L;

    /**
     * Constructs a no such persistent field exception.
     *
     * @param className The class in which the field was not found.
     * @param fieldName The name of the field.
     */
    public NoSuchPersistentFieldException(String className, String fieldName)
    {
        super(Localiser.msg("018009",fieldName,className));
    }

    /**
     * Constructs a no such persistent field exception.
     *
     * @param className     The class in which the field was not found.
     * @param fieldNumber   The field number  of the field.
     */
    public NoSuchPersistentFieldException(String className, int fieldNumber)
    {
        super(Localiser.msg("018010","" + fieldNumber,className));
    }
}
