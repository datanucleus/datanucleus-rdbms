/**********************************************************************
Copyright (c) 2004 Ralf Ullrich and others. All rights reserved.
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
package org.datanucleus.store.rdbms.exceptions;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.util.Localiser;

/**
 * A <i>NoTableManagedException</i> is thrown if an attempt is made to perform an
 * operation using a class that is not backed by an table or view
 * in the database and the operation is not supported on such classes.
 *
 * @see org.datanucleus.store.StoreManager
 */
public class NoTableManagedException extends NucleusUserException
{
    private static final long serialVersionUID = -1610018637755474684L;

    /**
     * Constructs a no table managed exception.
     * @param className Name of the class on which the operation requiring a
     *                  table was attempted.
     */
    public NoTableManagedException(String className)
    {
        super(Localiser.msg("020000",className));
    }
}