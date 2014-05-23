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

import org.datanucleus.store.rdbms.exceptions.ClassDefinitionException;
import org.datanucleus.util.Localiser;

/**
 * A <tt>PersistentSuperclassNotAllowedException</tt> is thrown if a
 * persistence-capable class is declared to have a persistence-capable
 * superclass when that class is backed by a view.
 *
 * @see org.datanucleus.store.rdbms.table.ClassView
 */
public class PersistentSuperclassNotAllowedException extends ClassDefinitionException
{
    /**
     * Constructs a persistent-superclass-not-allowed exception.
     *
     * @param className The class having the persistence-capable superclass.
     */
    public PersistentSuperclassNotAllowedException(String className)
    {
        super(Localiser.msg("020023",className));
    }
}