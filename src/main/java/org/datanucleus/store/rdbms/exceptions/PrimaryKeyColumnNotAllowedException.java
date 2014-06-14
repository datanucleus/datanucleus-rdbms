/**********************************************************************
Copyright (c) 2003 Mike Martin and others. All rights reserved.
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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.util.Localiser;

/**
 * A <tt>PrimaryKeyColumnNotAllowedException</tt> is thrown if an attempt is
 * made to add a primary key column to a view.
 */
public class PrimaryKeyColumnNotAllowedException extends NucleusException
{
    private static final long serialVersionUID = 4600461704079232724L;

    /**
     * Constructs a primary key not allowed exception.
     * @param viewName Name of the view being initialized.
     * @param columnName Name of the column having the duplicate name.
     */
    public PrimaryKeyColumnNotAllowedException(String viewName, String columnName)
    {
        super(Localiser.msg("020014",viewName,columnName));
        setFatal();
    }
}