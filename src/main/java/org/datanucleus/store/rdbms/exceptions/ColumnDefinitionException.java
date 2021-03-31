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
2004 Andy Jefferson - coding standards
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.exceptions;

import org.datanucleus.exceptions.NucleusUserException;

/**
 * A <i>ColumnDefinitionException</i> is thrown if the settings of a
 * database column are incompatible with the data type of the object field
 * to which it is mapped.
 *
 * @see org.datanucleus.store.rdbms.table.Column
 */
public class ColumnDefinitionException extends NucleusUserException
{
    private static final long serialVersionUID = -6850676317990100517L;

    /**
     * Constructs a column definition exception with no specific detail message.
     */
    public ColumnDefinitionException()
    {
        super();
        setFatal();
    }

    /**
     * Constructs a column definition exception with the specified detail message.
     * @param msg the detail message
     */
    public ColumnDefinitionException(String msg)
    {
        super(msg);
        setFatal();
    }
}