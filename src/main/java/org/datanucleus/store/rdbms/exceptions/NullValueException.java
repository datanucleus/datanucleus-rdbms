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

import org.datanucleus.exceptions.NucleusDataStoreException;

/**
 * A <tt>NullValueException</tt> is thrown if a null value is encountered
 * in a database column that should prohibit null values.
 * 
 * @version $Revision: 1.3 $
 */
public class NullValueException extends NucleusDataStoreException
{
    private static final long serialVersionUID = -4852762927328278822L;

    /**
     * Constructs a null value exception with no specific detail message.
     */
    public NullValueException()
    {
        super();
    }

    /**
     * Constructs a null value exception with the specified detail message.
     *
     * @param msg       the detail message
     */
    public NullValueException(String msg)
    {
        super(msg);
    }
}
