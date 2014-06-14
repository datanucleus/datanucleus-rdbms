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
 * A <tt>UnsupportedDataTypeException</tt> is thrown if an attempt is made
 * to persist an object field whose data type is not supported by the database
 * and/or the persistence package.
 *
 * @version $Revision: 1.4 $ 
 */
public class UnsupportedDataTypeException extends NucleusDataStoreException
{
    private static final long serialVersionUID = -5976850971131790147L;

    /**
     * Constructs an unsupported data type exception with no specific detail
     * message.
     */
    public UnsupportedDataTypeException()
    {
        super();
        setFatal();
    }

    /**
     * Constructs an unsupported data type exception with the specified detail
     * message.
     *
     * @param msg       the detail message
     */
    public UnsupportedDataTypeException(String msg)
    {
        super(msg);
        setFatal();
    }

    /**
     * Constructs an unsupported data type exception with the specified detail
     * message and nested exception.
     *
     * @param msg       the detail message
     * @param nested    the nested exception(s).
     */
    public UnsupportedDataTypeException(String msg, Exception nested)
    {
        super(msg, nested);
        setFatal();
    }
}