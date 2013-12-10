/**********************************************************************
Copyright (c) 2003 Mike Martin (TJDO) and others. All rights reserved.
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

import org.datanucleus.store.exceptions.DatastoreValidationException;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.datanucleus.util.Localiser;

/**
 * A <tt>WrongPrecisionException</tt> is thrown if a column is detected to have
 * the wrong precision in the database during schema validation.
 * @version $Revision: 1.3 $ 
 */
public class WrongPrecisionException extends DatastoreValidationException
{
    private static final Localiser LOCALISER_RDBMS=Localiser.getInstance("org.datanucleus.store.rdbms.Localisation",
        RDBMSStoreManager.class.getClassLoader());

    /**
     * Constructs a wrong precision exception.
     *
     * @param columnName Name of the column having the wrong precision.
     * @param expectedPrecision The expected precision of the column.
     * @param actualPrecision The actual precision of the column.
     */
    public WrongPrecisionException(String columnName, int expectedPrecision, int actualPrecision)
    {
        super(LOCALISER_RDBMS.msg("020018", columnName, "" + actualPrecision,"" + expectedPrecision));
    }
    
    /**
     * Constructs a wrong precision exception.
     *
     * @param columnName Name of the column having the wrong precision.
     * @param expectedPrecision The expected precision of the column.
     * @param actualPrecision The actual precision of the column.
     * @param fieldName The field name.
     */
    public WrongPrecisionException(String columnName, int expectedPrecision, int actualPrecision, String fieldName)
    {
        super(LOCALISER_RDBMS.msg("020019", columnName, "" + actualPrecision,"" + expectedPrecision, fieldName));
    }    
}
