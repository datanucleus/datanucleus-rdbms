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

import org.datanucleus.exceptions.DatastoreValidationException;
import org.datanucleus.util.Localiser;

/**
 * A <i>NotAViewException</i> is thrown during schema validation if a
 * table should be a view but is found not to be in the database.
 */
public class NotAViewException extends DatastoreValidationException
{
    private static final long serialVersionUID = -3924285872907278607L;

    /**
     * Constructs a not-a-view exception.
     * @param viewName Name of the view that is of the wrong type.
     * @param type the type of the object
     */
    public NotAViewException(String viewName, String type)
    {
        super(Localiser.msg("020013", viewName, type));
    }
}