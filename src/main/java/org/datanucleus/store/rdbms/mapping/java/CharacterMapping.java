/**********************************************************************
Copyright (c) 2002 Kelly Grizzle (TJDO) and others. All rights reserved. 
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
2002 Mike Martin (TJDO)
2003 Andy Jefferson - coding standards
2004 Erik Bengtson - included a check "is null" on getObject method 
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.java;

/**
 * Mapping for Character type.
 * In RDBMS, this mapping can be stored in INT or CHAR columns.
 * The use of INT columns facilitates greater than, less than operations within queries etc. 
 */
public class CharacterMapping extends SingleFieldMapping
{
    public Class getJavaType()
    {
        return Character.class;
    }

    /**
     * Method to return the default length of this type in the datastore.
     * Character(char will need a single character!
     * @param index The index position
     * @return The default length
     */
    public int getDefaultLength(int index)
    {
        return 1;
    }
}