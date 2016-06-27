/**********************************************************************
Copyright (c) 2005 Erik Bengtson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.adapter;

/**
 * Java Functions that are called by the database
 */
public class DerbySQLFunction
{
    private DerbySQLFunction(){}
    /**
     * ASCII code.
     * @param code The code
     * @return The ascii character
     */
    public static int ascii(String code)
    {
        return code.charAt(0);
    }

    /**
     * Matches code.
     * @param text The text
     * @param pattern the pattern
     * @return 1 if true according to String.matches rules, 0 otherwise
     */
    public static int matches(String text, String pattern)
    {
        return text.matches(pattern)?1:0;
    }
}