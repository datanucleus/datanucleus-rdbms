/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.identifier;

/**
 * Enum defining the types of cases that mapped component identifiers can be stored in.
 * UPPER case will be all in uppercase. LOWER case will all be in lowercase. MIXED case
 * can be in upper and lower case. Sometimes identifiers need quotes surrounding them, hence
 * the QUOTED options.
 */
public enum IdentifierCase 
{
    UPPER_CASE("UPPERCASE"),
    UPPER_CASE_QUOTED("\"UPPERCASE\""),
    LOWER_CASE("lowercase"),
    LOWER_CASE_QUOTED("\"lowercase\""),
    MIXED_CASE("MixedCase"),
    MIXED_CASE_QUOTED("\"MixedCase\"");

    String name;
    private IdentifierCase(String name)
    {
        this.name = name;
    }
    public String toString()
    {
        return name;
    }
}