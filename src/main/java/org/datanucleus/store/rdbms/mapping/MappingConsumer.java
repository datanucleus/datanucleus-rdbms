/**********************************************************************
Copyright (c) 2004 Erik Bengtson and others. All rights reserved. 
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
2004 Andy Jefferson - added discriminator constant
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.Column;

/**
 * Consumer of mappings.
 */
public interface MappingConsumer
{
    /**
     * This method is called before consuming the mappings
     * @param highestFieldNumber the highest number for the fields that are going to be provided in the consumer
     */
    void preConsumeMapping(int highestFieldNumber);

    /**
     * Consumes a mapping associated to a member
     * @param m The Java type mapping
     * @param mmd MetaData for the member
     */
    void consumeMapping(JavaTypeMapping m, AbstractMemberMetaData mmd);

    /**
     * Consumes a mapping not associated to a member
     * @param m Java type mapping
     * @param mappingType the Mapping type
     */
    void consumeMapping(JavaTypeMapping m, MappingType mappingType);

    /**
     * Consume a column without mapping.
     * @param col The column
     */
    void consumeUnmappedColumn(Column col);
}