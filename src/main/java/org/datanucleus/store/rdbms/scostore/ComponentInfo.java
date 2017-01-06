/**********************************************************************
Copyright (c) 2015 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.scostore;

import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping;
import org.datanucleus.store.rdbms.table.DatastoreClass;
import org.datanucleus.store.schema.table.SurrogateColumnType;

/**
 * Representation of a component of a collection/map. 
 * Can be element of a collection, element of an array, key of a map, or value of a map.
 */
public class ComponentInfo
{
    /** MetaData for the component class. */
    AbstractClassMetaData cmd;

    /** Primary table storing the component. */
    DatastoreClass table;

    JavaTypeMapping ownerMapping;

    public ComponentInfo(AbstractClassMetaData cmd, DatastoreClass table)
    {
        this.cmd = cmd;
        this.table = table;
    }
    public void setOwnerMapping(JavaTypeMapping m)
    {
        this.ownerMapping = m;
    }

    public String getClassName()
    {
        return cmd.getFullClassName();
    }
    public AbstractClassMetaData getAbstractClassMetaData()
    {
        return cmd;
    }
    public DatastoreClass getDatastoreClass()
    {
        return table;
    }
    public DiscriminatorStrategy getDiscriminatorStrategy()
    {
        return cmd.getDiscriminatorStrategyForTable();
    }
    public JavaTypeMapping getDiscriminatorMapping()
    {
        return table.getSurrogateMapping(SurrogateColumnType.DISCRIMINATOR, false);
    }
    public JavaTypeMapping getOwnerMapping()
    {
        return ownerMapping;
    }

    public String toString()
    {
        return "ComponentInfo : [class=" + cmd.getFullClassName() + " table=" + table + "]";
    }
}