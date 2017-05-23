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
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.request;

import org.datanucleus.store.rdbms.query.StatementMappingIndex;

/**
 * Holder for the StatementExpressionIndex for the various details in UPDATE/DELETE statements.
 * Comprised on UPDATE and WHERE clause information.
 */
class StatementMappingDefinition
{
    private StatementMappingIndex[] updateFields;
    private StatementMappingIndex updateVersion;

    private StatementMappingIndex[] whereFields;
    private StatementMappingIndex whereDatastoreId;
    private StatementMappingIndex whereVersion;
    
    /**
     * Accessor for the datastore id mapping index.
     * @return Returns the datastoreId.
     */
    public StatementMappingIndex getWhereDatastoreId()
    {
        return whereDatastoreId;
    }

    /**
     * Mutator for the datastore id mapping index.
     * @param datastoreId The datastoreId to set.
     */
    public void setWhereDatastoreId(StatementMappingIndex datastoreId)
    {
        this.whereDatastoreId = datastoreId;
    }

    /**
     * Accessor for the version mapping index.
     * @return Returns the version index.
     */
    public StatementMappingIndex getUpdateVersion()
    {
        return updateVersion;
    }

    /**
     * Mutator for the version mapping index.
     * @param ver The version to set.
     */
    public void setUpdateVersion(StatementMappingIndex ver)
    {
        this.updateVersion = ver;
    }

    /**
     * Accessor for the mapping indices for the fields.
     * @return Returns the fields.
     */
    public StatementMappingIndex[] getUpdateFields()
    {
        return updateFields;
    }

    /**
     * Mutator for the mapping indices for the fields.
     * @param fields The fields to set.
     */
    public void setUpdateFields(StatementMappingIndex[] fields)
    {
        this.updateFields = fields;
    }

    /**
     * Accessor for the mapping indices for the fields in the WHERE clause.
     * @return Returns the where clause fields.
     */
    public StatementMappingIndex[] getWhereFields()
    {
        return whereFields;
    }

    /**
     * Mutator for the mapping indices for the fields in the WHERE clause.
     * @param fields The where clause fields
     */
    public void setWhereFields(StatementMappingIndex[] fields)
    {
        this.whereFields = fields;
    }

    /**
     * Accessor for the WHERE version mapping index.
     * @return Returns the WHERE version index.
     */
    public StatementMappingIndex getWhereVersion()
    {
        return whereVersion;
    }

    /**
     * Accessor for the WHERE version mapping index.
     * @param ver The WHERE version.
     */
    public void setWhereVersion(StatementMappingIndex ver)
    {
        this.whereVersion = ver;
    }
}