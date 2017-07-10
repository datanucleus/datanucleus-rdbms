/**********************************************************************
Copyright (c) 2002 Mike Martin (TJDO) and others. All rights reserved.   
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
2003 Andy Jefferson - coding standards
2004 Erik Bengtson - added sequence handling
2006 Andy Jefferson - updated to MaxDB 7.6
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.adapter;

import java.sql.DatabaseMetaData;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.datanucleus.store.rdbms.key.CandidateKey;
import org.datanucleus.store.rdbms.key.ForeignKey;
import org.datanucleus.store.rdbms.key.Index;
import org.datanucleus.store.rdbms.key.PrimaryKey;
import org.datanucleus.util.Localiser;

/**
 * Provides methods for adapting SQL language elements to the SAPDB/MaxDB database.
 */
public class SAPDBAdapter extends BaseDatastoreAdapter
{
    /**
     * Constructs an SAP DB adapter based on the given JDBC metadata.
     * @param metadata the database metadata.
     */
    public SAPDBAdapter(DatabaseMetaData metadata)
    {
        super(metadata);

        supportedOptions.add(BOOLEAN_COMPARISON);
        supportedOptions.add(LOCK_WITH_SELECT_FOR_UPDATE);
        supportedOptions.add(SEQUENCES);
        supportedOptions.remove(ALTER_TABLE_DROP_CONSTRAINT_SYNTAX);
        supportedOptions.remove(DEFERRED_CONSTRAINTS);
        supportedOptions.add(BIT_IS_REALLY_BOOLEAN);
        supportedOptions.add(ORDERBY_USING_SELECT_COLUMN_INDEX);

        supportedOptions.remove(FK_UPDATE_ACTION_CASCADE);
        supportedOptions.remove(FK_UPDATE_ACTION_DEFAULT);
        supportedOptions.remove(FK_UPDATE_ACTION_NULL);
        supportedOptions.remove(FK_UPDATE_ACTION_RESTRICT);
    }

    public String getVendorID()
    {
        return "sapdb";
    }

    /**
     * The option to specify in "SELECT ... WITH (option)" to lock instances. Null if not supported.
     * @return The option to specify with "SELECT ... WITH (option)"
     */
    public String getSelectWithLockOption()
    {
        return "EXCLUSIVE LOCK";
    }

    public String getAddPrimaryKeyStatement(PrimaryKey pk, IdentifierFactory factory)
    {
        // MaxDB doesnt support having "ADD CONSTRAINT pk_name"
        return "ALTER TABLE " + pk.getTable().toString() + " ADD " + pk;
    }

    public String getAddCandidateKeyStatement(CandidateKey ck, IdentifierFactory factory)
    {
        Index idx = new Index(ck);
        idx.setName(ck.getName());
        return getCreateIndexStatement(idx, factory);
    }

    public String getAddForeignKeyStatement(ForeignKey fk, IdentifierFactory factory)
    {
        // MaxDB doesnt support having "ADD CONSTRAINT fk_name"
        return "ALTER TABLE " + fk.getTable().toString() + " ADD " + fk;
    }

    // ---------------------------- Sequence Support ---------------------------

    /**
     * Accessor for the sequence statement to create the sequence.
     * @param sequenceName Name of the sequence 
     * @param min Minimum value for the sequence
     * @param max Maximum value for the sequence
     * @param start Start value for the sequence
     * @param increment Increment value for the sequence
     * @param cacheSize Cache size for the sequence
     * @return The statement for getting the next id from the sequence
     */
    public String getSequenceCreateStmt(String sequenceName, Integer min, Integer max, Integer start, Integer increment, Integer cacheSize)
    {
        if (sequenceName == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }
        
        StringBuilder stmt = new StringBuilder("CREATE SEQUENCE ");
        stmt.append(sequenceName);
        if (min != null)
        {
            stmt.append(" MINVALUE " + min);
        }
        if (max != null)
        {
            stmt.append(" MAXVALUE " + max);
        }
        if (start != null)
        {
            stmt.append(" START WITH " + start);
        }
        if (increment != null)
        {
            stmt.append(" INCREMENT BY " + increment);
        }
        if (cacheSize != null)
        {
            stmt.append(" CACHE " + cacheSize);
        }
        else
        {
            stmt.append(" NOCACHE");
        }
        
        return stmt.toString();
    }

    /**
     * Accessor for the statement for getting the next id from the sequence for this datastore.
     * @param sequenceName Name of the sequence 
     * @return The statement for getting the next id for the sequence
     **/
    public String getSequenceNextStmt(String sequenceName)
    {
        if (sequenceName == null)
        {
            throw new NucleusUserException(Localiser.msg("051028"));
        }

        StringBuilder stmt=new StringBuilder("SELECT ");
        stmt.append(sequenceName);
        stmt.append(".nextval FROM dual");

        return stmt.toString();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.adapter.BaseDatastoreAdapter#getSQLMethodClass(java.lang.String, java.lang.String)
     */
    @Override
    public Class getSQLMethodClass(String className, String methodName)
    {
        if (className != null)
        {
            if ("java.lang.String".equals(className) && "length".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringLength3Method.class;
            else if ("java.lang.String".equals(className) && "substring".equals(methodName)) return org.datanucleus.store.rdbms.sql.method.StringSubstring3Method.class;
        }

        return super.getSQLMethodClass(className, methodName);
    }
}