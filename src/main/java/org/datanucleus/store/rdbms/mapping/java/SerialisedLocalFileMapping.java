/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.rdbms.mapping.java;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.rdbms.mapping.MappingCallbacks;
import org.datanucleus.store.rdbms.table.Table;
import org.datanucleus.transaction.TransactionEventListener;
import org.datanucleus.util.NucleusLogger;

/**
 * Mapping where we want to serialise a (Serializable) field to a local file. Since the field will be stored in
 * the local file system then this will have no "datastore mapping" (i.e column) as such.
 * The user defines the folder in which values of this field will be stored in metadata, and the filename in that 
 * folder is basically the "id" of the owning object. Assumes that the user is not storing multiple fields in the
 * same folder. 
 * <h3>Handling roll back</h3>
 * Whenever an insert/update/delete is performed it registers a listener on any active transaction
 * and allows a hook to attempt to roll back any changes to the value before that operation.
 */
public class SerialisedLocalFileMapping extends JavaTypeMapping implements MappingCallbacks
{
    public static final String EXTENSION_SERIALIZE_TO_FOLDER = "serializeToFileLocation";

    String folderName = null;

    public void initialize(AbstractMemberMetaData mmd, Table table, ClassLoaderResolver clr)
    {
        super.initialize(mmd, table, clr);
        folderName = mmd.getValueForExtension(EXTENSION_SERIALIZE_TO_FOLDER);
        File folder = new File(folderName);
        if (!folder.exists())
        {
            NucleusLogger.PERSISTENCE.debug("Creating folder for persistence data for field " + mmd.getFullFieldName() + " : folder=" + folderName);
            folder.mkdir();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping#includeInFetchStatement()
     */
    @Override
    public boolean includeInFetchStatement()
    {
        // Retrieved separately
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping#includeInUpdateStatement()
     */
    @Override
    public boolean includeInUpdateStatement()
    {
        // Persisted separately
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping#includeInInsertStatement()
     */
    @Override
    public boolean includeInInsertStatement()
    {
        // Persisted separately
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.rdbms.mapping.java.JavaTypeMapping#getJavaType()
     */
    @Override
    public Class getJavaType()
    {
        return mmd.getType();
    }

    @Override
    public void postInsert(final DNStateManager sm)
    {
        Object val = sm.provideField(mmd.getAbsoluteFieldNumber());
        serialiseFieldValue(sm, val);

        if (sm.getExecutionContext().getTransaction().isActive())
        {
            // Catch any rollback
            sm.getExecutionContext().getTransaction().addTransactionEventListener(new TransactionEventListener()
            {
                public void transactionPreRollBack()
                {
                    // Remove the file
                    File fieldFile = new File(getFilenameForStateManager(sm));
                    if (fieldFile.exists())
                    {
                        fieldFile.delete();
                    }
                }
                public void transactionStarted() {}
                public void transactionRolledBack() {}
                public void transactionPreFlush() {}
                public void transactionPreCommit() {}
                public void transactionFlushed() {}
                public void transactionEnded() {}
                public void transactionCommitted() {}
                public void transactionSetSavepoint(String name) {}
                public void transactionReleaseSavepoint(String name) {}
                public void transactionRollbackToSavepoint(String name) {}
            });
        }
    }

    @Override
    public void postFetch(DNStateManager sm)
    {
        Object value = deserialiseFieldValue(sm);
        sm.replaceField(mmd.getAbsoluteFieldNumber(), value);
    }

    @Override
    public void postUpdate(final DNStateManager sm)
    {
        final Object oldValue = deserialiseFieldValue(sm);

        Object val = sm.provideField(mmd.getAbsoluteFieldNumber());
        serialiseFieldValue(sm, val);

        if (sm.getExecutionContext().getTransaction().isActive())
        {
            // Catch any rollback
            sm.getExecutionContext().getTransaction().addTransactionEventListener(new TransactionEventListener()
            {
                public void transactionPreRollBack()
                {
                    // Reset to previous value
                    serialiseFieldValue(sm, oldValue);
                }
                public void transactionStarted() {}
                public void transactionRolledBack() {}
                public void transactionPreFlush() {}
                public void transactionPreCommit() {}
                public void transactionFlushed() {}
                public void transactionEnded() {}
                public void transactionCommitted() {}
                public void transactionSetSavepoint(String name) {}
                public void transactionReleaseSavepoint(String name) {}
                public void transactionRollbackToSavepoint(String name) {}
            });
        }
    }

    @Override
    public void preDelete(final DNStateManager sm)
    {
        final Object oldValue = sm.provideField(mmd.getAbsoluteFieldNumber());

        // Delete the file for this field of this StateManager
        File fieldFile = new File(getFilenameForStateManager(sm));
        if (fieldFile.exists())
        {
            fieldFile.delete();
        }

        if (sm.getExecutionContext().getTransaction().isActive())
        {
            // Catch any rollback
            sm.getExecutionContext().getTransaction().addTransactionEventListener(new TransactionEventListener()
            {
                public void transactionPreRollBack()
                {
                    // Reset to previous value
                    serialiseFieldValue(sm, oldValue);
                }
                public void transactionStarted() {}
                public void transactionRolledBack() {}
                public void transactionPreFlush() {}
                public void transactionPreCommit() {}
                public void transactionFlushed() {}
                public void transactionEnded() {}
                public void transactionCommitted() {}
                public void transactionSetSavepoint(String name) {}
                public void transactionReleaseSavepoint(String name) {}
                public void transactionRollbackToSavepoint(String name) {}
            });
        }
    }

    protected String getFilenameForStateManager(DNStateManager sm)
    {
        return folderName + System.getProperty("file.separator") + sm.getInternalObjectId();
    }

    /**
     * Method to serialise the value from StateManager to file.
     * @param sm StateManager
     * @param value The value being serialised
     */
    protected void serialiseFieldValue(DNStateManager sm, Object value)
    {
        try
        {
            // Serialise the field value to the appropriate file in this folder
            FileOutputStream fileOut = new FileOutputStream(getFilenameForStateManager(sm));
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(value);
            out.close();
            fileOut.close();
        }
        catch(IOException ioe)
        {
            ioe.printStackTrace();
        }
    }

    /**
     * Method to deserialise the value from file and return it.
     * @param sm StateManager to get the value for
     * @return The value currently stored
     */
    protected Object deserialiseFieldValue(DNStateManager sm)
    {
        Object value = null;
        try
        {
            // Deserialise the field value from the appropriate file in this folder
            FileInputStream fileIn = new FileInputStream(getFilenameForStateManager(sm));
            ObjectInputStream in = new ObjectInputStream(fileIn);
            value = in.readObject();
            in.close();
            fileIn.close();
        }
        catch (Exception e)
        {
           e.printStackTrace();
           return null;
        }
        return value;
    }
}