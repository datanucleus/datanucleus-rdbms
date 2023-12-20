package org.datanucleus.store.rdbms.flush;

import org.datanucleus.ExecutionContext;
import org.datanucleus.flush.FlushProcess;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.rdbms.query.StatementMappingIndex;

/**
 * A marker interface for defining a custom flush process for RDMS that goes beyond what can be
 * controlled by standard flush mechanisms.
 * Interface enables custom ways of specifying when insert-, update-
 * and delete-requests can be batched.
 * It all enables more batching (and thereby better performance) tailored to a given
 * implementation.
 */
public interface BatchingFlushProcess extends FlushProcess
{
    /**
     * Decide whether batching should be enabled for this kind of InsertRequest
     * @param hasIdentityColumn see InsertRequest for details
     * @param externalFKStmtMappings see InsertRequest for details
     * @param cmd Class meta data object for InsertRequest
     * @param ec execution context
     * @return whether batching should be enabled for this kind of InsertRequest
     */
    boolean batchInInsertRequest(boolean hasIdentityColumn, StatementMappingIndex[] externalFKStmtMappings, AbstractClassMetaData cmd, ExecutionContext ec);

    /**
     * Decide whether batching should be enabled for this kind of UpdateRequest
     * @param optimisticChecks whether optimistic locking is enabled for object being updated
     * @param ec execution context
     * @return whether batching should be enabled for this kind of UpdateRequest
     */
    boolean batchInUpdateRequest(boolean optimisticChecks, ExecutionContext ec);

    /**
     * Decide whether batching should be enabled for this kind of DeleteRequest
     * @param optimisticChecks whether optimistic locking is enabled for object being deleted
     * @param ec execution context
     * @return whether batching should be enabled for this kind of DeleteRequest
     */
    boolean batchInDeleteRequest(boolean optimisticChecks, ExecutionContext ec);
}
