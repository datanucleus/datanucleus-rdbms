package org.datanucleus.store.rdbms;

import org.datanucleus.ExecutionContext;
import org.datanucleus.store.connection.ManagedConnection;

/**
 * Managed connection that knows its execution context.
 */
public interface ManagedConnectionWithExecutionContext extends ManagedConnection
{
    /**
     * Get execution context of connection
     * @return execution context.
     */
    ExecutionContext getExecutionContext();
}
