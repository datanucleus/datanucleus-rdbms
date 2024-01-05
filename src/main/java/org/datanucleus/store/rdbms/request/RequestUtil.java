package org.datanucleus.store.rdbms.request;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.util.NucleusLogger;

public final class RequestUtil
{

    private RequestUtil()
    {
    }

    // Note that SQLController#getStatementForUpdate may throw when the previous batch
    // is executed, which may result in an error message that is not accurate.
    // This will extract correct exception.
    public static NucleusDataStoreException convertSqlException(String msg, SQLException e)
    {
        if (e instanceof BatchUpdateWithSQLException)
        {
            return convertSqlException(e.getMessage(), (SQLException) e.getCause());
        }
        NucleusLogger.DATASTORE_PERSIST.error(msg);
        List<SQLException> exceptions = new ArrayList<>();
        exceptions.add(e);
        while((e = e.getNextException())!=null)
        {
            exceptions.add(e);
        }

        // When doing batching we end up with two exceptions in the list: BatchUpdateException
        // and SQL-stuff. JDOException.getCause returns the first in the list, and that's problematic,
        // because ExceptionUtil.isCausedBy uses that to look for specific types.
        if (exceptions.size() > 1 && exceptions.get(0) instanceof BatchUpdateException)
            exceptions.remove(0);

        return new NucleusDataStoreException(msg, exceptions.toArray(new Throwable[0]));
    }

    public static class BatchUpdateWithSQLException extends SQLException
    {
        private static final long serialVersionUID = 7732720788650460371L;

        public BatchUpdateWithSQLException(String msg, SQLException e)
        {
            super(msg, e);
        }
    }
}
