/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.datanucleus.store.rdbms.datasource.dbcp.pool;

/**
 * A simple base implementation of <code>KeyedObjectPool</code>.
 * Optional operations are implemented to either do nothing, return a value
 * indicating it is unsupported or throw {@link UnsupportedOperationException}.
 *
 * @author Rodney Waldhoff
 * @author Sandy McArthur
 * @version $Revision: 965334 $ $Date: 2010-07-18 20:19:55 -0400 (Sun, 18 Jul 2010) $
 * @since Pool 1.0
 */
public abstract class BaseKeyedObjectPool implements KeyedObjectPool {
    
    public abstract Object borrowObject(Object key) throws Exception;
    
    public abstract void returnObject(Object key, Object obj) throws Exception;
    
    public abstract void invalidateObject(Object key, Object obj) throws Exception;

    public void addObject(Object key) throws Exception, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    public int getNumIdle(Object key) throws UnsupportedOperationException {
        return -1;
    }

    public int getNumActive(Object key) throws UnsupportedOperationException {
        return -1;
    }

    public int getNumIdle() throws UnsupportedOperationException {
        return -1;
    }

    public int getNumActive() throws UnsupportedOperationException {
        return -1;
    }

    public void clear() throws Exception, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    public void clear(Object key) throws Exception, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    public void close() throws Exception {
        closed = true;
    }

    public void setFactory(KeyedPoolableObjectFactory factory) throws IllegalStateException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    protected final boolean isClosed() {
        return closed;
    }

    protected final void assertOpen() throws IllegalStateException {
        if(isClosed()) {
            throw new IllegalStateException("Pool not open");
        }
    }

    private volatile boolean closed = false;
}
