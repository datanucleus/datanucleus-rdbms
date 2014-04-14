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

import java.util.NoSuchElementException;

/**
 * A "keyed" pooling interface.
 */
public interface KeyedObjectPool {

    Object borrowObject(Object key) throws Exception, NoSuchElementException, IllegalStateException;

    void returnObject(Object key, Object obj) throws Exception;

    void invalidateObject(Object key, Object obj) throws Exception;

    void addObject(Object key) throws Exception, IllegalStateException, UnsupportedOperationException;

    int getNumIdle(Object key) throws UnsupportedOperationException;

    int getNumActive(Object key) throws UnsupportedOperationException;

    int getNumIdle() throws UnsupportedOperationException;

    int getNumActive() throws UnsupportedOperationException;

    void clear() throws Exception, UnsupportedOperationException;

    void clear(Object key) throws Exception, UnsupportedOperationException;

    void close() throws Exception;

    void setFactory(KeyedPoolableObjectFactory factory) throws IllegalStateException, UnsupportedOperationException;
}
