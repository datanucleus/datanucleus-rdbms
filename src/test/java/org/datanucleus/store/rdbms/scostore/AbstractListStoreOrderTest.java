/**********************************************************************
Copyright (c) 2024 Contributors. All rights reserved.
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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.datanucleus.store.rdbms.adapter.DatastoreAdapter;
import org.datanucleus.store.rdbms.adapter.H2Adapter;

/**
 * Tests for the bulk shift ordering fix in AbstractListStore.
 * Verifies that H2Adapter registers the ORDER_BY_IN_UPDATE_STATEMENT capability,
 * which enables the ORDER BY DESC clause on bulk shift UPDATEs for positive (upward) shifts.
 *
 * <p>Background: On MySQL, when shifting list indices upward (e.g. inserting at the start),
 * the UPDATE processes rows in ascending PK/storage order. Without ORDER BY DESC, the lowest
 * index row is updated first, colliding with a not-yet-shifted higher index row, causing
 * a duplicate key violation. The fix adds ORDER BY idx DESC to the bulk UPDATE for positive
 * shifts, gated behind the ORDER_BY_IN_UPDATE_STATEMENT adapter capability.</p>
 *
 * <p>H2 does not exhibit the bug (it processes rows in a different order), but enabling the
 * capability ensures the fix is exercised and tested on H2.</p>
 */
public class AbstractListStoreOrderTest extends TestCase
{
    private Connection conn;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:mem:testOrderBy;DB_CLOSE_DELAY=-1", "sa", "");
    }

    @Override
    protected void tearDown() throws Exception
    {
        if (conn != null && !conn.isClosed())
        {
            conn.close();
        }
        super.tearDown();
    }

    /**
     * Verify that H2Adapter registers ORDER_BY_IN_UPDATE_STATEMENT capability.
     */
    public void testH2AdapterSupportsOrderByInUpdate() throws SQLException
    {
        DatabaseMetaData md = conn.getMetaData();
        H2Adapter adapter = new H2Adapter(md);
        assertTrue("H2Adapter should support ORDER_BY_IN_UPDATE_STATEMENT",
            adapter.supportsOption(DatastoreAdapter.ORDER_BY_IN_UPDATE_STATEMENT));
    }

    /**
     * Verify that the ORDER_BY_IN_UPDATE_STATEMENT constant is defined and has the expected value.
     */
    public void testOrderByInUpdateConstantDefined()
    {
        assertEquals("OrderByInUpdateStatement", DatastoreAdapter.ORDER_BY_IN_UPDATE_STATEMENT);
    }
}
