/**********************************************************************
Copyright (c) 2003 Erik Bengtson and others. All rights reserved.
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
2004 Andy Jefferson - coding standards
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.datastore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.sql.Blob;
import java.sql.SQLException;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.util.NucleusLogger;

/**
 * <p>
 * The representation (mapping) in the Java <sup><font size=-2>TM </font>
 * </sup> programming language of an SQL <code>BLOB</code> value. An SQL
 * <code>BLOB</code> is a built-in type that stores a Binary Large Object as a
 * column value in a row of a database table. The driver implements
 * <code>Blob</code> using an SQL <code>locator(BLOB)</code>, which means
 * that a <code>Blob</code> object contains a logical pointer to the SQL
 * <code>BLOB</code> data rather than the data itself. A <code>Blob</code>
 * object is valid for the duration of the transaction in which is was created.
 * <P>
 * Methods in the interfaces {@link java.sql.ResultSet},
 * {@link java.sql.CallableStatement}, and {@link java.sql.PreparedStatement},
 * such as <code>getBlob</code> and <code>setBlob</code> allow a programmer
 * to access an SQL <code>BLOB</code> value. The <code>Blob</code> interface
 * provides methods for getting the length of an SQL <code>BLOB</code> (Binary
 * Large Object) value, for materializing a <code>BLOB</code> value on the
 * client, and for determining the position of a pattern of bytes within a
 * <code>BLOB</code> value.
 * <P>
 * This class is new in the JDBC 2.0 API.
 */
public class BlobImpl implements Blob
{
    private InputStream stream;

    private int length;

    private byte bytes[];

    /** Whether we have already freed resources. */
    boolean freed = false;

    /**
     * Constructor taking a serialised object.
     * @param obj The serialised object.
     * @throws IOException
     */
    public BlobImpl(Object obj) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        bytes = baos.toByteArray();
        this.stream = new ByteArrayInputStream(bytes);
        this.length = bytes.length;
    }

    /**
     * Constructor taking a byte array.
     * @param bytes The byte array
     */
    public BlobImpl(byte[] bytes)
    {
        this.bytes = bytes;
        this.stream = new ByteArrayInputStream(bytes);
        this.length = bytes.length;
    }

    /**
     * Constructor taking an InputStream.
     * @param stream The InputStream
     */
    public BlobImpl(InputStream stream)
    {
        this.stream = stream;
//        this.length = bytes.length;
    }

    /**
     * Accessor for the Object.
     * @return The object.
     */
    public Object getObject()
    throws SQLException
    {
        if (freed)
        {
            throw new SQLException("free() has been called");
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois;
        try
        {
            ois = new ObjectInputStream(bais);
            return ois.readObject();
        }
        catch (StreamCorruptedException e)
        {
            String msg = "StreamCorruptedException: object is corrupted";
            NucleusLogger.DATASTORE.error(msg);
            throw new NucleusUserException(msg, e).setFatal();
        }
        catch (IOException e)
        {
            String msg = "IOException: error when reading object";
            NucleusLogger.DATASTORE.error(msg);
            throw new NucleusUserException(msg, e).setFatal();
        }
        catch (ClassNotFoundException e)
        {
            String msg = "ClassNotFoundException: error when creating object";
            NucleusLogger.DATASTORE.error(msg);
            throw new NucleusUserException(msg, e).setFatal();
        }
    }

    /* (non-Javadoc)
     * @see java.sql.Blob#length()
     */
    public long length() throws SQLException
    {
        if (freed)
        {
            throw new SQLException("free() has been called");
        }

        return length;
    }

    /* (non-Javadoc)
     * @see java.sql.Blob#getBytes(long, int)
     */
    public byte[] getBytes(long pos, int length) throws SQLException
    {
        if (freed)
        {
            throw new SQLException("free() has been called");
        }

        byte bytesToReturn[] = new byte[length];
        for (int i = 0; i < length; i++)
        {
            bytesToReturn[i] = bytes[(int) pos + i];
        }
        return bytesToReturn;
    }

    public int setBytes(long value, byte[] bytes, int pos, int length) throws SQLException
    {
        if (freed)
        {
            throw new SQLException("free() has been called");
        }

        return -1;
    }

    public void truncate(long value)
    throws SQLException
    {
        if (freed)
        {
            throw new SQLException("free() has been called");
        }
    }

    public int setBytes(long value, byte[] bytes) throws SQLException
    {
        if (freed)
        {
            throw new SQLException("free() has been called");
        }

        return -1;
    }

    /* (non-Javadoc)
     * @see java.sql.Blob#getBinaryStream()
     */
    public InputStream getBinaryStream() throws SQLException
    {
        if (freed)
        {
            throw new SQLException("free() has been called");
        }

        return stream;
    }

    /**
     * Returns an InputStream object that contains a partial Blob value, starting with the byte specified by pos, 
     * which is length bytes in length.
     * @param pos the offset to the first byte of the partial value to be retrieved. 
     *     The first byte in the Blob is at position 1
     * @param length the length in bytes of the partial value to be retrieved
     */
    public InputStream getBinaryStream(long pos, long length) throws SQLException
    {
        if (freed)
        {
            throw new SQLException("free() has been called");
        }

        // TODO Use pos, length
        return stream;
    }

    public OutputStream setBinaryStream(long value) throws SQLException
    {
        if (freed)
        {
            throw new SQLException("free() has been called");
        }

        return null;
    }

    /**
     * Free the Blob object and releases the resources that it holds.
     * The object is invalid once the free method is called.
     * @throws SQLException
     */
    public void free() throws SQLException
    {
        if (freed)
        {
            return;
        }

        bytes = null;
        if (stream != null)
        {
            try
            {
                stream.close();
            }
            catch (IOException ioe)
            {
                // Do nothing
            }
        }
        freed = true;
    }

    /* (non-Javadoc)
     * @see java.sql.Blob#position(byte[], long)
     */
    public long position(byte[] pattern, long start) throws SQLException
    {
        if (freed)
        {
            throw new SQLException("free() has been called");
        }

        throw new UnsupportedOperationException("[BlobImpl.position] may not be called");
    }

    /* (non-Javadoc)
     * @see java.sql.Blob#position(java.sql.Blob, long)
     */
    public long position(Blob pattern, long start) throws SQLException
    {
        if (freed)
        {
            throw new SQLException("free() has been called");
        }

        throw new UnsupportedOperationException("[BlobImpl.position] may not be called");
    }
}