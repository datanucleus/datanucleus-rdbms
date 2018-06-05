/**********************************************************************
Copyright (c) 2013 Guido Anzuoni and others. All rights reserved.
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
2013 Andy Jefferson - migrate contrib patch into usable format with DN3+
    ...
**********************************************************************/
package org.datanucleus.store.rdbms.mapping.column;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.util.NucleusLogger;

/**
 * Spooler for files being streamed from the datastore.
 */
public class StreamableSpooler
{
    protected static StreamableSpooler _instance = new StreamableSpooler();

    public static StreamableSpooler instance()
    {
        return _instance;
    }

    protected StreamableSpoolerGC gcInstance = null;

    protected File spoolDirectory;

    private StreamableSpooler()
    {
        String spool = null;

        // Try using system property
        spool = System.getProperty("datanucleus.binarystream.spool.directory");
        if (spool != null)
        {
            File f = new File(spool);
            if (!f.isDirectory())
            {
                throw new NucleusFatalUserException("Invalid binarystream spool directory:" + spool);
            }
            spoolDirectory = f;
        }
        else
        {
            // Fallback to users home directory
            spool = System.getProperty("user.dir");
            if (spool != null)
            {
                File f = new File(spool);
                if (!f.isDirectory())
                {
                    throw new NucleusFatalUserException("Invalid binarystream spool directory:" + spool);
                }
                spoolDirectory = f;
            }
        }

        if (spool == null)
        {
            throw new NucleusFatalUserException("Cannot get binary stream spool directory");
        }
    }

    public void spoolStreamTo(InputStream is, File target) throws IOException 
    {
        FileOutputStream fos = new FileOutputStream(target);
        try
        {
            copyStream(is, new BufferedOutputStream(fos), false, true);
        }
        finally
        {
            try
            {
                fos.close();
            }
            catch (IOException ioe)
            {
            }
        }
    }

    public File spoolStream(InputStream is) throws IOException
    {
        File spool = File.createTempFile("datanucleus.binarystream-", ".bin", spoolDirectory);
        if (gcInstance == null)
        {
            gcInstance = new StreamableSpoolerGC();
        }
        gcInstance.add(spool);
        NucleusLogger.GENERAL.debug("spool file created: " + spool.getAbsolutePath());
        spool.deleteOnExit();

        FileOutputStream fos = new FileOutputStream(spool);
        try
        {
            copyStream(is, new BufferedOutputStream(fos), false, true);
        }
        finally
        {
            try
            {
                fos.close();
            }
            catch (IOException ioe)
            {
            }
        }

        return spool;
    }

    public StreamableSpoolerGC getGCInstance()
    {
        return gcInstance;
    }

    public static void copyStream(InputStream is, OutputStream os) throws IOException 
    {
        copyStream(is, os, false, false);
    }

    public static void copyStream(InputStream is, OutputStream os, boolean close_src, boolean close_dest) throws IOException
    {
        int b;
        while ((b = is.read()) != -1)
        {
            os.write(b);
        }
        if (close_src)
        {
            is.close();
        }
        if (close_dest)
        {
            os.close();
        }
    }

    public class StreamableSpoolerGC extends Thread
    {
        protected ReferenceQueue refQ;

        protected Collection references = new HashSet();

        public StreamableSpoolerGC()
        {
            refQ = new ReferenceQueue();
            setDaemon(true);
            start();
        }

        public void add(File f)
        {
            try
            {
                FileWeakReference fwr = new FileWeakReference(f, refQ);
                references.add(fwr);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

        public void run()
        {
            while (true)
            {
                try
                {
                    Reference ref = refQ.remove(0);
                    FileWeakReference fwr = (FileWeakReference) ref;
                    fwr.gc();
                    references.remove(fwr);
                }
                catch (IllegalArgumentException ex)
                {
                }
                catch (InterruptedException ex)
                {
                    ex.printStackTrace();
                    break;
                }
            }
            Iterator iter = references.iterator();
            while (iter.hasNext())
            {
                FileWeakReference fwr = (FileWeakReference) iter.next();
                System.err.println(fwr.getFilename() + " not gc'ed");
            }
        }

        public void interrupt()
        {
            System.gc();
            System.runFinalization();
            super.interrupt();
        }
    }

    class FileWeakReference extends WeakReference
    {
        protected String filename;

        public FileWeakReference(File f) throws IOException
        {
            super(f);
            filename = f.getCanonicalPath();
        }

        public FileWeakReference(File f, ReferenceQueue refQ) throws IOException
        {
            super(f, refQ);
            filename = f.getCanonicalPath();
        }

        public void gc()
        {
            if (filename != null)
            {
                File f = new File(filename);
                f.delete();
                System.err.println(filename + " deleted");
                filename = null;
            }
        }

        public String getFilename()
        {
            return filename;
        }
    }
}