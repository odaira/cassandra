package org.apache.cassandra.cache.capi;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.ibm.research.capiblock.CapiBlockDevice;

public abstract class PersistenceDriverImpl implements PersistenceDriver {
    private static ConcurrentLinkedQueue<ByteBuffer> pooled4k = new ConcurrentLinkedQueue<ByteBuffer>();
    private static ConcurrentLinkedQueue<ByteBuffer> pooled8k = new ConcurrentLinkedQueue<ByteBuffer>();
    private static ConcurrentLinkedQueue<ByteBuffer> pooled16k = new ConcurrentLinkedQueue<ByteBuffer>();

    public static void clear() {
        pooled4k.clear();
        pooled8k.clear();
        pooled16k.clear();
    }

    public static class Identity {
        Object k;

        public Identity(Object k) {
            this.k = k;
        }

        public int hashCode() {
            return 0;
        }

        public boolean equals(Object other) {
            return other instanceof Identity && k == ((Identity) other).k;
        }
    }

    //static ConcurrentHashMap<Identity, Thread> allocationMap = new ConcurrentHashMap<>();

    public static ByteBuffer allocatePooled(int size) {
        ByteBuffer ret = null;
        if (size == BLOCK_SIZE)
            ret = pooled4k.poll();
        else if (size == 2 * BLOCK_SIZE)
            ret = pooled8k.poll();
        else if (size == 4 * BLOCK_SIZE)
            ret = pooled16k.poll();

        if (ret != null) {
            ret.rewind();
            ret.limit(ret.capacity());
        }

        if (ret != null) {
            //            if (allocationMap.putIfAbsent(new Identity(ret), Thread.currentThread()) != null)
            //                throw new IllegalStateException("already allocated. bb=" + ret);
            //allocationMap.remove(new Identity(ret));

            return ret.capacity() == size ? ret : ((ByteBuffer) ret.limit(size)).slice();
        } else {
            ret = ByteBuffer.allocateDirect(size);
            //            if (allocationMap.putIfAbsent(new Identity(ret), Thread.currentThread()) != null)
            //                throw new IllegalStateException("already allocated. bb=" + ret);
            return ret;
        }
    }

    public static void freePooled(ByteBuffer bb) {
        if (bb == null)
            return;
        
        if (true)
            return;

        //        Thread owner = allocationMap.remove(new Identity(bb));
        //        if (owner == null)
        //            throw new IllegalStateException("owner is differnet. ideal=" + owner + ", actual=" + Thread.currentThread());

        //        if (allocationMap.putIfAbsent(new Identity(bb), Thread.currentThread()) != null)
        //            throw new IllegalStateException("already reed. bb=" + bb);

        if (bb.capacity() == BLOCK_SIZE)
            pooled4k.add(bb);
        else if (bb.capacity() == 2 * BLOCK_SIZE)
            pooled8k.add(bb);
        else if (bb.capacity() == 4 * BLOCK_SIZE)
            pooled16k.add(bb);
        else {
            try {
                System.err.println("size=" + bb.capacity());
                throw new Exception();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

    }

    public static String summary(ByteBuffer bb) {
        if (bb == null)
            return "NULL";

        StringBuffer ret = new StringBuffer();

        for (int i = 0; i < 10; ++i)
            if (i < bb.capacity())
                ret.append(Byte.toString(bb.get(i)));

        ret.append("...");

        bb.rewind();

        return ret.toString();

    }

    public static String summary(byte[] b) {
        if (b == null)
            return "NULL";

        StringBuffer ret = new StringBuffer();

        for (int i = 0; i < 10; ++i)
            if (i < b.length)
                ret.append(Byte.toString(b[i]));

        ret.append("...");

        return ret.toString();

    }

    public static long getLBASize(long size) {
        if (size % (long) CapiBlockDevice.BLOCK_SIZE == 0L)
            return size / (long) CapiBlockDevice.BLOCK_SIZE;
        else
            return size / (long) CapiBlockDevice.BLOCK_SIZE + 1;
    }

    public static int getLBASize(int size) {
        if (size % CapiBlockDevice.BLOCK_SIZE == 0)
            return size / CapiBlockDevice.BLOCK_SIZE;
        else
            return size / CapiBlockDevice.BLOCK_SIZE + 1;
    }

    public static int getAlignedSize(int size) {
        return getLBASize(size) * PersistenceDriver.BLOCK_SIZE;
    }

    public static int DEFAULT_NUMBER_OF_EXECUTOR_CORETHREADS = 1;

    public PersistenceDriverImpl() {
    }

}
