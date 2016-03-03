package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class DirectFileChannel extends FileChannel
{

    private static final Logger logger = LoggerFactory.getLogger(DirectFileChannel.class);

    static long blockSize = 512L;

    static long pageSize = 64L * 1024L;

    public interface LibC extends Library
    {
        // int read(int fd, byte[] buf, int count);
        int read(int fd, ByteBuffer buf, int count);

        int open(String path, int flags, int mode);

        int open(String path, int flags);

        int close(int fd);

        long lseek(int fd, long offset, int whence);

        int posix_memalign(PointerByReference pp, long alignment, long size);

        void free(Pointer p);

        String strerror(int errnum);

    }

    public static final LibC LIBC = (LibC) Native.loadLibrary(Platform.C_LIBRARY_NAME, LibC.class);

    private static final int O_RDONLY = 00000000;
    private static final int O_DIRECT;
    private static final int SEEK_SET = 0;
    private static final int S_IRUSR = 0000400;

    static ThreadLocal<List<ByteBuffer>> alignedBBPools = new ThreadLocal<List<ByteBuffer>>()
    {
        protected List<ByteBuffer> initialValue()
        {
            List<ByteBuffer> alignedBBPool = new ArrayList<>();
            int bufferSize = 256;
            // int bufferSize = (int) pageSize / 2;
            while ((bufferSize *= 2) < pageSize * 2)
            {
                PointerByReference pp = new PointerByReference();
                LIBC.posix_memalign(pp, bufferSize, bufferSize);
                ByteBuffer bb = getByteBuffer(Pointer.nativeValue(pp.getValue()), bufferSize);
                alignedBBPool.add(bb);
            }
            return alignedBBPool;
        }
    };

    static
    {
        Native.register(Platform.C_LIBRARY_NAME);

        if ("ppc64le".equals(System.getProperty("os.arch")))
            O_DIRECT = 00400000;
        else
            O_DIRECT = 00040000;
    }

    ByteBuffer getAlignedByteBuffer(int sizeAtLeast)
    {
        // if (true)
        // return alignedBB;

        List<ByteBuffer> pool = alignedBBPools.get();
        for (ByteBuffer ret : pool)
            if (sizeAtLeast <= ret.capacity())
                return ret;
        return pool.get(pool.size() - 1);
    }

    public static native void free(Pointer p);

    private static native int open(String pathname, int flags, int mode);

    public native int close(int fd);

    private static native NativeLong lseek(int fd, NativeLong offset, int whence);

    private static native NativeLong read(int fd, Pointer buf, NativeLong count);

    private static native String strerror(int errnum);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static ByteBuffer getByteBuffer(long address, int capacity)
    {
        try
        {
            Class cl = Class.forName("java.nio.DirectByteBuffer");
            Constructor constructor = cl.getDeclaredConstructor(Long.TYPE, Integer.TYPE, Object.class);
            constructor.setAccessible(true);
            return (ByteBuffer) constructor.newInstance(address, capacity, null);
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    final String path;
    final RandomAccessFile file;
    final FileChannel parent;
    final int fd;
    long pos = 0L;

    Thread cachedThread = null;
    ByteBuffer cachedBB = null;

    public DirectFileChannel(String path) throws IOException
    {
        this.path = path;
        this.file = new RandomAccessFile(path, "r");
        this.parent = file.getChannel();
        this.fd = LIBC.open(path, O_RDONLY | O_DIRECT);
        if (this.fd < 0)
            throw new IOException("open error: " + path + ", " + LIBC.strerror(Native.getLastError()));
    }

    // private ByteBuffer getAlignedByteBuffer(int size)
    // {
    // ByteBuffer ret = null;
    // for (int i = 0; i < alignedBBs.size(); ++i)
    // {
    // ret = alignedBBs.get(i);
    // if (size <= ret.capacity())
    // return ret;
    // }
    // return ret;
    // }

    public void finalize()
    {
        if (this.fd >= 0)
            LIBC.close(this.fd);
    }

    public long size() throws IOException
    {
        return parent.size();
    }

    public long position()
    {
        return pos;
    }

    public DirectFileChannel position(long pos)
    {
        //logger.info("pos: " + pos);
        this.pos = pos;
        this.cachedBB = null;
        this.cachedThread = null;
        return this;
    }

    @Override
    public int read(ByteBuffer bb) throws IOException
    {
        if (bb.remaining() > 1024)
        {
            // if (!Thread.currentThread().getName().contains("Thrift"))
            // if (bb.remaining() < pageSize)
            return readDIO(bb);
        }
        else if (Thread.currentThread() == cachedThread && cachedBB != null
                && bb.capacity() <= cachedBB.capacity())
        {
            // logger.info("read cache");
            return readCache(bb);
        }
        else
        {
            // if (cachedThread != null)
            // logger.info("miss cache: thread=" + (Thread.currentThread() == cachedThread) + ", cached="
            // + (cachedBB == null ? "null" : cachedBB.remaining()) + ", requested=" + bb.remaining());
            this.cachedBB = null;
            this.cachedThread = null;
            // if (path.contains("usertable"))
            // requestedRead(pos, bb.remaining());
            return readVFS(bb);
        }
    }

    public int readVFS(ByteBuffer bb) throws IOException
    {
        parent.position(pos);

        int read = parent.read(bb);
        if (read < 0)
            return read;

        pos += read;

        return read;
    }

    static AtomicLong requested = new AtomicLong();

    static ConcurrentHashMap<Long, AtomicInteger> fileStats = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer, AtomicInteger> stats = new ConcurrentHashMap<>();

    void requestedRead(long position, int size)
    {

        AtomicInteger count = stats.get(size);
        if (count == null)
        {
            count = new AtomicInteger();
            AtomicInteger tmp = stats.put(size, count);
            if (tmp != null)
                count = tmp;
        }
        count.incrementAndGet();

        String strKey = Thread.currentThread().getName();
        strKey = strKey.substring(0, Math.min(strKey.length(), 10));
        strKey = path;
        // count = fileStats.get(strKey);
        count = fileStats.get(pos);
        if (count == null)
        {
            count = new AtomicInteger();
            // AtomicInteger tmp = fileStats.put(strKey, count);
            AtomicInteger tmp = fileStats.put(pos, count);
            if (tmp != null)
                count = tmp;
        }
        count.incrementAndGet();

        if (requested.incrementAndGet() % 10000L == 0L)
        {

            try
            {
                throw new Exception();
            }
            catch (Exception ex)
            {
                logger.info("stack: ", ex);
            }

            // List<String> deleteList = new ArrayList<>();
            // for (Map.Entry<String, AtomicInteger> entry : fileStats.entrySet())
            // if (entry.getValue().get() == 1)
            // deleteList.add(entry.getKey());
            //
            // for (String delete : deleteList)
            // fileStats.remove(delete);

            // List<Map.Entry<String, AtomicInteger>> fileStatList = new ArrayList<>(fileStats.entrySet());
            List<Map.Entry<Long, AtomicInteger>> fileStatList = new ArrayList<>(fileStats.entrySet());
            Collections.sort(fileStatList, new Comparator<Map.Entry<Long, AtomicInteger>>()
            {
                @Override
                public int compare(Entry<Long, AtomicInteger> left, Entry<Long, AtomicInteger> right)
                {
                    return right.getValue().get() - left.getValue().get();
                }
            });

            StringBuffer buf = new StringBuffer("totalsize=").append(fileStats.size()).append("\n");
            for (int i = 0; i < Math.min(fileStatList.size(), 100); ++i)
                buf.append(fileStatList.get(i).getKey() + ":" + fileStatList.get(i).getValue() + "\n");

            logger.info(buf.toString());
        }
    }

    public int readCache(ByteBuffer bb) throws IOException
    {
        int ret = bb.remaining();
        bb.put(((ByteBuffer) cachedBB.limit(bb.remaining())).slice());
        cachedBB = null;
        cachedThread = null;
        return ret;
    }

    public int readDIO(ByteBuffer bb) throws IOException
    {
        // requestedRead(bb.remaining());
        // logger.info("################## direct_read: start: pos=" + pos + ", length=" + bb.remaining());

        long startAddr = pos;
        long endAddr = startAddr + bb.remaining();
        int size = bb.remaining();

        long alignedAddr = startAddr - startAddr % blockSize;
        long alignedEndAddr = endAddr + (blockSize - (endAddr % blockSize));

        if (LIBC.lseek(fd, alignedAddr, SEEK_SET) < 0)
            return -1;

        int ret = 0;
        while (ret < size)
        {
            ByteBuffer alignedBB = getAlignedByteBuffer((int) (alignedEndAddr - alignedAddr));
            alignedBB.clear();

            int readSize = Math.min((int) (alignedEndAddr - alignedAddr), alignedBB.capacity());

            if (logger.isDebugEnabled())
                logger.debug("direct_read: reading: pos=" + (pos + ret) + ", length=" + readSize + ",bb="
                        + alignedBB.capacity());

            cachedBB = null;
            cachedThread = null;

            int actualRead = LIBC.read(fd, alignedBB, readSize);
            if (actualRead < 0)
            {
                ret = ret == 0 ? -1 : ret;
                break;
            }

            int read = actualRead;
            alignedBB.rewind();

            if (logger.isDebugEnabled())
                logger.debug("direct_read: read: pos=" + (pos + ret) + ", length=" + readSize + ", bb="
                        + alignedBB.capacity() + ", read=" + read);

            if (readSize < alignedBB.capacity() || read != readSize || size <= read + ret)
            {
                boolean eof = read < readSize;

                if (alignedAddr < startAddr)
                {
                    alignedBB.position((int) (startAddr - alignedAddr));
                    read -= (int) (startAddr - alignedAddr);
                }
                if (ret + read > size)
                    read -= ret + read - size;

                if (logger.isDebugEnabled())
                    logger.debug("direct_read: adjusted: pos=" + (pos + ret) + ", length=" + readSize + ", bb="
                            + alignedBB.capacity() + ", read=" + read);

                int endPos = alignedBB.position() + read;
                bb.put(((ByteBuffer) alignedBB.limit(endPos)).slice());

                alignedBB.clear();
                if (actualRead != endPos)
                {
                    // logger.info("create cache");
                    cachedBB = ((ByteBuffer) alignedBB.position(endPos).limit(actualRead)).slice();
                    cachedBB.rewind();
                    cachedThread = Thread.currentThread();
                }

                if (eof)
                {
                    ret += read;
                    break;
                }
            }
            else
            {
                if (alignedAddr < startAddr)
                {
                    alignedBB.position((int) (startAddr - alignedAddr));
                    read -= (int) (startAddr - alignedAddr);
                }
                bb.put(alignedBB);
            }

            // shift

            alignedAddr += alignedBB.capacity();
            ret += read;
        }

        // requestedRead(pos, ret);

        if (ret > 0)
            pos += ret;

        return ret;
    }

    @Override
    public void force(boolean paramBoolean) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock lock(long paramLong1, long paramLong2, boolean paramBoolean) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode paramMapMode, long paramLong1, long paramLong2) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer paramByteBuffer, long paramLong) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] paramArrayOfByteBuffer, int paramInt1, int paramInt2) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(ReadableByteChannel paramReadableByteChannel, long paramLong1, long paramLong2)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferTo(long paramLong1, long paramLong2, WritableByteChannel paramWritableByteChannel)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel truncate(long paramLong) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(long paramLong1, long paramLong2, boolean paramBoolean) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer paramByteBuffer) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer paramByteBuffer, long paramLong) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] paramArrayOfByteBuffer, int paramInt1, int paramInt2) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void implCloseChannel() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public static void main(String[] args) throws Exception
    {
        String path = args[0];
        int size = Integer.parseInt(args[1]);
        int from = Integer.parseInt(args[2]);

        DirectFileChannel dch = new DirectFileChannel(path);
        FileChannel ch = new RandomAccessFile(path, "r").getChannel();

        dch.position(from);
        ch.position(from);

        ByteBuffer bb1 = ByteBuffer.allocate(size);
        ByteBuffer bb2 = ByteBuffer.allocate(size);

        dch.read(bb1);
        ch.read(bb2);

        bb1.rewind();
        bb2.rewind();
        System.out.println(bb1.equals(bb2));
    }

}
