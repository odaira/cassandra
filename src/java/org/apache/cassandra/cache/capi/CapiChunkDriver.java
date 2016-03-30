package org.apache.cassandra.cache.capi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.research.capiblock.CapiBlockDevice;
import com.ibm.research.capiblock.Chunk;

public class CapiChunkDriver extends PersistenceDriverImpl implements PersistenceDriver {
    private static final Logger logger = LoggerFactory.getLogger(CapiChunkDriver.class);

    public static AtomicLong requested = new AtomicLong();
    public static AtomicLong executed = new AtomicLong();
    public static AtomicLong totalElapsed = new AtomicLong();

    public static final boolean pooled = System.getProperty("capi.driver.task.nopool") == null;
    public static final int numOfSubThreads = Integer.parseInt(System.getProperty("capi.driver.thread.sub", "32"));
    public static final int numOfCoreThreads = Integer.parseInt(System.getProperty("capi.driver.thread.core", "1"));

    private class Task {
        boolean write;
        AsyncHandler handler;
        long lba;
        ByteBuffer bb;
        long createdTs;
        Future<Long> future;
        Chunk chunk = getNextChunk();

        boolean error = false;

        private Task() {
        }

        Task init(boolean write, long lba, AsyncHandler handler, ByteBuffer bb) {
            this.write = write;
            this.lba = lba;
            this.handler = handler;
            this.bb = bb;
            this.createdTs = System.currentTimeMillis();
            this.future = null;
            this.error = false;
            return this;
        }

        boolean fork() {
            try {
                int numOfBlocks = getLBASize(bb.capacity());
                future = write ? chunk.writeBlockAsync(lba, numOfBlocks, bb) : chunk.readBlockAsync(lba, numOfBlocks, bb);
                return true;
            } catch (IOException ex) {
                logger.error("capi write error: " + ex.getMessage(), ex);
                handler.error("capi " + (write ? "write" : "read") + " error: deviceName=" + chunk2Device.get(chunk) + ", lba=" + lba);
                return false;
            }
        }

        long waitingTs() {
            return System.currentTimeMillis() - createdTs;
        }

        void join() {
            try {
                long rc = 0;

                if (future == null)
                    throw new IllegalStateException();

                long start = System.currentTimeMillis();
                while ((rc = future.get(20L, TimeUnit.SECONDS)) == 0L)
                    ;
                long elapsed = System.currentTimeMillis() - start;

                executed.incrementAndGet();
                totalElapsed.addAndGet(elapsed);

                if (rc < 0)
                    error = true;
                else if (!write)
                    bb.rewind();

            } catch (Exception ex) {
                logger.error("capi write error: " + ex.getMessage(), ex);
                handler.error("capi " + (write ? "write" : "read") + " error: deviceName=" + chunk2Device.get(chunk) + ", lba=" + lba);
            }
        }

        void report() {
            if (error)
                handler.error("capi " + (write ? "write" : "read") + " error: deviceName=" + chunk2Device.get(chunk) + ", lba=" + lba);
            else
                handler.success(bb);

            freeTask(this);
        }
    }

    public class FlushThread extends Thread {

        public FlushThread() {
            super("CAPI-Sync");
        }

        public void run() {
            while (!closed.get()) {
                while (!waiting.isEmpty() || !processing.isEmpty()) {
                    process(true);
                }
            }
        }
    }

    public class Flush implements Runnable {

        public void run() {
            if (closed.get())
                return;
            process(false);
            flushTaskPool.add(this);
        }
    }

    static final int FLUSH_QUEUE_MAX_DEPTH = 256;
    static final int TASK_POOL_COUNT = 256;
    static final int TASK_POOL_LENGTH = 32;

    final String[] deviceNames;
    final Chunk[] chunks;
    final Map<Chunk, String> chunk2Device = new HashMap<>();
    final int numOfAsync;
    final int limitOfAsync;
    final ConcurrentLinkedQueue<Task> waiting = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Task> processing = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Flush> flushTaskPool = new ConcurrentLinkedQueue<>();
    final AtomicInteger numOfProcessing = new AtomicInteger(0);
    final AtomicInteger numOfReporting = new AtomicInteger(0);
    final AtomicBoolean closed = new AtomicBoolean(false);
    @SuppressWarnings("unchecked")
    final List<Task>[] pooledTasks = new List[TASK_POOL_COUNT];
    final AtomicInteger nextChunk = new AtomicInteger(0);

    final ExecutorService service;

    public CapiChunkDriver(String deviceName, int numOfAsync) throws IOException {
        this(new String[] { deviceName }, numOfAsync);
    }

    public CapiChunkDriver(String[] deviceNames, int numOfAsync) throws IOException {
        this.deviceNames = deviceNames;
        this.chunks = new Chunk[deviceNames.length];
        for (int i = 0; i < chunks.length; ++i) {
            this.chunks[i] = CapiBlockDevice.getInstance().openChunk(deviceNames[i], numOfAsync);
            this.chunk2Device.put(this.chunks[i], deviceNames[i]);
        }

        this.numOfAsync = numOfAsync;
        this.limitOfAsync = Math.max(numOfAsync - numOfSubThreads, 1);

        this.service = new ThreadPoolExecutor(1, numOfSubThreads, 1, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(FLUSH_QUEUE_MAX_DEPTH));

        for (int i = 0; i < FLUSH_QUEUE_MAX_DEPTH; ++i)
            flushTaskPool.add(new Flush());

        for (int i = 0; i < TASK_POOL_COUNT; ++i)
            pooledTasks[i] = new ArrayList<>();

        for (int i = 0; i < numOfCoreThreads; ++i)
            new FlushThread().start();
    }

    public String toString() {
        return "[capichunkdriver path=" + chunk2Device.values() + "]";
    }

    private Chunk getNextChunk() {
        return chunks[Math.abs(nextChunk.incrementAndGet() % chunks.length)];
    }

    public ByteBuffer read(long lba, int numOfBlocks) throws IOException {
        ByteBuffer bb = allocatePooled((int) (numOfBlocks * CapiBlockDevice.BLOCK_SIZE));
        Chunk chunk = getNextChunk();
        Future<Long> future = chunk.readBlockAsync(lba, numOfBlocks, bb);
        long rc = 0L;
        try {
            while ((rc = future.get(10L, TimeUnit.SECONDS)) == 0L)
                ;
        } catch (Exception ex) {
            logger.error("capi read error: deviceName=" + chunk2Device.get(chunk) + ", lba=" + lba);
            throw new IOException(ex);
        }

        if (rc != numOfBlocks)
            throw new IOException("capi device error: " + chunk2Device.get(chunk));

        return bb;
    }

    public void write(long lba, ByteBuffer bb) throws IOException {
        ByteBuffer original = null;
        if (bb.capacity() % BLOCK_SIZE != 0) {
            original = bb;
            bb = allocatePooled(original.capacity());
            bb.put(original);
            bb.rewind();
        }

        int numOfBlocks = getLBASize(bb.capacity());
        Chunk chunk = getNextChunk();

        Future<Long> future = chunk.writeBlockAsync(lba, numOfBlocks, bb);
        long rc = 0L;
        try {
            while ((rc = future.get(10L, TimeUnit.SECONDS)) == 0L)
                ;
        } catch (Exception ex) {
            logger.error("capi read error: deviceName=" + chunk2Device.get(chunk) + ", lba=" + lba);
            throw new IOException(ex);
        }

        if (rc != numOfBlocks)
            throw new IOException("capi device error: " + chunk2Device.get(chunk));
    }

    private Task newTask(long lba) {
        Task ret = null;
        if (pooled) {
            int poolIdx = Math.abs((int) lba % TASK_POOL_COUNT);
            List<Task> pool = pooledTasks[poolIdx];
            synchronized (pool) {
                if (!pool.isEmpty())
                    ret = pool.remove(pool.size() - 1);
            }
        }
        if (ret == null)
            ret = new Task();
        return ret;
    }

    private void freeTask(Task task) {
        if (pooled) {
            int poolIdx = Math.abs((int) task.lba % TASK_POOL_COUNT);
            List<Task> pool = pooledTasks[poolIdx];
            synchronized (pool) {
                if (pool.size() < TASK_POOL_LENGTH)
                    pool.add(task);
            }
        }
    }

    public void writeAsync(long lba, ByteBuffer bb, AsyncHandler handler) throws IOException {
        ByteBuffer original = null;
        if (bb.capacity() % BLOCK_SIZE != 0) {
            original = bb;
            bb = allocatePooled(original.capacity());
            bb.put(original);
            bb.rewind();
        }
        requested.incrementAndGet();
        waiting.add(newTask(lba).init(true, lba, handler, bb));
        requestProcess();
    }

    public void readAsync(long lba, int numOfBlocks, AsyncHandler handler) throws IOException {
        ByteBuffer bb = allocatePooled(numOfBlocks * CapiBlockDevice.BLOCK_SIZE);
        requested.incrementAndGet();
        waiting.add(newTask(lba).init(false, lba, handler, bb));
        requestProcess();
    }

    private void requestProcess() {
        Flush flush = flushTaskPool.poll();
        if (flush == null)
            return;
        service.submit(flush);
    }

    public boolean process() {
        return process(true);
    }

    private boolean process(boolean force) {
        boolean needToJoin = false || force;
        while (true) {
            int current = numOfProcessing.get();

            if (current >= numOfAsync) {
                needToJoin = true;
                break; // need process
            }

            if (!numOfProcessing.compareAndSet(current, current + 1))
                continue;

            Task task = waiting.poll();
            if (task == null)
                numOfProcessing.decrementAndGet();
            else if (task.fork())
                processing.add(task);
            break;
        }

        if (!needToJoin) {
            Task tmp = processing.peek();
            needToJoin = tmp != null && tmp.waitingTs() > 10L;
        }

        Task joininig;
        if (needToJoin) {
            numOfReporting.incrementAndGet();
            if ((joininig = processing.poll()) != null) {
                joininig.join();
                try {
                    Task next = waiting.poll();
                    if (next == null) {
                        numOfProcessing.decrementAndGet();
                    } else if (next.fork()) {
                        processing.add(next);
                    }
                } finally {
                    if (joininig != null)
                        joininig.report();
                    numOfReporting.decrementAndGet();
                }
                return true;
            }
            numOfReporting.decrementAndGet();
        }
        return false;
    }

    public boolean flush() throws IOException {
        boolean ret = false;
        while (!processing.isEmpty() || !waiting.isEmpty() || numOfProcessing.get() != 0 || numOfReporting.get() != 0) {
            ret = true;
            process(true);
            //System.out.println(processing.size() + ":" + waiting.size() + ":" + numOfProcessing + ":" + numOfReporting);
        }
        return ret;
    }

    public void close() throws IOException {
        flush();
        for (Chunk chunk : chunks)
            chunk.close();

        service.shutdown();
        closed.set(true);
    }

    public int remaining() {
        return numOfProcessing.get();
    }

}
