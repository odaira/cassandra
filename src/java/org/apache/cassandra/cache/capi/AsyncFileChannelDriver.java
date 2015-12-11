package org.apache.cassandra.cache.capi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

public class AsyncFileChannelDriver extends PersistenceDriverImpl implements PersistenceDriver {
    private static final Logger logger = LoggerFactory.getLogger(AsyncFileChannelDriver.class);

    public static AtomicLong requested = new AtomicLong();
    public static AtomicLong executed = new AtomicLong();
    public static AtomicLong totalElapsed = new AtomicLong();

    private class Task {
        boolean write;
        AsyncHandler handler;
        long lba;
        ByteBuffer bb;
        long createdTs = System.currentTimeMillis();
        Future<Integer> future;

        boolean error = false;

        private Task(boolean write, long lba, AsyncHandler handler, ByteBuffer bb) {
            this.write = write;
            this.lba = lba;
            this.handler = handler;
            this.bb = bb;
        }

        boolean fork() {
            future = write ? channel.write(bb, lba * BLOCK_SIZE) : channel.read(bb, lba * BLOCK_SIZE);
            return true;
        }

        long waitingTs() {
            return System.currentTimeMillis() - createdTs;
        }

        void join() {
            try {
                if (future == null)
                    throw new IllegalStateException();

                long start = System.currentTimeMillis();
                while (!future.isDone())
                    ;
                long elapsed = System.currentTimeMillis() - start;

                executed.incrementAndGet();
                totalElapsed.addAndGet(elapsed);

                try {
                    if (future.get() != bb.capacity()) {
                        error = true;
                    } else if (!write)
                        bb.rewind();
                } catch (Exception ex) {
                    error = true;
                }

            } catch (Exception ex) {
                logger.error("file write error: " + ex.getMessage(), ex);
                handler.error("file " + (write ? "write" : "read") + " error: deviceName=" + deviceName + ", lba=" + lba);
            }
        }

        void report() {
            if (error)
                handler.error("file " + (write ? "write" : "read") + " error: deviceName=" + deviceName + ", lba=" + lba);
            else
                handler.success(bb);
        }
    }

    public class FlushThread extends Thread {

        public FlushThread() {
            super("file-Sync");
        }

        public void run() {
            while (!closed.get()) {
                while (!waiting.isEmpty() || !processing.isEmpty()) {
                    boolean processed = false;
                    while (process(false))
                        processed = true;
                    if (!processed)
                        processed = process(true);
                }
            }
        }
    }

    public class Flush implements Runnable {

        public void run() {
            if (closed.get())
                return;
            while (!waiting.isEmpty() || !processing.isEmpty()) {
                boolean processed = false;
                while (process(!processed))
                    processed = true;
                if (!processed)
                    processed = process(true);
            }
            flushTaskPool.add(this);
        }
    }

    static final int FLUSH_QUEUE_MAX_DEPTH = 256;

    final String deviceName;
    final AsynchronousFileChannel channel;
    final int numOfAsync;
    final int limitOfAsync;
    final ConcurrentLinkedQueue<Task> waiting = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Task> processing = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Flush> flushTaskPool = new ConcurrentLinkedQueue<>();
    final AtomicInteger numOfProcessing = new AtomicInteger(0);
    final AtomicInteger numOfReporting = new AtomicInteger(0);
    final AtomicBoolean closed = new AtomicBoolean(false);

    final ExecutorService service;

    public AsyncFileChannelDriver(String deviceName, int numOfAsync, int blockLimit) throws IOException {
        this.deviceName = deviceName;
        this.channel = AsynchronousFileChannel.open(Paths.get(deviceName), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        this.numOfAsync = numOfAsync;
        this.limitOfAsync = Math.max(numOfAsync - 10, 1);

        this.service = new ThreadPoolExecutor(10, 10, 1, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(FLUSH_QUEUE_MAX_DEPTH));

        for (int i = 0; i < FLUSH_QUEUE_MAX_DEPTH; ++i)
            flushTaskPool.add(new Flush());

        if (this.channel.size() < (long) blockLimit * ((long) BLOCK_SIZE - 1L)) {
            // INIT
            ByteBuffer empty = ByteBuffer.allocateDirect(BLOCK_SIZE);
            this.channel.write(empty, Math.max((long) BLOCK_SIZE, (long) blockLimit * ((long) BLOCK_SIZE - 1L)));
            this.channel.force(false);
        }

        //        new FlushThread().start();
    }

    public ByteBuffer read(long lba, int numOfBlocks) throws IOException {
        ByteBuffer bb = allocatePooled((int) (numOfBlocks * BLOCK_SIZE));
        Future<Integer> future = channel.read(bb, lba * BLOCK_SIZE);
        try {
            while (!future.isDone())
                ;
        } catch (Exception ex) {
            logger.error("file read error: deviceName=" + deviceName + ", lba=" + lba);
            throw new IOException(ex);
        }

        try {
            if (future.get() != bb.capacity())
                throw new IOException("file read error: " + deviceName);
        } catch (Exception ex) {
            throw new IOException("file read error: " + deviceName);
        }

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

        Future<Integer> future = channel.write(bb, lba * BLOCK_SIZE);
        try {
            while (!future.isDone())
                ;
        } catch (Exception ex) {
            logger.error("file read error: deviceName=" + deviceName + ", lba=" + lba);
            throw new IOException(ex);
        }

        try {
            if (future.get() != bb.capacity())
                throw new IOException("file write error: " + deviceName);
        } catch (Exception ex) {
            throw new IOException("file write error: " + deviceName);
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
        waiting.add(new Task(true, lba, handler, bb));
        requestProcess();
    }

    public void readAsync(long lba, int numOfBlocks, AsyncHandler handler) throws IOException {
        ByteBuffer bb = allocatePooled(numOfBlocks * BLOCK_SIZE);
        requested.incrementAndGet();
        waiting.add(new Task(false, lba, handler, bb));
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
            try {
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
                    }
                    return true;
                }
            } finally {
                numOfReporting.decrementAndGet();
            }
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
        channel.close();
        service.shutdown();
        closed.set(true);
    }

    public int remaining() {
        return numOfProcessing.get();
    }

}
