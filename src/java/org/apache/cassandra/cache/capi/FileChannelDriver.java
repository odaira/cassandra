package org.apache.cassandra.cache.capi;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class FileChannelDriver extends PersistenceDriverImpl implements PersistenceDriver {
    final File file;
    final RandomAccessFile rfile;
    final FileChannel channel;
    final int numOfAsync;

    private class QueueEntry {
        final boolean write;
        final AsyncHandler handler;
        final ByteBuffer bb;
        final long position;

        public QueueEntry(boolean write, AsyncHandler handler, long position, ByteBuffer bb) {
            this.write = write;
            this.handler = handler;
            this.position = position;
            this.bb = bb;
        }

        private void handle() {
            if (write) {
                //System.out.println("write: " + summary(bb));
                try {
                    while (true) {
                        int written;
                        long cur = position;
                        while ((written = channel.write(bb, cur)) != 0) {
                            cur += (long) written;
                        }

                        bb.limit(0);

                        for (int i = bb.position(); i < bb.limit(); ++i)
                            bb.put((byte) 0);

                        channel.force(false);

                        bb.limit(bb.capacity());
                        bb.rewind();

                        handler.success(null);
                        break;
                    }
                } catch (IOException e) {
                    handler.error(e.getMessage());
                }
            } else {
                try {
                    long cur = position;
                    int read = 0;
                    while ((read = channel.read(bb, cur)) != 0)
                        cur += read;

                    bb.limit(bb.capacity());
                    channel.force(true);
                    bb.rewind();
                    //System.out.println("read: " + summary(bb));
                    handler.success(bb);
                } catch (IOException e) {
                    handler.error(e.getMessage());
                }
            }
        }
    }

    final ConcurrentLinkedQueue<QueueEntry> buffers = new ConcurrentLinkedQueue<>();
    final AtomicInteger lazyBufferCounter = new AtomicInteger(0);

    public FileChannelDriver(String path, int numOfAsync, int blockLimit) throws IOException {
        this.file = new File(path);
        this.rfile = new RandomAccessFile(file, "rws");
        this.channel = rfile.getChannel();
        this.numOfAsync = numOfAsync;

        // System.out.println(path + ": " + this.channel.size());

        if (this.channel.size() < (long) blockLimit * ((long) BLOCK_SIZE - 1L)) {
            // INIT
            ByteBuffer empty = ByteBuffer.allocateDirect(BLOCK_SIZE);
            this.channel.write(empty, Math.max((long) BLOCK_SIZE, (long) blockLimit * ((long) BLOCK_SIZE - 1L)));
            this.channel.force(false);
        }
    }

    public String toString() {
        return "[file=" + file + "]";
    }

    @Override
    public ByteBuffer read(long lba, int numOfBlocks) throws IOException {
        channel.force(false);
        ByteBuffer ret = ByteBuffer.allocateDirect((int) numOfBlocks * BLOCK_SIZE);
        channel.read(ret, lba * BLOCK_SIZE);
        ret.position(0);
        return ret;
    }

    @Override
    public void write(long lba, ByteBuffer bb) throws IOException {
        ByteBuffer original = null;
        if (bb.capacity() % BLOCK_SIZE != 0) {
            original = bb;
            bb = ByteBuffer.allocateDirect(original.capacity());
            bb.put(original);
            bb.rewind();
        }
        channel.write(bb, lba * BLOCK_SIZE);
        channel.force(false);

    }

    @Override
    public void readAsync(long lba, int numOfBlocks, AsyncHandler handler) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(numOfBlocks * BLOCK_SIZE);
        // System.out.println("read: " + lba * BLOCK_SIZE + ":" + summary(bb));

        QueueEntry entry = new QueueEntry(false, handler, lba * BLOCK_SIZE, bb);
        buffers.add(entry);

        int counter = lazyBufferCounter.incrementAndGet();
        while (counter >= numOfAsync) {
            QueueEntry finished = buffers.poll();
            counter = lazyBufferCounter.decrementAndGet();
            if (finished != null)
                finished.handle();
        }
    }

    @Override
    public void writeAsync(long lba, ByteBuffer bb, AsyncHandler handler) throws IOException {
        ByteBuffer original = null;
        if (bb.capacity() % BLOCK_SIZE != 0) {
            original = bb;
            bb = ByteBuffer.allocateDirect(original.capacity());
            bb.put(original);
            bb.rewind();
        }

        //System.out.println("write: " + lba * BLOCK_SIZE + ":" + summary(bb));

        QueueEntry entry = new QueueEntry(true, handler, lba * BLOCK_SIZE, bb);

        buffers.add(entry);

        int counter = lazyBufferCounter.incrementAndGet();
        while (counter >= numOfAsync) {
            channel.force(false);
            QueueEntry finished = buffers.poll();
            counter = lazyBufferCounter.decrementAndGet();
            if (finished != null)
                finished.handle();
        }
    }

    public boolean process() {
        QueueEntry entry = buffers.poll();
        if (entry == null)
            return false;
        lazyBufferCounter.decrementAndGet();
        entry.handle();
        return true;
    }

    public boolean flush() throws IOException {
        boolean ret = false;
        QueueEntry entry;

        while (!buffers.isEmpty()) {
            channel.force(false);
            while ((entry = buffers.poll()) != null) {
                lazyBufferCounter.decrementAndGet();
                entry.handle();
            }
            ret = true;
        }
        return ret;
    }

    public void close() throws IOException {
        flush();
        rfile.close();
    }

    public int remaining() {
        return lazyBufferCounter.get();
    }

}
