package org.apache.cassandra.cache.capi;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface PersistenceDriver {
    static final int BLOCK_SIZE = 4096;

    public static interface AsyncHandler {
        public void success(ByteBuffer bb);

        public void error(String msg);
    }

    public ByteBuffer read(long lba, int numOfBlocks) throws IOException;

    public void write(long lba, ByteBuffer buffer) throws IOException;

    public void readAsync(long lba, int numOfBlocks, AsyncHandler handler) throws IOException;

    public void writeAsync(long lba, ByteBuffer alignedBB, AsyncHandler handler) throws IOException;

    public boolean flush() throws IOException;

    public void close() throws IOException;

    public int remaining();

    public boolean process();

}
