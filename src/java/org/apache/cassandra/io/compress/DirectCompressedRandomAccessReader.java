package org.apache.cassandra.io.compress;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.apache.cassandra.io.util.PoolingSegmentedFile;

public class DirectCompressedRandomAccessReader extends CompressedRandomAccessReader {

    DirectFileChannel ch = null;
    
    protected DirectCompressedRandomAccessReader(String dataFilePath, CompressionMetadata metadata, PoolingSegmentedFile owner) throws FileNotFoundException {
        super(dataFilePath, metadata, owner);
    }

    protected FileChannel getChannel0() throws FileNotFoundException {
        if (ch == null) {
            try {
                ch = new DirectFileChannel(getPath());
            } catch (IOException e) {
                throw new FileNotFoundException("direct access to " + getPath() + " is not prohibited.");
            }
        }

        return ch;
    }

}
