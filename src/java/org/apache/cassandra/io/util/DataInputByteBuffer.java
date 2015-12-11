package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DataInputByteBuffer extends AbstractDataInput
{
    private final ByteBuffer bb;
    private int position = 0;

    public DataInputByteBuffer(ByteBuffer bb)
    {
        this.bb = bb;
    }

    public int read() throws IOException
    {
        return bb.get(position++) & 0xFF;
    }

    public void readFully(byte[] buffer, int offset, int count) throws IOException
    {
        bb.position(0);
        bb.get(buffer, offset, count);
        position += count;
    }

    protected void seekInternal(int pos)
    {
        position = pos;
    }

    protected int getPosition()
    {
        return position;
    }

    public int skipBytes(int n) throws IOException
    {
        seekInternal(getPosition() + n);
        return position;
    }

    public void close()
    {
        // do nothing.
    }
}