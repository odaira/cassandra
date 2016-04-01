package org.apache.cassandra.cache.capi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.cache.capi.CapiChunkDriver.AsyncHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.research.capiblock.CapiBlockDevice;

public class StorageManager {
    private static final Logger logger = LoggerFactory.getLogger(StorageManager.class);

    public static int FOOTER_SIZE = 1;

    public static long ALLOCATE_LBA_SIZE = Long.parseLong(System.getProperty("capi.allocator.grow", "1024"));

    public static class OutOfStorageException extends IOException {
        private static final long serialVersionUID = 1L;

        public OutOfStorageException(String msg) {
            super(msg);
        }

    }

    private List<CapiChunkDriver> drivers = new ArrayList<>();
    private List<Long> driver2StartLBA = new ArrayList<>();
    private long lbaSizeForEach = 0;
    private boolean initialized = false; // must be volatile for safty
    private boolean closed = false;// must be volatile for safety
    private long limit;

    private AtomicInteger nextAllocateIdx = new AtomicInteger(-1);
    private List<AtomicLong> freeLBAs = new ArrayList<>();
    private List<AtomicLong> persistedFreeLBAs = new ArrayList<>();
    private List<ByteBuffer> footers = new ArrayList<>();

    private ConcurrentLinkedQueue<Long> freed4List = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<Long> freed8List = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<Long> freed16List = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<Long> freed32List = new ConcurrentLinkedQueue<>();
    private ConcurrentHashMap<Long, Integer> freedList = new ConcurrentHashMap<>();

    public StorageManager() {
    }

    void add(CapiChunkDriver driver, long startLba, long lbaSize) {
        assert(driver != null);
        assert(startLba >= 0);
        assert(lbaSize > 0);

        if (initialized)
            throw new IllegalStateException("already initialized.");

        drivers.add(driver);
        driver2StartLBA.add(startLba);
        freeLBAs.add(new AtomicLong(0));
        persistedFreeLBAs.add(new AtomicLong(0));

        if (lbaSizeForEach == 0L)
            lbaSizeForEach = lbaSize - FOOTER_SIZE;
        else
            lbaSizeForEach = Math.min(lbaSizeForEach, lbaSize - FOOTER_SIZE);
    }

    public void initialize() throws IOException {
        initialize(true);
    }

    public synchronized void initialize(boolean clean) throws IOException {
        if (initialized)
            throw new IllegalStateException("already initialized.");

        if (clean)
            initFooters();
        else
            recoverFooters();

        limit = (long) drivers.size() * (long) lbaSizeForEach * (long) CapiBlockDevice.BLOCK_SIZE;

        initialized = true;

    }

    private void reset(ByteBuffer bb) {
        bb.rewind();
        bb.limit(bb.capacity());
    }

    private void initFooters() throws IOException {
        final ConcurrentLinkedQueue<String> errMsgs = new ConcurrentLinkedQueue<>();
        for (int deviceIdx = 0; deviceIdx < drivers.size(); ++deviceIdx) {
            CapiChunkDriver driver = drivers.get(deviceIdx);

            long initSize = ALLOCATE_LBA_SIZE;
            if (ALLOCATE_LBA_SIZE >= lbaSizeForEach)
                initSize = 0L;

            long initFreeLba = freeLBAs.get(deviceIdx).get() + initSize;
            persistedFreeLBAs.set(deviceIdx, new AtomicLong(initFreeLba));
            long footerLBA = getFooterLBA(deviceIdx);

            final ByteBuffer footer = getByteBuffer(CapiBlockDevice.BLOCK_SIZE * FOOTER_SIZE);
            footers.add(footer);

            reset(footer);
            footer.putLong(initFreeLba); // 1. freePoint
            reset(footer);

            if (logger.isDebugEnabled())
                logger.debug("write footer: driver=" + driver + ", footer(" + footerLBA + ")=" + "[free_from=" + initFreeLba + "]");

            driver.writeAsync(footerLBA, footer, new AsyncHandler() {
                @Override
                public void success(ByteBuffer bb) {
                }

                @Override
                public void error(String msg) {
                    errMsgs.add(msg);
                }
            });
        }

        for (CapiChunkDriver driver : drivers)
            driver.flush();

        if (!errMsgs.isEmpty())
            throw new IOException(errMsgs.toString());

        // for (int deviceIdx = 0; deviceIdx < drivers.size(); ++deviceIdx)
        // {
        // System.out.println(AbstractPersistencDriverImpl.summary(drivers.get(deviceIdx).read(getFooterLBA(deviceIdx),
        // 1)));
        // }
    }

    private void recoverFooters() throws IOException {
        synchronized (StorageManager.this) {
            for (int i = 0; i < drivers.size(); ++i)
                footers.add(null);
        }

        final ConcurrentLinkedQueue<String> errMsgs = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < drivers.size(); ++i) {
            final int deviceIdx = i;
            final CapiChunkDriver driver = drivers.get(deviceIdx);
            final long footerLBA = getFooterLBA(deviceIdx);

            ByteBuffer footer = driver.read(footerLBA, FOOTER_SIZE);

            reset(footer);
            footers.set(deviceIdx, footer);

            long persistedFreeLBA = footer.getLong();
            if (persistedFreeLBA < 0 || persistedFreeLBA > lbaSizeForEach) {
                errMsgs.add("restore error. invalid free lba. " + "device=" + driver //
                        + ", freeLBA=" + persistedFreeLBA);
            } else {
                persistedFreeLBAs.set(deviceIdx, new AtomicLong(persistedFreeLBA));
                AtomicLong currentReserved = freeLBAs.get(deviceIdx);
                if (persistedFreeLBA < currentReserved.get()) {
                    errMsgs.add("reserved area is too large: " + "device=" + driver //
                            + ", freeLBA=" + persistedFreeLBA + ", reserveLBA=" + currentReserved.get());
                }

                freeLBAs.set(deviceIdx, new AtomicLong(persistedFreeLBA));
            }

            logger.info("read footer: driver=" + deviceIdx + ":" + driver + ", tail=" + persistedFreeLBA);
        }

        if (!errMsgs.isEmpty())
            throw new IOException(errMsgs.toString());

        synchronized (this) {
            boolean finish = true;
            for (int i = 0; i < footers.size(); ++i) {
                if (footers.get(i) == null) {
                    finish = false;
                    break;
                }
            }
            if (!finish) {
                int numOfTries = 0;
                while (true) {
                    finish = true;
                    try {
                        this.wait();
                    } catch (InterruptedException ex) {
                        throw new IllegalStateException(ex);
                    }

                    for (int i = 0; i < footers.size(); ++i) {
                        if (footers.get(i) == null) {
                            finish = false;
                            logger.error("footer initialization error: index=" + i);
                        }
                    }

                    if (finish)
                        break;

                    if (++numOfTries > footers.size()) {
                        logger.error("footer initialization error");
                        throw new IllegalStateException("footer initialization error");
                    }

                }
            }
        }

        for (int driverIdx = 0; driverIdx < drivers.size(); ++driverIdx)
            extendSize(driverIdx);

        for (CapiChunkDriver driver : drivers)
            driver.flush();
    }

    public void flush() throws IOException {
        checkActive();

        for (CapiChunkDriver driver : drivers)
            driver.flush();
    }

    public synchronized void close() throws IOException {
        if (closed)
            return;

        for (CapiChunkDriver driver : drivers)
            driver.flush();

        for (CapiChunkDriver driver : drivers)
            driver.close();

        closed = true;
    }

    private long getFooterLBA(int driverIdx) {
        return driver2StartLBA.get(driverIdx) + lbaSizeForEach + 1L;
    }

    private Set<CapiChunkDriver> lockedDrivers = new HashSet<>();

    private void extendSize(int driverIdx) throws IOException {
        final AtomicLong persistedFreeLBA = persistedFreeLBAs.get(driverIdx);
        AtomicLong freeLBA = freeLBAs.get(driverIdx);
        final CapiChunkDriver driver = drivers.get(driverIdx);
        ByteBuffer footer = footers.get(driverIdx);
        long footerLba = getFooterLBA(driverIdx);

        final long nextPersistedFreeLBA;
        synchronized (lockedDrivers) {
            while (lockedDrivers.contains(driver)) {
                try {
                    lockedDrivers.wait();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }

            long currentPersistedFreeLBA = persistedFreeLBA.get();
            if (persistedFreeLBA.get() - freeLBA.get() > ALLOCATE_LBA_SIZE)
                return;

            nextPersistedFreeLBA = currentPersistedFreeLBA + ALLOCATE_LBA_SIZE;
            if (nextPersistedFreeLBA > lbaSizeForEach)
                throw new OutOfStorageException("driver=" + driver + ", lba=" + nextPersistedFreeLBA);

            reset(footer);
            footer.putLong(nextPersistedFreeLBA);

            lockedDrivers.add(driver);
        }

        try {
            reset(footer);
            driver.write(footerLba, footer);
            synchronized (lockedDrivers) {
                persistedFreeLBA.set(nextPersistedFreeLBA);
                lockedDrivers.remove(driver);
                lockedDrivers.notifyAll();
                //logger.info("extended. driver=" + driver + ", tail=" + nextPersistedFreeLBA);
            }
        } catch (IOException ex) {
            synchronized (lockedDrivers) {
                persistedFreeLBA.set(nextPersistedFreeLBA);
                lockedDrivers.remove(driver);
            }
            logger.error(ex.getMessage(), ex);
        }

        // driver.writeAsync(footerLba, footer, new AsyncHandler()
        // {
        //
        // @Override
        // public void success(ByteBuffer bb)
        // {
        // synchronized (lockedDrivers)
        // {
        // persistedFreeLBA.set(nextPersistedFreeLBA);
        // lockedDrivers.remove(driver);
        // lockedDrivers.notifyAll();
        // }
        // }
        //
        // @Override
        // public void error(String msg)
        // {
        // synchronized (lockedDrivers)
        // {
        // persistedFreeLBA.set(nextPersistedFreeLBA);
        // lockedDrivers.remove(driver);
        // }
        // logger.error(msg);
        // }
        // });

    }

    int getNumberOfDevices() {
        return drivers.size();
    }

    private void checkActive() {
        if (!initialized)
            throw new IllegalStateException("not initialized.");
        if (closed)
            throw new IllegalStateException("already closed.");
    }

    long getLimitForEachDriver() {
        checkActive();
        return lbaSizeForEach;
    }

    long getLimit() {
        checkActive();
        return limit;
    }

    public long reserve(CapiChunkDriver driver, long sizeOfBytesInt) throws IOException {
        if (initialized)
            throw new IllegalStateException("already initialized.");

        checkSizeAlignment(sizeOfBytesInt);

        int driverIdx = drivers.indexOf(driver);
        if (driverIdx < 0)
            throw new IllegalArgumentException();

        int numOfBlocks = (int) (sizeOfBytesInt / (long) CapiBlockDevice.BLOCK_SIZE);

        return allocate0(driverIdx, numOfBlocks);
    }

    public long allocate(long sizeOfBytes) throws IOException {
        checkActive();
        checkSizeAlignment(sizeOfBytes);

        if (sizeOfBytes > Integer.MAX_VALUE)
            throw new IllegalArgumentException("each block must be less than " + Integer.MAX_VALUE);

        int sizeOfBytesInt = (int) sizeOfBytes;

        int numOfBlocks = sizeOfBytesInt / CapiBlockDevice.BLOCK_SIZE;

        Long ret = null;
        if (numOfBlocks == 1)
            ret = freed4List.poll();
        else if (numOfBlocks == 2)
            ret = freed8List.poll();
        else if (numOfBlocks == 4)
            ret = freed16List.poll();
        else if (numOfBlocks == 8)
            ret = freed32List.poll();
        else {
            for (Map.Entry<Long, Integer> entry : freedList.entrySet()) {
                if (entry == null)
                    continue;
                Long address = entry.getKey();
                Integer freedSize = entry.getValue();
                if (freedSize.intValue() == numOfBlocks) {
                    freedSize = freedList.remove(address);
                    if (freedSize == null)
                        continue;
                    ret = address;
                    break;
                }
            }
        }

        if (ret != null)
            return ret;

        int driverIdx = nextAllocateIdx.incrementAndGet() % drivers.size();
        return allocate0(driverIdx, numOfBlocks);
    }

    private long allocate0(int driverIdx, int numOfBlocks) throws IOException {
        AtomicLong freeLBA = freeLBAs.get(driverIdx);
        if (freeLBA.get() + (long) numOfBlocks > lbaSizeForEach)
            throw new OutOfStorageException("no storage space");

        long lba = freeLBA.addAndGet(numOfBlocks);

        if (lba >= lbaSizeForEach) {
            freeLBA.addAndGet((long) ((-1) * numOfBlocks));
            throw new OutOfStorageException("no storage space");
        }

        if (initialized) {
            while (persistedFreeLBAs.get(driverIdx).get() < lba)
                extendSize(driverIdx);
        }

        lba -= (long) numOfBlocks;

        //System.err.println("allocated: driver=" + drivers.get(driverIdx) + ", memory=" + lba + "-" + (lba + numOfBlocks));

        long address = ((long) driverIdx * lbaSizeForEach + lba) * (long) CapiBlockDevice.BLOCK_SIZE;
        return address;
    }

    public void free(long address, long sizeOfBytes) {
        checkActive();
        checkSizeAlignment(sizeOfBytes);

        if (sizeOfBytes > Integer.MAX_VALUE)
            throw new IllegalArgumentException("each block must be less than " + Integer.MAX_VALUE);

        int sizeOfBytesInt = (int) sizeOfBytes;

        int numOfBlocks = sizeOfBytesInt / CapiBlockDevice.BLOCK_SIZE;
        if (numOfBlocks == 1)
            freed4List.add(address);
        else if (numOfBlocks == 2)
            freed8List.add(address);
        else if (numOfBlocks == 4)
            freed16List.add(address);
        else if (numOfBlocks == 8)
            freed32List.add(address);
        else
            freedList.put(address, numOfBlocks);
    }

    public ByteBuffer getByteBuffer(long sizeOfBytes) {
        if (sizeOfBytes > Integer.MAX_VALUE)
            throw new IllegalArgumentException("each block must be less than " + Integer.MAX_VALUE);

        int sizeOfBytesInt = (int) sizeOfBytes;

        ByteBuffer bb = ByteBuffer.allocateDirect(sizeOfBytesInt);

        return bb;
    }

    class RealAddressRange {
        CapiChunkDriver driver;
        long startLBA;
        long endLBA;

        int numOfReads = 0;
        int numOfWrites = 0;

        RealAddressRange prev = null;
        RealAddressRange next = null;

        boolean isOverlapedWith(RealAddressRange other) {
            if (driver != other.driver)
                return false;
            if (startLBA < other.startLBA && other.startLBA < endLBA)
                return true;
            if (startLBA < other.endLBA && other.endLBA < endLBA)
                return true;
            if (other.startLBA < startLBA && endLBA < other.endLBA)
                return true;
            return false;
        }

        public String toString() {
            return "[ " + driver + ":" + startLBA + "-" + endLBA + "]";
        }

        public int size() {
            return (int) (endLBA - startLBA) * CapiBlockDevice.BLOCK_SIZE;
        }

        ByteBuffer read() throws IOException {
            int numOfBlocks = (int) (endLBA - startLBA);
            ByteBuffer bb = driver.read(startLBA, numOfBlocks);
            return bb;
        }

        void readAsync(final AsyncHandler handler) throws IOException {
            int numOfBlocks = (int) (endLBA - startLBA);
            driver.readAsync(startLBA, numOfBlocks, handler);
        }

        void write(ByteBuffer bb) throws IOException {
            driver.write(startLBA, bb);
        }

        void writeAsync(final ByteBuffer bb, final AsyncHandler handler) throws IOException {
            driver.writeAsync(startLBA, bb, handler);
        }
    }

    private void checkSizeAlignment(long sizeOfBytesInt) {
        if (sizeOfBytesInt % (long) CapiBlockDevice.BLOCK_SIZE != 0L)
            throw new IllegalArgumentException("size is not aligned.");
    }

    private void checkAddressAlignment(long address) {
        if (address % (long) CapiBlockDevice.BLOCK_SIZE != 0L)
            throw new IllegalArgumentException("address is not aligned.");
    }

    private void checkSize(long size) {
        if (size <= 0L)
            throw new IllegalArgumentException("size must be positive: size=" + size);
        if (size >= (long) Integer.MAX_VALUE)
            throw new IllegalArgumentException("size must be in Integer.MAX_VALUE.");
    }

    private void checkRange(long address, long sizeOfBytesInt) {
        int startDeviceIdx = (int) (address / ((long) lbaSizeForEach * (long) CapiBlockDevice.BLOCK_SIZE));
        int endDeviceIdx = (int) ((address + sizeOfBytesInt - 1) / ((long) lbaSizeForEach * (long) CapiBlockDevice.BLOCK_SIZE));

        if (startDeviceIdx != endDeviceIdx)
            throw new IllegalArgumentException("inter-driver allocation is not supportted in the current version.");
    }

    RealAddressRange getRealAddressRange(long address, int sizeOfBytesInt) {
        if (address > limit)
            throw new IllegalArgumentException("address is too large");
        if (address < 0)
            throw new IllegalArgumentException("address is negative");

        RealAddressRange real = new RealAddressRange();

        int idx = (int) (address / ((long) CapiBlockDevice.BLOCK_SIZE * (long) lbaSizeForEach));
        real.driver = drivers.get(idx);
        long startLBA = driver2StartLBA.get(idx);

        long addressInDriver = address % ((long) CapiBlockDevice.BLOCK_SIZE * (long) lbaSizeForEach);

        real.startLBA = (int) (addressInDriver / (long) CapiBlockDevice.BLOCK_SIZE) + startLBA;

        real.endLBA = (int) ((addressInDriver + (long) sizeOfBytesInt) / (long) CapiBlockDevice.BLOCK_SIZE) + startLBA;

        return real;
    }

    public ByteBuffer read(long address, long sizeOfBytes) throws IOException {
        checkActive();
        checkSize(sizeOfBytes);
        checkAddressAlignment(address);
        checkAddressAlignment(address + sizeOfBytes);
        checkRange(address, sizeOfBytes);

        if (sizeOfBytes > Integer.MAX_VALUE)
            throw new IllegalArgumentException("each block must be less than " + Integer.MAX_VALUE);

        int sizeOfBytesInt = (int) sizeOfBytes;

        RealAddressRange range = getRealAddressRange(address, sizeOfBytesInt);
        return range.read();
    }

    public void write(long address, ByteBuffer bb) throws IOException {
        checkActive();
        checkAddressAlignment(address);
        checkAddressAlignment(address + bb.capacity());
        checkRange(address, bb.capacity());

        RealAddressRange range = getRealAddressRange(address, bb.capacity());
        // System.out.println(range);
        range.write(bb);
    }

    public void readAsync(long address, long sizeOfBytes, AsyncHandler handler) throws IOException {
        checkActive();
        checkSize(sizeOfBytes);
        checkAddressAlignment(address);
        checkAddressAlignment(address + sizeOfBytes);
        checkRange(address, sizeOfBytes);

        if (sizeOfBytes > Integer.MAX_VALUE)
            throw new IllegalArgumentException("each block must be less than " + Integer.MAX_VALUE);

        int sizeOfBytesInt = (int) sizeOfBytes;

        RealAddressRange range = getRealAddressRange(address, sizeOfBytesInt);
        range.readAsync(handler);
    }

    public void writeAsync(long address, ByteBuffer bb, AsyncHandler handler) throws IOException {
        checkActive();
        checkAddressAlignment(address);
        checkAddressAlignment(address + bb.capacity());
        checkRange(address, bb.capacity());
        //System.out.println("write: " + address + "-" + (address + bb.capacity()));

        RealAddressRange range = getRealAddressRange(address, bb.capacity());
        range.writeAsync(bb, handler);

    }

}
