package org.apache.cassandra.cache.capi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.cache.capi.PersistenceDriver.AsyncHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

public class CapiCache<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(CapiCache.class);

    static final byte TYPE_NULL = 0;
    static final byte TYPE_KV = 1;
    static final byte TYPE_KLIST = 2;

    static final int CELL_KV_OVERHEAD = 21; // 1. type, 2. hash code, 3. key length, 4. key body, 5. value length, 6. value body
    static final int CELL_KLIST_OVERHEAD = 17; // 1. type, 2. list size, 3. external list, 4. external list size
    static final int CELL_EXTKLIST_OVERHEAD = 4; // 1. list size
    static final int KV_OVERHEAD = 16; // 1. key length, 2. key body, 3. value length, 4. value body

    public interface Serializer<K, V> {
        int hashCode(K k);

        boolean equals(K key, ByteBuffer bb);

        void serializeKey(K key, ByteBuffer bb);

        void serializeValue(V value, ByteBuffer bb);

        K deserializeKey(ByteBuffer bb);

        V deserializeValue(ByteBuffer bb);

        int keySize(K k);

        int valueSize(V v);
    }

    public interface CacheHandler<K, V> {
        public void hit(K k, V v);

        public void miss(K k);

        public void error(K k, String msg);

        public void updated(K k);
    }

    private class DeviceConfig {
        String devicePath;
        List<String> mirrors = new ArrayList<>();
        long startLBA;
        long lbaSize;

        PersistenceDriver createPersistenceDriver(int numOfAsync) throws IOException {
            PersistenceDriver ret;
            if (devicePath.startsWith("/dev")) {
                String[] devicePaths = new String[mirrors.size() + 1];
                devicePaths[0] = devicePath;
                for (int i = 0; i < mirrors.size(); ++i)
                    devicePaths[i + 1] = mirrors.get(i);
                ret = new CapiChunkDriver(devicePaths, numOfAsync);
            } else if (System.getProperties().getProperty("capicache.file.asynch", "false").equals("true"))
                ret = new AsyncFileChannelDriver(devicePath, numOfAsync, (int) (startLBA + lbaSize));
            else
                ret = new FileChannelDriver(devicePath, numOfAsync, (int) (startLBA + lbaSize));

            return ret;
        }
    }

    private static class KeyListEntry {
        int hashCode;
        long kvAddr;
        int kvSize;

        public String toString() {
            return "[" + kvAddr + "," + kvSize + "," + hashCode + "]";
        }
    }

    static interface InternalCacheHandler<K, V> {
        public void hitInCell(ByteBuffer cellBB, int keylen, K k, ByteBuffer valueBBtmp);

        public void hitInKeyList(ByteBuffer cellBB, long kvAddr, ByteBuffer kvBB, K k, int keylen, ByteBuffer valueBB);

        public void hitInExtKeyList(ByteBuffer cellBB, ByteBuffer extListBB, long kvAddr, ByteBuffer kvBB, K k, int keylen, ByteBuffer valueBB);

        public void miss(ByteBuffer cellBB, int keylen, K k);

        public void error(K k, String msg);
    }

    private static long CELL_ADDRESS = 0L;

    final StorageManager sm;
    final int numOfCells;
    final int cellUnitByteSize;
    final int numOfAsync;
    final Serializer<K, V> serializer;
    final List<PersistenceDriver> drivers = new ArrayList<>();
    final List<DeviceConfig> deviceConfigs = new ArrayList<>();
    final CrossThreadLockManager<Long> lockMgr = new CrossThreadLockManager<>();
    final ConcurrentLinkedHashMap<Long, ByteBuffer> cellCaches;

    int numOfCellsForEach;

    boolean initialized = false;

    public CapiCache(long totalCellByteSize, int cellByteSize, int numOfAsync, Serializer<K, V> serializer) {
        this.sm = new StorageManager();
        this.cellUnitByteSize = PersistenceDriverImpl.getAlignedSize(cellByteSize);
        this.numOfAsync = numOfAsync;
        this.serializer = serializer;

        long numOfCellsLong = totalCellByteSize / (long) cellUnitByteSize;
        if (numOfCellsLong > (long) Integer.MAX_VALUE)
            numOfCells = Integer.MAX_VALUE;
        else
            numOfCells = (int) numOfCellsLong;

        this.cellCaches = new ConcurrentLinkedHashMap.Builder<Long, ByteBuffer>().maximumWeightedCapacity(Integer.parseInt(System.getProperty(this.getClass().getName() + ".cellcache", "1000"))).build();
    }

    public void addDevice(String deviceName, long startLBA, long sizeOfLBA, String... mirrorDevices) throws IOException {
        if (initialized)
            throw new IllegalStateException("already initialized");

        if (sizeOfLBA < 0L || startLBA < 0L)
            throw new IllegalArgumentException("lba must be positive");

        DeviceConfig config = new DeviceConfig();
        config.devicePath = deviceName;
        config.startLBA = startLBA;
        config.lbaSize = sizeOfLBA;
        for (String otherDevicePath : mirrorDevices)
            config.mirrors.add(otherDevicePath);

        deviceConfigs.add(config);
    }

    public String toString() {
        StringBuffer buff = new StringBuffer();
        buff.append("[capicache drivers=").append(drivers).append(")");
        return buff.toString();
    }

    public void initialize(boolean clean) throws IOException {
        if (initialized)
            throw new IllegalStateException("already initialized.");

        int numOfAsyncForEach = numOfAsync / deviceConfigs.size();
        if (numOfAsyncForEach == 0)
            numOfAsyncForEach = 1;

        for (DeviceConfig config : deviceConfigs) {
            PersistenceDriver driver = config.createPersistenceDriver(numOfAsyncForEach);
            drivers.add(driver);
            sm.add(driver, config.startLBA, config.lbaSize);
        }

        this.numOfCellsForEach = numOfCells / drivers.size();
        if (numOfCells % drivers.size() != 0)
            this.numOfCellsForEach += 1;

        for (PersistenceDriver driver : drivers)
            sm.reserve(driver, (long) this.numOfCellsForEach * (long) PersistenceDriver.BLOCK_SIZE);

        sm.initialize(clean);

        if (clean)
            initCells();
        else
            logger.info("restore: " + stat(1000));

        initialized = true;
    }

    private void initCells() throws IOException {
        //            final ConcurrentLinkedQueue<ByteBuffer> initBBs = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<String> errMsgs = new ConcurrentLinkedQueue<>();

        long startCellInit = System.currentTimeMillis();

        final AtomicLong initializedCell = new AtomicLong(0L);

        if (logger.isInfoEnabled())
            logger.info("capi cache is intializing: #cell=" + numOfCells);

        final LazySynchronizer syncher = new LazySynchronizer(100);
        //AtomicBoolean syncher = new AtomicBoolean(false);

        final ConcurrentLinkedQueue<ByteBuffer> initBBs = new ConcurrentLinkedQueue<>();
        for (int cellIdx = 0; cellIdx < numOfCells; ++cellIdx) {
            final AtomicBoolean checkpoint = syncher.checkpoint();

            final int cellIdxf = cellIdx;
            // System.out.println("clean: writeAsync: " + cellIdxf + "->" + getCellAddrByIndex(cellIdxf));

            ByteBuffer pooledBB = initBBs.poll();
            if (pooledBB == null) {
                pooledBB = sm.getByteBuffer(PersistenceDriver.BLOCK_SIZE);
                for (int i = 0; i < PersistenceDriver.BLOCK_SIZE; ++i)
                    pooledBB.put((byte) 0);
            }
            reset(pooledBB);

            final ByteBuffer initBB = pooledBB;

            sm.writeAsync(getCellAddrByIndex(cellIdxf), initBB, new AsyncHandler() {
                @Override
                public void success(ByteBuffer bb) {
                    try {
                        // System.out.println("clean: success: " + cellIdxf + "->" + getCellAddrByIndex(cellIdxf) + "("
                        // + PersistencDriverImpl.summary(bb) + ")");
                        long n = initializedCell.incrementAndGet();
                        if (n % 1000000L == 0L)
                            //if (n % 10000L == 0L)
                            logger.info("initialized capi map: " + n + "/" + numOfCells);

                    } finally {
                        syncher.processed(checkpoint);
                        initBBs.add(initBB);
                        //reset(initBB);
                    }
                }

                @Override
                public void error(String msg) {
                    try {
                        errMsgs.add(msg);
                    } finally {
                        syncher.processed(checkpoint);
                    }
                }
            });
        }

        flush();

        //for (ByteBuffer initBB : initBBs)
        //    allocator.releaseByteBuffer(initBB);

        for (int i = 1; i < numOfCells; i *= 2) {
            ByteBuffer cell = sm.read(getCellAddrByIndex(i), PersistenceDriver.BLOCK_SIZE);
            cell.rewind();
            if (cell.get() != (byte) 0)
                throw new IllegalStateException();
        }

        long elapsedCellInit = System.currentTimeMillis() - startCellInit;

        if (logger.isInfoEnabled())
            logger.info("capi cache is initialized: elapsed=" + elapsedCellInit + "ms");

        if (!errMsgs.isEmpty())
            throw new IOException("capi initialize error: " + errMsgs);
    }

    public void flush() {
        boolean again;
        do {
            again = false;
            for (PersistenceDriver driver : drivers) {
                try {
                    again |= driver.flush();
                } catch (IOException e) {
                    logger.error("flush error: driver=" + driver + ", msg=" + e.getMessage(), e);
                }
            }
        } while (again);
    }

    private long getCellAddress(int hashCode) {
        return getCellAddrByIndex(Math.abs(hashCode) % numOfCells);
    }

    private long getCellAddrByIndex(int cellIdx) {
        int driverIdx = cellIdx / numOfCellsForEach;
        int cellOffset = cellIdx % numOfCellsForEach;

        long lbaForEach = sm.getLimitForEachDriver();

        return (lbaForEach * (long) driverIdx + cellOffset) * (long) PersistenceDriver.BLOCK_SIZE;
    }

    private void readLock(int hashCode) {
        while (!lockMgr.readLock((Long) getCellAddress(hashCode))) {
            for (PersistenceDriver driver : drivers) {
                driver.process();
            }
        }
    }

    private boolean writeLockSoft(K k, int hashCode) {
        return lockMgr.writeLock(getCellAddress(hashCode));
    }

    private void writeLock(K k, int hashCode) {
        boolean conflicted = false;
        while (!lockMgr.writeLock(getCellAddress(hashCode))) {
            if (!conflicted) {
                //logger.info("lock conflict: " + (Long) getCellAddress(hashCode) + ":" + k + "<>" + lastLock.get(getCellAddress(hashCode)));
                conflicted = true;
            }
            for (PersistenceDriver driver : drivers) {
                driver.process();
            }
        }
    }

    private void releaseReadLock(K k, int hashCode) {
        lockMgr.releaseRead((Long) getCellAddress(hashCode));
    }

    private void releaseWriteLock(K k, int hashCode) {
        lockMgr.releaseWrite((Long) getCellAddress(hashCode));
    }

    private final Object nullObject = new Object();

    @SuppressWarnings("unchecked")
    public V getSync(K k) throws IOException {
        final AtomicReference<Object> ret = new AtomicReference<>();

        get(k, new CacheHandler<K, V>() {

            @Override
            public void updated(K k) {
                ret.set(new IOException("illegal state"));
                synchronized (ret) {
                    ret.notify();
                }
            }

            @Override
            public void miss(K k) {
                ret.set(nullObject);
                synchronized (ret) {
                    ret.notify();
                }
            }

            @Override
            public void hit(K k, V v) {
                if (v == null)
                    ret.set(nullObject);
                else
                    ret.set(v);
                synchronized (ret) {
                    ret.notify();
                }
            }

            @Override
            public void error(K k, String msg) {
                ret.set(new IOException(msg));
                synchronized (ret) {
                    ret.notify();
                }
            }
        });

        boolean first = true;
        while (ret.get() == null) {
            if (first)
                first = false;
            else
                for (PersistenceDriver driver : drivers)
                    driver.process();
            synchronized (ret) {
                try {
                    ret.wait();
                } catch (InterruptedException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }

        if (ret.get() instanceof IOException)
            throw (IOException) ret.get();

        if (ret.get() == nullObject)
            return null;

        return (V) ret.get();
    }

    public void get(K k, final CacheHandler<K, V> handler) {
        getWithLock(k, handler);
    }

    private void getWithLock(K k, final CacheHandler<K, V> handler) {
        final int hashCode = serializer.hashCode(k);
        readLock(hashCode);

        getWithCellCache(k, hashCode, new InternalCacheHandler<K, V>() {
            @Override
            public void miss(ByteBuffer cellBB, int keylen, K k) {
                releaseReadLock(k, hashCode);
                handler.miss(k);
                PersistenceDriverImpl.freePooled(cellBB);
            }

            @Override
            public void hitInCell(ByteBuffer cellBB, int keylen, K k, ByteBuffer valueBBtmp) {
                releaseReadLock(k, hashCode);
                if (valueBBtmp.capacity() == 0)
                    handler.hit(k, null);
                else
                    handler.hit(k, serializer.deserializeValue(valueBBtmp));
                PersistenceDriverImpl.freePooled(cellBB);
            }

            @Override
            public void hitInKeyList(ByteBuffer cellBB, long kvAddr, ByteBuffer kvBB, K k, int keylen, ByteBuffer valueBBtmp) {
                releaseReadLock(k, hashCode);
                if (valueBBtmp.capacity() == 0)
                    handler.hit(k, null);
                else
                    handler.hit(k, serializer.deserializeValue(valueBBtmp));
                PersistenceDriverImpl.freePooled(cellBB);
            }

            @Override
            public void error(K k, String msg) {
                releaseReadLock(k, hashCode);
                handler.error(k, msg);
            }

            @Override
            public void hitInExtKeyList(ByteBuffer cellBB, ByteBuffer extListBB, long kvAddr, ByteBuffer kvBB, K k, int keylen, ByteBuffer valueBBtmp) {
                releaseReadLock(k, hashCode);
                if (valueBBtmp.capacity() == 0)
                    handler.hit(k, null);
                else
                    handler.hit(k, serializer.deserializeValue(valueBBtmp));
                PersistenceDriverImpl.freePooled(cellBB);
                PersistenceDriverImpl.freePooled(extListBB);
            }
        });
    }

    private void getWithCellCache(final K k, final int hashCode, final InternalCacheHandler<K, V> handler) {
        final long cellAddr = getCellAddress(hashCode);

        try {
            ByteBuffer cachedCellBB = cellCaches.get(cellAddr);
            if (cachedCellBB != null) {
                ByteBuffer cachedCellBBCopy;
                synchronized (cachedCellBB) {
                    cachedCellBB.rewind();
                    cachedCellBBCopy = PersistenceDriverImpl.allocatePooled(cachedCellBB.capacity()).put(cachedCellBB);
                }
                get0(k, hashCode, handler, cachedCellBBCopy);
            } else {
                // System.out.println("get0: readAsync:" + cellAddr);
                sm.readAsync(cellAddr, cellUnitByteSize, new AsyncHandler() {
                    @Override
                    public void success(ByteBuffer cellBB) {
                        //cellCaches.put(cellAddr, PersistenceDriverImpl.allocatePooled(cellBB.capacity()).put(cellBB));
                        get0(k, hashCode, handler, cellBB);
                    }

                    @Override
                    public void error(String msg) {
                        handler.error(k, msg);
                    }
                });
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            handler.error(k, e.getMessage());
        }
    }

    private void get0(final K k, int hashCode, final InternalCacheHandler<K, V> handler, ByteBuffer cellBB) {
        int position = 0;
        int capacity = cellBB.capacity();

        reset(cellBB);
        byte type = cellBB.get(); // 1. type
        switch (type) {
        case TYPE_NULL: {
            reset(cellBB);
            handler.miss(cellBB, -1, k);
            break;
        }
        case TYPE_KV: {
            int cellHashCode = cellBB.getInt(); // 2. hash code
            int cellKeylen = cellBB.getInt(); // 3. key length
            position = cellBB.position();
            cellBB.limit(position + cellKeylen);
            ByteBuffer cellKeyBB = cellBB.slice(); // 4. key body
            cellBB.limit(capacity);
            cellBB.position(position + cellKeylen);
            int cellValuelen = cellBB.getInt(); // 5. value length
            position = cellBB.position();
            cellBB.limit(position + cellValuelen);
            ByteBuffer cellValueBB = cellBB.slice(); // 6. value body
            cellBB.limit(capacity);

            reset(cellBB);
            if (cellHashCode == hashCode && serializer.equals(k, cellKeyBB)) {
                //System.out.println("read " + dumpCellBB(cellBB));
                handler.hitInCell(cellBB, cellKeylen, k, cellValueBB);
            } else {
                handler.miss(cellBB, -1, k);
            }
            break;
        }
        case TYPE_KLIST: {
            int keySize = cellBB.getInt(); // 2. size
            //long extKeyListAddress = 
            cellBB.getLong(); // 3. external list address
            //int extKeyListBytes = 
            cellBB.getInt(); // 4. external list size

            //StringBuffer buff = new StringBuffer();
            List<KeyListEntry> keylist = new ArrayList<>();
            for (int i = 0; i < keySize; ++i) {
                KeyListEntry entry = new KeyListEntry();
                entry.hashCode = cellBB.getInt(); // 5. hashcode
                entry.kvAddr = cellBB.getLong(); // 6. kv address
                entry.kvSize = cellBB.getInt(); // 7. kv size 
                keylist.add(entry);
                //buff.append(entry);
            }
            //System.out.println("get0: " + extKeyListAddress + ":" + buff.toString());
            searchKeyList(cellBB, k, hashCode, keylist, 0, null, handler);
            break;
        }
        default: {
            logger.error("unknown cell type: " + type);
            handler.error(k, "unknown cell type: " + type);
        }
        }
    }

    private void searchKeyList(final ByteBuffer cellBB, final K k, final int hashCode, final List<KeyListEntry> keylist, int from, final ByteBuffer extKeyListBB, final InternalCacheHandler<K, V> handler) {
        try {
            boolean hit = false;
            for (int i = from; i < keylist.size(); ++i) {
                final KeyListEntry entry = keylist.get(i);

                if (hashCode != entry.hashCode)
                    continue;

                hit = true;
                final int nextFrom = i + 1;

                int sizeOfBytes = PersistenceDriverImpl.getAlignedSize(entry.kvSize);
                sm.readAsync(entry.kvAddr, sizeOfBytes, new AsyncHandler() {
                    @Override
                    public void success(ByteBuffer kvBB) {
                        int position;
                        int capacity = kvBB.capacity();

                        int keylen = kvBB.getInt(); // 1. key length

                        position = kvBB.position();
                        kvBB.limit(position + keylen); // 2. key body
                        ByteBuffer kvKeyBB = kvBB.slice();
                        kvBB.limit(capacity);
                        kvBB.position(position + keylen);

                        int valuelen = kvBB.getInt(); // 3. value length

                        position = kvBB.position();
                        kvBB.limit(position + valuelen);
                        ByteBuffer kvValueBB = kvBB.slice(); // 4. value body
                        kvBB.limit(capacity);
                        kvBB.position(position + valuelen);

                        if (serializer.equals(k, kvKeyBB)) {
                            //System.out.println("read key=" + k + "@" + kvAddr);
                            reset(kvBB);
                            if (extKeyListBB == null)
                                handler.hitInKeyList(cellBB, entry.kvAddr, kvBB, k, keylen, kvValueBB);
                            else
                                handler.hitInExtKeyList(cellBB, extKeyListBB, entry.kvAddr, kvBB, k, keylen, kvValueBB);
                        } else {
                            searchKeyList(cellBB, k, hashCode, keylist, nextFrom, extKeyListBB, handler);
                        }
                    }

                    @Override
                    public void error(String msg) {
                        handler.error(k, msg);
                    }
                });
                break;
            }

            if (!hit) {
                long extKeyListAddr = getExtKeyListAddress(cellBB);
                if (extKeyListAddr == CELL_ADDRESS || extKeyListBB != null) {
                    reset(cellBB);
                    handler.miss(cellBB, -1, k);
                } else {
                    //System.out.println("ext key list addr: " + extKeyListAddr);
                    int extKeyListBytes = getExtKeyListBytes(cellBB);
                    sm.readAsync(extKeyListAddr, extKeyListBytes, new AsyncHandler() {

                        @Override
                        public void success(ByteBuffer extKeyListBB) {
                            reset(extKeyListBB);

                            int keySize = extKeyListBB.getInt(); // 1. size
                            List<KeyListEntry> keylist = new ArrayList<>();

                            //System.out.println("search ext key list: " + extKeyListAddr + ": " + keySize);
                            for (int i = 0; i < keySize; ++i) {
                                KeyListEntry entry = new KeyListEntry();
                                entry.hashCode = extKeyListBB.getInt(); // 2. hashcode
                                entry.kvAddr = extKeyListBB.getLong(); // 3. kv address
                                entry.kvSize = extKeyListBB.getInt(); // 4. kv size 
                                keylist.add(entry);
                                //System.out.println("  " + entry.hashCode + ":" + entry.kvAddr + ":" + entry.kvSize);
                            }

                            searchKeyList(cellBB, k, hashCode, keylist, 0, extKeyListBB, handler);
                        }

                        @Override
                        public void error(String msg) {
                            logger.error(msg);
                            handler.error(k, msg);
                        }
                    });

                }
            }

        } catch (

        IOException e) {
            logger.error(e.getMessage(), e);
            handler.error(k, e.getMessage());
        }
    }

    private Object putSuccess = new Object();

    public void putSync(K k, final V v) throws IOException {
        final AtomicReference<Object> ret = new AtomicReference<>();

        put(k, v, new CacheHandler<K, V>() {

            @Override
            public void updated(K k) {
                ret.set(putSuccess);
                synchronized (ret) {
                    ret.notify();
                }
            }

            @Override
            public void miss(K k) {
                ret.set(new IOException("illegal state"));
                synchronized (ret) {
                    ret.notify();
                }
            }

            @Override
            public void hit(K k, V v) {
                ret.set(new IOException("illegal state"));
                synchronized (ret) {
                    ret.notify();
                }
            }

            @Override
            public void error(K k, String msg) {
                ret.set(new IOException(msg));
                synchronized (ret) {
                    ret.notify();
                }
            }
        });

        synchronized (ret) {
            while (ret.get() == null)
                try {
                    ret.wait();
                } catch (InterruptedException ex) {
                    throw new IllegalStateException(ex);
                }

            if (ret.get() instanceof IOException)
                throw (IOException) ret.get();
        }
    }

    public void remove(K k) {
        removeWithLock(k, new CacheHandler<K, V>() {

            @Override
            public void updated(K k) {
            }

            @Override
            public void miss(K k) {
            }

            @Override
            public void hit(K k, V v) {
            }

            @Override
            public void error(K k, String msg) {
            }

        }, false);
    }

    public void removeSync(K k) throws IOException {
        final AtomicReference<Object> ret = new AtomicReference<>();

        remove(k, new CacheHandler<K, V>() {

            @Override
            public void updated(K k) {
                ret.set(putSuccess);
                synchronized (ret) {
                    ret.notify();
                }
            }

            @Override
            public void miss(K k) {
                ret.set(new IOException("illegal state"));
                synchronized (ret) {
                    ret.notify();
                }
            }

            @Override
            public void hit(K k, V v) {
                ret.set(new IOException("illegal state"));
                synchronized (ret) {
                    ret.notify();
                }
            }

            @Override
            public void error(K k, String msg) {
                ret.set(new IOException(msg));
                synchronized (ret) {
                    ret.notify();
                }
            }
        });

        synchronized (ret) {
            while (ret.get() == null)
                try {
                    ret.wait();
                } catch (InterruptedException ex) {
                    throw new IllegalStateException(ex);
                }

            if (ret.get() instanceof IOException)
                throw (IOException) ret.get();
        }
    }

    public void remove(K k, final CacheHandler<K, V> handler) {
        removeWithLock(k, handler, false);
    }

    private void removeWithLock(K k, final CacheHandler<K, V> handler, boolean soft) {
        final int hashCode = serializer.hashCode(k);
        if (soft) {
            if (!writeLockSoft(k, hashCode)) {
                handler.error(k, "lock conflict:" + k);
                return;
            }
        } else {
            writeLock(k, hashCode);
        }

        remove0(k, hashCode, new CacheHandler<K, V>() {

            @Override
            public void updated(K k) {
                releaseWriteLock(k, hashCode);
                handler.updated(k);
            }

            @Override
            public void miss(K k) {
                releaseWriteLock(k, hashCode);
                handler.miss(k);
            }

            @Override
            public void hit(K k, V v) {
                throw new IllegalStateException();
            }

            @Override
            public void error(K k, String msg) {
                handler.error(k, msg);
                releaseWriteLock(k, hashCode);
            }
        });
    }

    private void remove0(K k, final int hashCode, final CacheHandler<K, V> handler) {

        getWithCellCache(k, hashCode, new InternalCacheHandler<K, V>() {
            @Override
            public void miss(ByteBuffer cellBB, int keylen, K k) {
                handler.miss(k);
            }

            @Override
            public void hitInCell(ByteBuffer cellBB, int keylen, final K k, ByteBuffer valueBBtmp) {
                if (keylen == -1)
                    keylen = serializer.keySize(k);

                reset(cellBB);
                cellBB.put(TYPE_NULL); // 1. type
                reset(cellBB);

                long cellAddr = getCellAddress(hashCode);
                cellCaches.remove(cellAddr);

                try {
                    //System.out.println("write key=" + k + "@" + cellAddr + "-" + (cellAddr + cellBB.capacity()) + ":" + dumpCellBB(cellBB));
                    sm.writeAsync(cellAddr, cellBB, new AsyncHandler() {
                        @Override
                        public void success(ByteBuffer bb) {
                            //cellCaches.put(cellAddr, PersistenceDriverImpl.allocatePooled(cellBB.capacity()).put(cellBB));
                            handler.updated(k);
                        }

                        @Override
                        public void error(String msg) {
                            handler.error(k, msg);
                        }
                    });
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    handler.error(k, e.getMessage());
                }
            }

            @Override
            public void hitInKeyList(ByteBuffer cellBB, final long kvAddr, final ByteBuffer kvBB, K k, int keylen, ByteBuffer valueBB) {

                long cellAddr = getCellAddress(hashCode);
                cellCaches.remove(cellAddr);

                reset(cellBB);
                cellBB.get(); // 1. type
                int keylistSize = cellBB.getInt(); // 2. list size
                List<KeyListEntry> storedList = new ArrayList<>(keylistSize - 1);
                cellBB.getLong(); // 3. external list address
                cellBB.getInt(); // 4. external list size

                for (int i = 0; i < keylistSize; ++i) {
                    KeyListEntry entry = new KeyListEntry();
                    entry.hashCode = cellBB.getInt(); // 5. hash code
                    entry.kvAddr = cellBB.getLong();// 6. kv address
                    entry.kvSize = cellBB.getInt(); // 7. kv length
                    if (entry.kvAddr != kvAddr)
                        storedList.add(entry);
                }

                updateKeyList(cellBB, k, hashCode, storedList, new CacheHandler<K, V>() {

                    @Override
                    public void updated(K k) {
                        sm.free(kvAddr, kvBB.capacity());
                        handler.updated(k);
                    }

                    @Override
                    public void miss(K k) {
                        throw new IllegalStateException();
                    }

                    @Override
                    public void hit(K k, V v) {
                        throw new IllegalStateException();
                    }

                    @Override
                    public void error(K k, String msg) {
                        handler.error(k, msg);
                    }
                });
            }

            @Override
            public void hitInExtKeyList(ByteBuffer cellBB, ByteBuffer extListBB, final long kvAddr, final ByteBuffer kvBB, K k, int keylen, ByteBuffer valueBBtmp) {

                long cellAddr = getCellAddress(hashCode);
                cellCaches.remove(cellAddr);

                reset(extListBB);
                int keylistSize = extListBB.getInt(); // 1. list size
                List<KeyListEntry> storedList = new ArrayList<>(keylistSize + 1);

                for (int i = 0; i < keylistSize; ++i) {
                    KeyListEntry entry = new KeyListEntry();
                    entry.hashCode = extListBB.getInt(); // 2. hash code
                    entry.kvAddr = extListBB.getLong();// 3. kv address
                    entry.kvSize = extListBB.getInt(); // 4. kv length
                    if (entry.kvAddr != kvAddr)
                        storedList.add(entry);
                }

                updateExtKeyList(cellBB, extListBB, k, hashCode, storedList, new CacheHandler<K, V>() {
                    @Override
                    public void updated(K k) {
                        sm.free(kvAddr, kvBB.capacity());
                        handler.updated(k);
                    }

                    @Override
                    public void miss(K k) {
                        throw new IllegalStateException();
                    }

                    @Override
                    public void hit(K k, V v) {
                        throw new IllegalStateException();
                    }

                    @Override
                    public void error(K k, String msg) {
                        handler.error(k, msg);
                    }
                });
            }

            @Override
            public void error(K k, String msg) {
                handler.error(k, msg);
            }
        });
    }

    public void put(K k, final V v) {
        put(k, v, new CacheHandler<K, V>() {

            @Override
            public void updated(K k) {
            }

            @Override
            public void miss(K k) {
            }

            @Override
            public void hit(K k, V v) {
            }

            @Override
            public void error(K k, String msg) {
            }
        });
    }

    public void putSoft(K k, final V v, final CacheHandler<K, V> handler) {
        putWithLock(k, v, handler, true);
    }

    public void put(K k, final V v, final CacheHandler<K, V> handler) {
        putWithLock(k, v, handler, false);
    }

    private void putWithLock(K k, final V v, final CacheHandler<K, V> handler, boolean soft) {
        final int hashCode = serializer.hashCode(k);
        if (soft) {
            if (!writeLockSoft(k, hashCode)) {
                handler.error(k, "lock conflict:" + k);
                return;
            }
        } else {
            writeLock(k, hashCode);
        }

        put0(k, hashCode, v, new CacheHandler<K, V>() {

            @Override
            public void updated(K k) {
                releaseWriteLock(k, hashCode);
                handler.updated(k);
            }

            @Override
            public void miss(K k) {
                throw new IllegalStateException();
            }

            @Override
            public void hit(K k, V v) {
                throw new IllegalStateException();
            }

            @Override
            public void error(K k, String msg) {
                handler.error(k, msg);
                releaseWriteLock(k, hashCode);
            }
        });

    }

    public void put0(K k, final int hashCode, final V v, final CacheHandler<K, V> handler) {
        getWithCellCache(k, hashCode, new InternalCacheHandler<K, V>() {
            @Override
            public void miss(ByteBuffer cellBB, int keylen, K k) {
                reset(cellBB);
                byte type = cellBB.get(); // 1. type

                switch (type) {
                case TYPE_NULL:
                    updateKeyValueCell(cellBB, keylen, k, hashCode, v, handler);
                    break;
                case TYPE_KV:
                    int position;
                    int capacity = cellBB.capacity();

                    int copyHashCode = cellBB.getInt(); // 2. hash code
                    int copyKeylen = cellBB.getInt(); // 3. key length
                    position = cellBB.position();
                    cellBB.limit(position + copyKeylen);
                    ByteBuffer copyKeyBB = cellBB.slice(); // 4. key body
                    cellBB.limit(capacity);
                    cellBB.position(position + copyKeylen);
                    int copyValuelen = cellBB.getInt(); // 5. value length
                    position = cellBB.position();
                    cellBB.limit(position + copyValuelen);
                    ByteBuffer copyValueBB = cellBB.slice(); // 6. value body
                    cellBB.limit(capacity);

                    reset(cellBB);

                    copyAndAddKeyList(cellBB, copyKeyBB, copyHashCode, copyValueBB, keylen, k, hashCode, -1, v, handler);

                    break;
                case TYPE_KLIST:
                    int keylistSize = cellBB.getInt(); // 2. list size
                    List<KeyListEntry> storedList = new ArrayList<>(keylistSize + 1);
                    //long extKeyListAddr = 
                    cellBB.getLong(); // 3. external list address
                    //int extKeyListBytes = 
                    cellBB.getInt(); // 4. external list size

                    for (int i = 0; i < keylistSize; ++i) {
                        KeyListEntry entry = new KeyListEntry();
                        entry.hashCode = cellBB.getInt(); // 5. hash code
                        entry.kvAddr = cellBB.getLong();// 6. kv address
                        entry.kvSize = cellBB.getInt(); // 7. kv length
                        storedList.add(entry);
                    }

                    if ((storedList.size() + 1) * 16 + CELL_KLIST_OVERHEAD < cellUnitByteSize)
                        addToKeyList(cellBB, keylen, k, hashCode, -1, v, storedList, handler);
                    else if (getExtKeyListAddress(cellBB) == CELL_ADDRESS)
                        createExtKeyList(cellBB, keylen, k, hashCode, -1, v, handler);
                    else
                        addToExtKeyList(cellBB, keylen, k, hashCode, v, handler);
                    break;
                }

            }

            @Override
            public void hitInCell(ByteBuffer cellBB, int keylen, K k, ByteBuffer valueBBtmp) {
                updateKeyValueCell(cellBB, keylen, k, hashCode, v, handler);
            }

            @Override
            public void hitInKeyList(ByteBuffer cellBB, long kvAddr, ByteBuffer kvBB, K k, int keylen, ByteBuffer valueBB) {
                reset(cellBB);
                cellBB.get(); // 1. type
                int keylistSize = cellBB.getInt(); // 2. list size
                List<KeyListEntry> storedList = new ArrayList<>(keylistSize + 1);
                cellBB.getLong(); // 3. external list address
                cellBB.getInt(); // 4. external list size

                for (int i = 0; i < keylistSize; ++i) {
                    KeyListEntry entry = new KeyListEntry();
                    entry.hashCode = cellBB.getInt(); // 5. hash code
                    entry.kvAddr = cellBB.getLong();// 6. kv address
                    entry.kvSize = cellBB.getInt(); // 7. kv length
                    if (entry.kvAddr != kvAddr)
                        storedList.add(entry);
                }

                addToKeyListAndFree(cellBB, kvAddr, kvBB.capacity(), keylen, k, hashCode, -1, v, storedList, handler);
            }

            @Override
            public void hitInExtKeyList(ByteBuffer cellBB, ByteBuffer extListBB, long kvAddr, ByteBuffer kvBB, K k, int keylen, ByteBuffer valueBBtmp) {
                reset(extListBB);
                int keylistSize = extListBB.getInt(); // 1. list size
                List<KeyListEntry> storedList = new ArrayList<>(keylistSize + 1);

                for (int i = 0; i < keylistSize; ++i) {
                    KeyListEntry entry = new KeyListEntry();
                    entry.hashCode = extListBB.getInt(); // 2. hash code
                    entry.kvAddr = extListBB.getLong();// 3. kv address
                    entry.kvSize = extListBB.getInt(); // 4. kv length
                    if (entry.kvAddr != kvAddr)
                        storedList.add(entry);
                }

                addToExtKeyListAndFree(cellBB, extListBB, kvAddr, kvBB.capacity(), keylen, k, hashCode, -1, v, storedList, handler);
            }

            @Override
            public void error(K k, String msg) {
                handler.error(k, msg);
            }
        });
    }

    interface InternalHandler {
        void handled();

        void error(String msg);
    }

    final InternalHandler dummyHandler = new InternalHandler() {

        @Override
        public void handled() {
        }

        @Override
        public void error(String msg) {
            logger.error("error: " + msg);
            throw new IllegalStateException(msg);
        }
    };

    private void reset(ByteBuffer bb) {
        bb.rewind();
        bb.limit(bb.capacity());
    }

    private void updateKeyValueCell(ByteBuffer cellBB, int keylen, K k, int hashCode, V v, final CacheHandler<K, V> handler) {
        if (keylen == -1)
            keylen = serializer.keySize(k);

        int valuelen;
        if (v == null)
            valuelen = 0;
        else
            valuelen = serializer.valueSize(v);

        if (keylen + valuelen + CELL_KV_OVERHEAD < cellUnitByteSize)
            replaceCellWithKeyValue(handler, cellBB, keylen, k, hashCode, valuelen, v);
        else
            addToKeyList(cellBB, keylen, k, hashCode, valuelen, v, new ArrayList<KeyListEntry>(), handler);
    }

    private void replaceCellWithKeyValue(final CacheHandler<K, V> handler, final ByteBuffer cellBB, int keylen, final K k, int hashCode, int valuelen, V v) {
        int position;
        reset(cellBB);
        cellBB.put(TYPE_KV); // 1. type

        cellBB.putInt(hashCode); // 2. hash code
        cellBB.putInt(keylen); // 3. key length

        position = cellBB.position();
        cellBB.limit(position + keylen);
        serializer.serializeKey(k, cellBB.slice()); // 4. key body

        cellBB.limit(cellUnitByteSize);
        cellBB.position(position + keylen);

        cellBB.putInt(valuelen);// 5. value length

        if (valuelen != 0) {
            position = cellBB.position();
            cellBB.limit(position + valuelen);
            serializer.serializeValue(v, cellBB.slice()); // 6. value body

            cellBB.limit(cellUnitByteSize);
            cellBB.position(position + valuelen);
        }

        reset(cellBB);

        long cellAddr = getCellAddress(hashCode);
        cellCaches.remove(cellAddr);

        try {
            //System.out.println("write key=" + k + "@" + cellAddr + "-" + (cellAddr + cellBB.capacity()) + ":" + dumpCellBB(cellBB));
            sm.writeAsync(cellAddr, cellBB, new AsyncHandler() {
                @Override
                public void success(ByteBuffer bb) {
                    //cellCaches.put(cellAddr, PersistenceDriverImpl.allocatePooled(cellBB.capacity()).put(cellBB));
                    handler.updated(k);
                }

                @Override
                public void error(String msg) {
                    handler.error(k, msg);
                }
            });
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            handler.error(k, e.getMessage());
        }
    }

    private void copyAndAddKeyList(final ByteBuffer cellBB, ByteBuffer copyKeyBB, final int copyHashCode, ByteBuffer copyValueBB, final int newKeylen, final K newK, final int newHashCode, final int newValuelen, final V newV, final CacheHandler<K, V> handler) {

        final ByteBuffer copyKvBB = createKeyValueBlock(copyKeyBB, copyHashCode, copyValueBB);

        final long copyKvAddr;
        final long alignedSize;
        try {
            alignedSize = PersistenceDriverImpl.getAlignedSize(copyKvBB.capacity());
            copyKvAddr = sm.allocate(alignedSize);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            handler.error(newK, e.getMessage());
            return;
        }

        reset(cellBB);
        cellBB.put(TYPE_KLIST); // 1. type
        cellBB.putInt(2); // 2. list size
        cellBB.putLong(CELL_ADDRESS); // 3. external list
        cellBB.putInt(0); // 4. external list size

        try {
            sm.writeAsync(copyKvAddr, copyKvBB, new AsyncHandler() {
                @Override
                public void success(ByteBuffer bb) {
                    reset(cellBB);

                    List<KeyListEntry> storedList = new ArrayList<>();
                    KeyListEntry entry = new KeyListEntry();
                    entry.hashCode = copyHashCode;
                    entry.kvAddr = copyKvAddr;
                    entry.kvSize = copyKvBB.capacity();
                    storedList.add(entry);

                    addToKeyList(cellBB, newKeylen, newK, newHashCode, newValuelen, newV, storedList, handler);
                }

                @Override
                public void error(String msg) {
                    sm.free(copyKvAddr, alignedSize);
                    handler.error(newK, msg);
                }
            });
        } catch (IOException e) {
            sm.free(copyKvAddr, alignedSize);
            logger.error(e.getMessage(), e);
            handler.error(newK, e.getMessage());
        }
    }

    private void addToKeyListAndFree(final ByteBuffer cellBB, final long freeKvAddr, final int freeKvSize, int keylen, final K k, int hashCode, int valuelen, V v, final List<KeyListEntry> storedList, final CacheHandler<K, V> handler) {

        addToKeyList(cellBB, keylen, k, hashCode, valuelen, v, storedList, new CacheHandler<K, V>() {

            @Override
            public void updated(K k) {
                sm.free(freeKvAddr, freeKvSize);
                handler.updated(k);
            }

            @Override
            public void miss(K k) {
                throw new IllegalStateException();
            }

            @Override
            public void hit(K k, V v) {
                throw new IllegalStateException();
            }

            @Override
            public void error(K k, String msg) {
                handler.error(k, msg);
            }
        });

    }

    private void addToExtKeyListAndFree(final ByteBuffer cellBB, final ByteBuffer extKeyListBB, final long freeKvAddr, final int freeKvSize, int keylen, final K k, int hashCode, int valuelen, V v, final List<KeyListEntry> storedList, final CacheHandler<K, V> handler) {

        addToExtKeyList(cellBB, extKeyListBB, keylen, k, hashCode, valuelen, v, storedList, new CacheHandler<K, V>() {

            @Override
            public void updated(K k) {
                sm.free(freeKvAddr, freeKvSize);
                handler.updated(k);
            }

            @Override
            public void miss(K k) {
                throw new IllegalStateException();
            }

            @Override
            public void hit(K k, V v) {
                throw new IllegalStateException();
            }

            @Override
            public void error(K k, String msg) {
                handler.error(k, msg);
            }
        });

    }

    private void addToKeyList(final ByteBuffer cellBB, int keylen, final K k, final int hashCode, int valuelen, V v, final List<KeyListEntry> storedList, final CacheHandler<K, V> handler) {

        if (keylen == -1)
            keylen = serializer.keySize(k);

        if (valuelen == -1)
            if (v == null)
                valuelen = 0;
            else
                valuelen = serializer.valueSize(v);

        ByteBuffer kvBB = createKeyValueBlock(keylen, k, hashCode, valuelen, v);

        final KeyListEntry newEntry = new KeyListEntry();
        newEntry.hashCode = hashCode;
        newEntry.kvSize = kvBB.capacity();

        try {
            newEntry.kvAddr = sm.allocate(kvBB.capacity());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            handler.error(k, e.getMessage());
            return;
        }

        try {
            sm.writeAsync(newEntry.kvAddr, kvBB, new AsyncHandler() {

                @Override
                public void success(ByteBuffer bb) {
                    reset(cellBB);
                    storedList.add(newEntry);
                    updateKeyList(cellBB, k, hashCode, storedList, handler);
                }

                @Override
                public void error(String msg) {
                    logger.error(msg);
                    handler.error(k, msg);
                }
            });
        } catch (IOException e) {
            sm.free(newEntry.kvAddr, kvBB.capacity());
            logger.error(e.getMessage(), e);
            handler.error(k, e.getMessage());
        }
    }

    private void createExtKeyList(final ByteBuffer cellBB, int keylen, final K k, final int hashCode, int valuelen, V v, final CacheHandler<K, V> handler) {

        List<KeyListEntry> storedList = new ArrayList<>();
        long extKeyListAddr;
        try {
            extKeyListAddr = sm.allocate(PersistenceDriverImpl.BLOCK_SIZE);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            handler.error(k, e.getMessage());
            return;
        }

        ByteBuffer extKeyListBB = sm.getByteBuffer(PersistenceDriverImpl.BLOCK_SIZE);

        modifyExtKeyList(cellBB, extKeyListAddr, PersistenceDriverImpl.BLOCK_SIZE);

        addToExtKeyList(cellBB, extKeyListBB, keylen, k, hashCode, valuelen, v, storedList, new CacheHandler<K, V>() {

            @Override
            public void updated(final K k) {
                long cellAddr = getCellAddress(hashCode);
                cellCaches.remove(cellAddr);

                try {
                    sm.writeAsync(cellAddr, cellBB, new AsyncHandler() {
                        @Override
                        public void success(ByteBuffer bb) {
                            //cellCaches.put(cellAddr, PersistenceDriverImpl.allocatePooled(cellBB.capacity()).put(cellBB));
                            handler.updated(k);
                        }

                        @Override
                        public void error(String msg) {
                            logger.error(msg);
                            handler.error(k, msg);
                        }
                    });
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    handler.error(k, e.getMessage());
                }

            }

            @Override
            public void miss(K k) {
                throw new IllegalStateException();
            }

            @Override
            public void hit(K k, V v) {
                throw new IllegalStateException();
            }

            @Override
            public void error(K k, String msg) {
                logger.error(msg);
                handler.error(k, msg);
            }
        });
    }

    private void updateKeyList(final ByteBuffer cellBB, final K k, int hashCode, final List<KeyListEntry> storedList, final CacheHandler<K, V> handler) {
        reset(cellBB);
        cellBB.put(TYPE_KLIST); // 1. type
        cellBB.putInt(storedList.size()); // 2. list size
        //long extKeyListAddr = 
        cellBB.getLong(); // 3. external list
        //int extKeyListBytes = 
        cellBB.getInt(); // 4. external list size

        for (KeyListEntry keyListEntry : storedList) {
            cellBB.putInt(keyListEntry.hashCode); // 5. hash code
            cellBB.putLong(keyListEntry.kvAddr); // 6. kv address
            cellBB.putInt(keyListEntry.kvSize); // 7. kv length
        }
        reset(cellBB);

        long cellAddr = getCellAddress(hashCode);
        cellCaches.remove(cellAddr);
        try {
            //System.out.println("write key=" + k + "@" + cellAddr + "-" + (cellAddr + cellBB.capacity()) + ":" + dumpCellBB(cellBB));
            sm.writeAsync(cellAddr, cellBB, new AsyncHandler() {
                @Override
                public void success(ByteBuffer bb) {
                    //cellCaches.put(cellAddr, PersistenceDriverImpl.allocatePooled(cellBB.capacity()).put(cellBB));
                    handler.updated(k);
                }

                @Override
                public void error(String msg) {
                    handler.error(k, msg);
                }
            });
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            handler.error(k, e.getMessage());
        }
    }

    private void addToExtKeyList(final ByteBuffer cellBB, final int keylen, final K k, final int hashCode, final V v, final CacheHandler<K, V> handler) {
        long extKeyListAddr = getExtKeyListAddress(cellBB);
        int extKeyListBytes = getExtKeyListBytes(cellBB);

        try {
            sm.readAsync(extKeyListAddr, extKeyListBytes, new AsyncHandler() {
                @Override
                public void success(ByteBuffer extKeyListBB) {
                    int keylistSize = extKeyListBB.getInt(); // 1. list size
                    List<KeyListEntry> storedList = new ArrayList<>(keylistSize + 1);

                    for (int i = 0; i < keylistSize; ++i) {
                        KeyListEntry entry = new KeyListEntry();
                        entry.hashCode = extKeyListBB.getInt(); // 2. hash code
                        entry.kvAddr = extKeyListBB.getLong();// 3. kv address
                        entry.kvSize = extKeyListBB.getInt(); // 4. kv length
                        storedList.add(entry);
                    }
                    addToExtKeyList(cellBB, extKeyListBB, keylen, k, hashCode, -1, v, storedList, handler);
                }

                @Override
                public void error(String msg) {
                    logger.error(msg);
                    handler.error(k, msg);
                }
            });
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            handler.error(k, e.getMessage());
        }

    }

    private void addToExtKeyList(final ByteBuffer cellBB, final ByteBuffer extKeyListBB, int keylen, final K k, final int hashCode, int valuelen, V v, final List<KeyListEntry> storedList, final CacheHandler<K, V> handler) {
        if (keylen == -1)
            keylen = serializer.keySize(k);

        if (valuelen == -1) {
            if (v == null)
                valuelen = 0;
            else
                valuelen = serializer.valueSize(v);
        }

        ByteBuffer kvBB = createKeyValueBlock(keylen, k, hashCode, valuelen, v);

        final KeyListEntry newEntry = new KeyListEntry();
        newEntry.hashCode = hashCode;
        newEntry.kvSize = kvBB.capacity();

        try {
            newEntry.kvAddr = sm.allocate(kvBB.capacity());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            handler.error(k, e.getMessage());
            return;
        }

        try {
            sm.writeAsync(newEntry.kvAddr, kvBB, new AsyncHandler() {

                @Override
                public void success(ByteBuffer bb) {
                    reset(extKeyListBB);

                    int extListBytes = getExtKeyListBytes(cellBB);
                    storedList.add(newEntry);

                    if ((storedList.size() + 1) * 16 + CELL_EXTKLIST_OVERHEAD < extListBytes) {
                        updateExtKeyList(cellBB, extKeyListBB, k, hashCode, storedList, handler); // TODO free kvBB in case of error
                    } else {
                        growExtKeyList(cellBB, extKeyListBB, k, hashCode, storedList, newEntry, handler); // TODO free kvBB in case of error
                    }
                }

                @Override
                public void error(String msg) {
                    logger.error(msg);
                    handler.error(k, msg);
                }
            });
        } catch (IOException e) {
            sm.free(newEntry.kvAddr, kvBB.capacity());
            logger.error(e.getMessage(), e);
            handler.error(k, e.getMessage());
        }
    }

    private void growExtKeyList(final ByteBuffer cellBB, final ByteBuffer extKeyListBB, final K k, final int hashCode, final List<KeyListEntry> storedList, KeyListEntry newEntry, final CacheHandler<K, V> handler) {
        final int extListBytes = getExtKeyListBytes(cellBB);
        final int newExtListBytes = extListBytes * 2;

        final long newExtKeyListAddr;
        final long obsoleteExtKeyListAddr = getExtKeyListAddress(cellBB);
        try {
            newExtKeyListAddr = sm.allocate(extListBytes * 2);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            handler.error(k, ex.getMessage());
            return;
        }

        ByteBuffer newExtKeyListBB = sm.getByteBuffer(newExtListBytes);
        modifyExtKeyList(cellBB, newExtKeyListAddr, newExtListBytes);

        updateExtKeyList(cellBB, newExtKeyListBB, k, hashCode, storedList, new CacheHandler<K, V>() {
            @Override
            public void updated(final K k) {
                final long cellAddr = getCellAddress(hashCode);
                cellCaches.remove(cellAddr);
                try {
                    sm.writeAsync(cellAddr, cellBB, new AsyncHandler() {
                        @Override
                        public void success(ByteBuffer bb) {
                            //cellCaches.put(cellAddr, PersistenceDriverImpl.allocatePooled(cellBB.capacity()).put(cellBB));
                            sm.free(obsoleteExtKeyListAddr, extListBytes);
                            System.out.println("grew ext key list: " + obsoleteExtKeyListAddr + "->" + newExtKeyListAddr);
                            handler.updated(k);
                        }

                        @Override
                        public void error(String msg) {
                            sm.free(newExtKeyListAddr, newExtListBytes);
                            logger.error(msg);
                            handler.error(k, msg);
                        }
                    });
                } catch (IOException e) {
                    sm.free(newExtKeyListAddr, newExtListBytes);
                    logger.error(e.getMessage(), e);
                    handler.error(k, e.getMessage());
                }
            }

            @Override
            public void miss(K k) {
                throw new IllegalStateException();
            }

            @Override
            public void hit(K k, V v) {
                throw new IllegalStateException();
            }

            @Override
            public void error(K k, String msg) {
                logger.error(msg);
                handler.error(k, msg);
            }
        });
    }

    private void updateExtKeyList(final ByteBuffer cellBB, final ByteBuffer extKeyListBB, final K k, int hashCode, final List<KeyListEntry> storedList, final CacheHandler<K, V> handler) {
        reset(extKeyListBB);
        extKeyListBB.putInt(storedList.size()); // 1. list size

        long extKeylistAddr = getExtKeyListAddress(cellBB);
        //System.out.println("update ext key list: " + extKeylistAddr + ": " + storedList.size());
        for (KeyListEntry keyListEntry : storedList) {
            extKeyListBB.putInt(keyListEntry.hashCode); // 2. hash code
            extKeyListBB.putLong(keyListEntry.kvAddr); // 3. kv address
            extKeyListBB.putInt(keyListEntry.kvSize); // 4. kv length
            //System.out.println("  " + keyListEntry.hashCode + ":" + keyListEntry.kvAddr + ":" + keyListEntry.kvSize);
        }
        reset(extKeyListBB);

        try {
            sm.writeAsync(extKeylistAddr, extKeyListBB, new AsyncHandler() {
                @Override
                public void success(ByteBuffer bb) {
                    reset(cellBB);
                    handler.updated(k);
                }

                @Override
                public void error(String msg) {
                    handler.error(k, msg);
                }
            });
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            handler.error(k, e.getMessage());
        }
    }

    private ByteBuffer createKeyValueBlock(int keylen, final K k, final int hashCode, final int valuelen, V v) {
        final int alignedSize = PersistenceDriverImpl.getAlignedSize(keylen + valuelen + KV_OVERHEAD);
        int position;
        final ByteBuffer kvBB = sm.getByteBuffer(alignedSize);

        kvBB.putInt(keylen); // 1. key length
        position = kvBB.position();

        kvBB.limit(position + keylen);
        serializer.serializeKey(k, kvBB.slice()); // 2. key body
        kvBB.limit(alignedSize);
        kvBB.position(position + keylen);

        kvBB.putInt(valuelen); // 3. value length
        position = kvBB.position();

        if (valuelen != 0) {
            kvBB.limit(position + valuelen);
            serializer.serializeValue(v, kvBB.slice()); // 4. value body
            kvBB.limit(alignedSize);
            kvBB.position(position + valuelen);
        }

        reset(kvBB);

        return kvBB;
    }

    private ByteBuffer createKeyValueBlock(ByteBuffer keyBB, final int hashCode, ByteBuffer valueBB) {
        final int alignedSize = PersistenceDriverImpl.getAlignedSize(keyBB.capacity() + valueBB.capacity() + KV_OVERHEAD);
        final ByteBuffer kvBB = sm.getByteBuffer(alignedSize);

        kvBB.putInt(keyBB.capacity()); // 1. key length
        kvBB.put(keyBB); // 2. key body
        kvBB.putInt(valueBB.capacity()); // 3. value length
        kvBB.put(valueBB);
        reset(kvBB);
        return kvBB;
    }

    private long getExtKeyListAddress(ByteBuffer cellBB) {
        reset(cellBB);
        //byte type = //
        cellBB.get(); // 1. type
        //int size = //
        cellBB.getInt(); // 2. list size
        long ret = cellBB.getLong(); // 3. external list address
        reset(cellBB);
        return ret;
    }

    private void modifyExtKeyList(ByteBuffer cellBB, long extKeyListAddr, int extKeyListBytes) {
        reset(cellBB);
        cellBB.get(); // 1. type
        cellBB.getInt(); // 2. list size
        cellBB.putLong(extKeyListAddr); // 3. external list address
        cellBB.putInt(extKeyListBytes); // 4. size
        reset(cellBB);
    }

    private int getExtKeyListBytes(ByteBuffer cellBB) {
        reset(cellBB);
        //byte type = 
        cellBB.get(); // 1. type
        //int listSize = 
        cellBB.getInt(); // 2. list size
        //long addr = 
        cellBB.getLong(); // 3. external list address
        int ret = cellBB.getInt(); // 4. size
        reset(cellBB);
        return ret;
    }

    public String stat(int sampling) {
        final LazySynchronizer synch = new LazySynchronizer(100);
        final ConcurrentHashMap<Long, Integer> extKeyListAddr2Size = new ConcurrentHashMap<>();

        final AtomicLong blank = new AtomicLong();
        final AtomicLong inCell = new AtomicLong();
        final AtomicLong inList = new AtomicLong();

        for (int cellIdx = 0; cellIdx < numOfCells; ++cellIdx) {
            if (cellIdx % sampling != 0)
                continue;

            final AtomicBoolean checkpoint = synch.checkpoint();

            long cellAddr = getCellAddrByIndex(cellIdx);
            try {
                sm.readAsync(cellAddr, cellUnitByteSize, new AsyncHandler() {
                    @Override
                    public void success(ByteBuffer cellBB) {
                        reset(cellBB);
                        byte type = cellBB.get(); // 1. type

                        //CELL_KV_OVERHEAD = 21; // 1. type, 2. hash code, 3. key length, 4. key body, 5. value length, 6. value body
                        //CELL_KLIST_OVERHEAD = 17; // 1. type, 2. list size, 3. external list, 4. external list size
                        //CELL_EXTKLIST_OVERHEAD = 4; // 1. list size
                        //KV_OVERHEAD = 16; // 1. key length, 2. key body, 3. value length, 4. value body

                        switch (type) {
                        case TYPE_NULL: {
                            blank.incrementAndGet();
                            break;
                        }
                        case TYPE_KV: {
                            inCell.incrementAndGet();
                            break;
                        }
                        case TYPE_KLIST: {
                            int keySize = cellBB.getInt(); // 2. list size
                            inList.addAndGet(keySize);
                            long extKeyListAddr = cellBB.getLong(); // 3. external list address
                            int extKeyListSize = cellBB.getInt(); // 4. external list size
                            if (extKeyListAddr != CELL_ADDRESS)
                                extKeyListAddr2Size.put(extKeyListAddr, extKeyListSize);
                            break;
                        }
                        }
                        synch.processed(checkpoint);
                    }

                    @Override
                    public void error(String msg) {
                        synch.processed(checkpoint);
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
                synch.processed(checkpoint);
            }
        }

        try {
            sm.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
            return "error";
        }
        synch.synchAll();

        synch.init(100);
        for (Map.Entry<Long, Integer> entry : extKeyListAddr2Size.entrySet()) {
            final AtomicBoolean checkpoint = synch.checkpoint();
            try {
                sm.readAsync(entry.getKey(), entry.getValue(), new AsyncHandler() {

                    @Override
                    public void success(ByteBuffer kvBB) {
                        reset(kvBB);
                        // 1. list size
                        inList.addAndGet(kvBB.getInt());

                        synch.processed(checkpoint);
                    }

                    @Override
                    public void error(String msg) {
                        synch.processed(checkpoint);
                    }
                });
            } catch (

            IOException ex) {
                ex.printStackTrace();
                synch.processed(checkpoint);

            }

            synch.synchAll();

        }

        return "cell: " + inCell + ", list: " + inList + ", empty: " + blank;
    }

    public void close() throws IOException {

        flush();

        for (PersistenceDriver driver : drivers)
            driver.close();

    }

}
