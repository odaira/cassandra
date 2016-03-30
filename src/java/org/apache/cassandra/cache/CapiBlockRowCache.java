package org.apache.cassandra.cache;

import java.io.DataInput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.cache.capi.CapiCache;
import org.apache.cassandra.cache.capi.CapiCache.CacheHandler;
import org.apache.cassandra.cache.capi.CapiChunkDriver;
import org.apache.cassandra.cache.capi.PersistenceDriver;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputByteBuffer;
import org.apache.cassandra.io.util.DataOutputByteBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.vint.EncodedDataInputStream;
import org.apache.cassandra.utils.vint.EncodedDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;

public class CapiBlockRowCache //
        implements ICache<RowCacheKey, IRowCacheEntry>, EvictionListener<RowCacheKey, IRowCacheEntry>
{
    private static final Logger logger = LoggerFactory.getLogger(CapiBlockRowCache.class);

    public static final String PROP_CAPI_DEVICE_NAMES = "capi.devices";

    private static final int DEFAULT_CONCURENCY_LEVEL = 64;

    public static final long DEFAULT_START_OFFSET = 0L;
    public static final long DEFAULT_SIZE_IN_TB_BYTES = 10L; // 10 giga
    public static final int CACHE_LOWER_BOUND = 1024;

    public static AtomicLong touched = new AtomicLong();
    public static AtomicLong cacheHit = new AtomicLong();
    public static AtomicLong cacheMiss = new AtomicLong();
    public static AtomicLong swapin = new AtomicLong();
    public static AtomicLong swapinMiss = new AtomicLong();
    public static AtomicLong swapinErr = new AtomicLong();
    public static AtomicLong remove = new AtomicLong();

    public interface HashFunction
    {
        int hashCode(byte[] bb);
    }

    final CapiCache<RowCacheKey, IRowCacheEntry> capiCache;
    final ConcurrentLinkedHashMap<RowCacheKey, IRowCacheEntry> map;
    final HashFunction hashFunc;

    ISerializer<IRowCacheEntry> serializer = new ISerializer<IRowCacheEntry>()
    {
        public void serialize(IRowCacheEntry entry, DataOutputPlus out) throws IOException
        {
            assert entry != null; // unlike CFS we don't support nulls, since there is no need for that in the cache
            boolean isSentinel = entry instanceof RowCacheSentinel;
            out.writeBoolean(isSentinel);
            if (isSentinel)
                out.writeLong(((RowCacheSentinel) entry).sentinelId);
            else
                ColumnFamily.serializer.serialize((ColumnFamily) entry, out, MessagingService.current_version);
        }

        public IRowCacheEntry deserialize(DataInput in) throws IOException
        {
            boolean isSentinel = in.readBoolean();
            if (isSentinel)
                return new RowCacheSentinel(in.readLong());
            return ColumnFamily.serializer.deserialize(in, MessagingService.current_version);
        }

        public long serializedSize(IRowCacheEntry entry, TypeSizes typeSizes)
        {
            int size = typeSizes.sizeof(true);
            if (entry instanceof RowCacheSentinel)
                size += typeSizes.sizeof(((RowCacheSentinel) entry).sentinelId);
            else
                size += ColumnFamily.serializer.serializedSize((ColumnFamily) entry, typeSizes,
                        MessagingService.current_version);
            return size;
        }
    };

    // Weighers.<RefCountedMemory> singleton()
    public CapiBlockRowCache(long capacity)
    {
        this.map = new ConcurrentLinkedHashMap.Builder<RowCacheKey, IRowCacheEntry>()
                .weigher(new Weigher<IRowCacheEntry>()
                {
                    public int weightOf(IRowCacheEntry value)
                    {
                        long serializedSize = serializer.serializedSize(value, TypeSizes.VINT);
                        if (serializedSize > Integer.MAX_VALUE)
                            throw new IllegalArgumentException("Unable to allocate " + serializedSize + " bytes");

                        return (int) serializedSize;
                    }
                })
                .maximumWeightedCapacity(capacity)
                .concurrencyLevel(DEFAULT_CONCURENCY_LEVEL)
                .listener(this)
                .build();

        String hashClass = System.getProperty("capi.hash");
        if (hashClass == null)
        {
            hashFunc = new HashFunction()
            {
                @Override
                public int hashCode(byte[] key)
                {
                    return Arrays.hashCode(key);
                }
            };
            logger.info("default hash function is loaded. class=" + hashFunc.getClass().getName());
        }
        else
        {
            try
            {
                hashFunc = (HashFunction) Class.forName(hashClass).newInstance();
                logger.info("custom hash function is loaded. class=" + hashFunc.getClass().getName());
            }
            catch (Exception e)
            {
                throw new IllegalStateException(e);
            }
        }

        // -Dcapi.cell=2048
        long cellSize = 1024L * 1024L * 1024L * Integer.parseInt(System.getProperty("capi.cell", "10"));
        logger.info("capicache: cell=" + (cellSize / 1024.0 / 1024.0 / 1024.0) + "GB");

        int cellByte = PersistenceDriver.BLOCK_SIZE;
        logger.info("capicache: cellsize=" + (PersistenceDriver.BLOCK_SIZE / 1024) + "KB");

        // -Dcapi.async=256
        int numOfAsync = Integer.parseInt(System.getProperty("capi.async", "64"));
        logger.info("capicache: asynch=" + numOfAsync);

        this.capiCache = new CapiCache<RowCacheKey, IRowCacheEntry>(cellSize, cellByte, numOfAsync, cacheSerializer);

        String deviceNamesStr = System.getProperty(PROP_CAPI_DEVICE_NAMES);
        if (deviceNamesStr == null)
        {
            logger.error(
                    "no valid device name in " + PROP_CAPI_DEVICE_NAMES);
            throw new IllegalStateException("no valid device name in " + PROP_CAPI_DEVICE_NAMES);
        }

        // -Dcapi.devices=/dev/sg7:<OFFSET>:<GB_SIZE>:/dev/sg8:/dev/sg9:/dev/sg10,/dev/sdc:<OFFSET>:<GB_SIZE>
        String[] deviceInfos = deviceNamesStr.split(",");

        for (String deviceInfo : deviceInfos)
        {
            try
            {
                // /dev/sdb:<OFFSET>:<GB_SIZE>
                String[] deviceAttrs = deviceInfo.split(":");
                String deviceName = deviceAttrs[0];
                long offset = deviceAttrs.length > 1 ? Long.parseLong(deviceAttrs[1]) : 0;
                long sizeInBytes = 1024L * 1024L * 1024L *
                        (deviceAttrs.length > 2 ? Long.parseLong(deviceAttrs[2]) : DEFAULT_SIZE_IN_TB_BYTES);
                String[] mirrorDevices = new String[Math.max(0, deviceAttrs.length - 3)];
                for (int i = 0; i < mirrorDevices.length; ++i)
                    mirrorDevices[i] = deviceAttrs[i + 3];

                capiCache.addDevice(deviceName, offset, sizeInBytes / PersistenceDriver.BLOCK_SIZE, mirrorDevices);

                logger.info("capicache: device=" + deviceName + ", start=" + offset + ", size="
                        + (sizeInBytes / 1024.0 / 1024.0) + "MB");
            }
            catch (IOException ex)
            {
                logger.error("errors to create chunks for " + deviceNamesStr, ex);
                throw new IllegalStateException(ex);
            }
        }

        // -Dcapi.restore=true
        try
        {
            capiCache.initialize(Boolean.parseBoolean(System.getProperty("capi.init", "true")));
        }
        catch (IOException ex)
        {
            logger.error(ex.getMessage(), ex);
            throw new IllegalStateException(ex);
        }
    }

    CapiCache.Serializer<RowCacheKey, IRowCacheEntry> cacheSerializer = new CapiCache.Serializer<RowCacheKey, IRowCacheEntry>()
    {
        @Override
        public int hashCode(RowCacheKey key)
        {
            int result = key.ksAndCFName.hashCode();
            result = 31 * result + (key != null ? hashFunc.hashCode(key.key) : 0);
            return result;
        }

        @Override
        public boolean equals(RowCacheKey key, ByteBuffer keyBB)
        {
            if (!readString(keyBB).equals(key.ksAndCFName.left))
                return false;
            if (!readString(keyBB).equals(key.ksAndCFName.right))
                return false;
            if (keyBB.remaining() != key.key.length)
                return false;

            for (int i = 0; i < key.key.length; ++i)
                if (key.key[i] != keyBB.get())
                    return false;
            return true;
        }

        @Override
        public void serializeKey(RowCacheKey key, ByteBuffer bb)
        {
            try
            {
                // logger.info("serializeKey=" + bb + ": " + key);
                writeString(bb, key.ksAndCFName.left);
                writeString(bb, key.ksAndCFName.right);
                bb.put(key.key);
                bb.rewind();
            }
            catch (BufferOverflowException ex)
            {
                logger.error("ideal: " + (getByteSizeForString(key.ksAndCFName.left) + getByteSizeForString(key.ksAndCFName.right) + key.key.length) + ", actual=" + bb.capacity());
                throw ex;
            }
        }

        @Override
        public void serializeValue(IRowCacheEntry value, ByteBuffer bb)
        {
            try
            {
                serializer.serialize(value, new EncodedDataOutputStream(new DataOutputByteBuffer(bb)));
            }
            catch (IOException e)
            {
                logger.debug("Cannot fetch in memory data, we will fallback to read from disk ", e);
                throw new IllegalStateException(e);
            }
        }
        
        public void writeString(ByteBuffer bb, String str) 
        {
            byte[] bytes = str.getBytes();
            bb.putInt(bytes.length);
            bb.put(bytes);
        }
        
        public  String readString(ByteBuffer bb) 
        {
            int length = bb.getInt();
            byte[] bytes = new byte[length];
            bb.get(bytes);
            return new String(bytes);
        }

        public  int getByteSizeForString(String str) 
        {
            return str.getBytes().length + 4;
        }
        
        @Override
        public RowCacheKey deserializeKey(ByteBuffer bb)
        {
            String ksName = readString(bb);
            String cfName = readString(bb);
            ByteBuffer keyBody = ByteBuffer.allocateDirect(bb.remaining());
            keyBody.put(bb);
            keyBody.rewind();
            return new RowCacheKey(Pair.create(ksName, cfName), keyBody);
        }

        @Override
        public IRowCacheEntry deserializeValue(ByteBuffer bb)
        {
            try
            {
                return serializer.deserialize(new EncodedDataInputStream(new DataInputByteBuffer(bb)));
            }
            catch (IOException e)
            {
                logger.debug("Cannot fetch in memory data, we will fallback to read from disk ", e);
                throw new IllegalStateException(e);
            }
        }

        @Override
        public int keySize(RowCacheKey k)
        {
            return getByteSizeForString(k.ksAndCFName.left) + getByteSizeForString(k.ksAndCFName.right) + k.key.length;
        }

        @Override
        public int valueSize(IRowCacheEntry v)
        {
            int size = (int) serializer.serializedSize(v, TypeSizes.VINT);

            if (size == Integer.MAX_VALUE)
                throw new IllegalStateException();

            return size + 1;
        }
    };

    public void onEviction(final RowCacheKey k, final IRowCacheEntry v)
    {
        if (v instanceof RowCacheSentinel)
            return;
    }

    public Set<RowCacheKey> keySet()
    {
        // logger.info("#####CapiRowCache keySet");
        // TODO
        return map.keySet();
    }

    public long capacity()
    {
        // logger.info("#####CapiRowCache capacity");
        // this method is called to check whether the system uses row cache or not
        // TODO
        return map.capacity();
    }

    public void setCapacity(long capacity)
    {
        // logger.info("#####CapiRowCache setCapacity");
        // TODO
        map.setCapacity(capacity);
    }

    public boolean isEmpty()
    {
        // logger.info("#####CapiRowCache isEmpty");
        throw new UnsupportedOperationException();
    }

    public int size()
    {
        // TODO
        return map.size();
    }

    public long weightedSize()
    {
        // TODO
        return map.weightedSize();
    }

    public void clear()
    {
        // TODO
        map.clear();
    }

    public IRowCacheEntry get(RowCacheKey key)
    {
        IRowCacheEntry entry = map.get(key);

        if (entry == null)
        {
            try
            {
                // logger.info("> CAPI-getSync");
                // try
                // {
                // throw new Exception();
                // }
                // catch (Exception ex)
                // {
                // logger.error("get: ", ex);
                // ex.printStackTrace();
                // }

                entry = capiCache.getSync(key);
                if (entry != null)
                    swapin.incrementAndGet();
                else
                    swapinMiss.incrementAndGet();
                // logger.info("< CAPI-getSync");
            }
            catch (IOException ex)
            {
                if (swapinErr.incrementAndGet() % logUnit == 0)
                    log();
            }
        }

        if (entry == null)
        {
            if (cacheMiss.incrementAndGet() % logUnit == 0)
                log();
        }
        else
        {
            if (cacheHit.incrementAndGet() % logUnit == 0)
                log();
        }
        return entry;
    }

    public void put(RowCacheKey key, IRowCacheEntry value)
    {
        // logger.info("#####CapiRowCache put");

        map.put(key, value);

        // logger.info("> CAPI-put (put)");
        capiCache.put(key, value);
    }

    public boolean putIfAbsent(RowCacheKey key, IRowCacheEntry value)
    {
        // logger.info("#####CapiRowCache putIfAbsent");

        if (!(value instanceof RowCacheSentinel))
            throw new UnsupportedOperationException();

        // logger.info("putIfAbsent: " + value);
        boolean ret = map.putIfAbsent(key, value) == null;
        return ret;
    }

    public boolean replace(final RowCacheKey k, final IRowCacheEntry oldToReplace, final IRowCacheEntry v)
    {
        // logger.info("#####CapiRowCache replace");

        if (!(oldToReplace instanceof RowCacheSentinel))
            throw new UnsupportedOperationException();

        // if there is no old value in our map, we fail
        IRowCacheEntry old = map.get(k);
        if (old == null)
        {
            // logger.info("> CAPI-put (replace1)");
            capiCache.remove(k);
            return false;
        }

        synchronized (old)
        {
            if (!old.equals(oldToReplace))
                return false;

            if (map.replace(k, old, v))
            {
                // logger.info("> CAPI-put (replace2)");
                capiCache.put(k, v);
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    public void remove(RowCacheKey k)
    {
        // logger.info("#####CapiRowCache remove");
        // try
        // {
        // throw new Exception();
        // }
        // catch (Exception ex)
        // {
        // logger.error("remove: ", ex);
        // ex.printStackTrace();
        // }

        // logger.info("> CAPI-put (remove)");
        capiCache.remove(k, new CacheHandler<RowCacheKey, IRowCacheEntry>()
        {

            @Override
            public void hit(RowCacheKey k, IRowCacheEntry v)
            {
                throw new IllegalStateException();
            }

            @Override
            public void miss(RowCacheKey k)
            {
            }

            @Override
            public void error(RowCacheKey k, String msg)
            {
                logger.error("capi cache remove error: " + msg);
            }

            @Override
            public void updated(RowCacheKey k)
            {
                if (remove.incrementAndGet() % logUnit == 0)
                    log();
            }
        });
    }

    public Set<RowCacheKey> hotKeySet(int n)
    {
        // logger.info("#####CapiRowCache hotKeySet");
        return map.descendingKeySetWithLimit(n);
    }

    public boolean containsKey(RowCacheKey key)
    {
        // logger.info("#####CapiRowCache containsKey");
        throw new UnsupportedOperationException();
    }

    long logUnit = 1000000L;
    long lastLog = System.currentTimeMillis();
    long lastCapiRead = 0L;
    long lastCapiRowRead = 0L;

    private void log()
    {
        long now = System.currentTimeMillis();
        long elapsed = now - lastLog;
        lastLog = now;

        long currentCapiRead = swapin.incrementAndGet() + swapinMiss.incrementAndGet();
        long count = currentCapiRead - lastCapiRead;
        lastCapiRead = currentCapiRead;

        long currentCapiRowRead = CapiChunkDriver.executed.get();
        long rowCount = currentCapiRowRead - lastCapiRowRead;
        lastCapiRowRead = currentCapiRowRead;

        logger.info("cache hit/miss : " + cacheHit + "/" + cacheMiss
                + ", "//
                + "total swapped-in (success/miss/error/remove) : " + swapin + "/" + swapinMiss + "/" + swapinErr + "/"
                + remove + ", throughput (cache/capi): " + (count / (double) elapsed) * 1000.0 + "/"
                + (rowCount / (double) elapsed) * 1000.0);
    }

}
