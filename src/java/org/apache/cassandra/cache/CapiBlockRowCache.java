/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cache;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.cassandra.cache.SerializingCacheProvider.RowCacheSerializer;
import org.apache.cassandra.cache.capi.CapiCache;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;
import com.ibm.research.capiblock.CapiBlockDevice;

public class CapiBlockRowCache implements ICache<RowCacheKey, IRowCacheEntry>, EvictionListener<RowCacheKey, IRowCacheEntry>
{
    private static final Logger logger = LoggerFactory.getLogger(CapiBlockRowCache.class);

    public interface HashFunction
    {
        int hashCode(byte[] bb);
    }
    public static final String PROP_CAPI_DEVICE_NAMES = "capi.devices";

    public static final long DEFAULT_START_OFFSET = 0L;
    public static final long DEFAULT_SIZE_IN_TB_BYTES = 10L; // 10 giga
    public static final int CACHE_LOWER_BOUND = 1024;
    
    public static final int DEFAULT_CONCURENCY_LEVEL = 64;
    private final ConcurrentLinkedHashMap<RowCacheKey, IRowCacheEntry> map;
    private final CapiCache<RowCacheKey, IRowCacheEntry> capiCache;
    final HashFunction hashFunc;

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
                serializer.serialize(value, new DataOutputBufferFixed(bb));
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
                return serializer.deserialize(new DataInputBuffer(bb, false));
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
            int size = (int) serializer.serializedSize(v);

            if (size == Integer.MAX_VALUE)
                throw new IllegalStateException();

            return size + 1;
        }
    };

    ISerializer<IRowCacheEntry> serializer = new RowCacheSerializer();

    public CapiBlockRowCache(long capacity)
    {
        this.map = new ConcurrentLinkedHashMap.Builder<RowCacheKey, IRowCacheEntry>()
                .weigher(new Weigher<IRowCacheEntry>()
                {
                    public int weightOf(IRowCacheEntry value)
                    {
                        long serializedSize = serializer.serializedSize(value);
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

        int cellByte = CapiBlockDevice.BLOCK_SIZE;
        logger.info("capicache: cellsize=" + (CapiBlockDevice.BLOCK_SIZE / 1024) + "KB");

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

                capiCache.addDevice(deviceName, offset, sizeInBytes / CapiBlockDevice.BLOCK_SIZE, mirrorDevices);

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

    public long capacity()
    {
        return map.capacity();
    }

    public void setCapacity(long capacity)
    {
        map.setCapacity(capacity);
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public int size()
    {
        return map.size();
    }

    public long weightedSize()
    {
        return map.weightedSize();
    }

    public void clear()
    {
        map.clear();
    }

    public IRowCacheEntry get(RowCacheKey key)
    {
        return map.get(key);
    }

    public void put(RowCacheKey key, IRowCacheEntry value)
    {
        map.put(key, value);
    }

    public boolean putIfAbsent(RowCacheKey key, IRowCacheEntry value)
    {
        return map.putIfAbsent(key, value) == null;
    }

    public boolean replace(RowCacheKey key, IRowCacheEntry old, IRowCacheEntry value)
    {
        return map.replace(key, old, value);
    }

    public void remove(RowCacheKey key)
    {
        map.remove(key);
    }

    public Iterator<RowCacheKey> keyIterator()
    {
        return map.keySet().iterator();
    }

    public Iterator<RowCacheKey> hotKeyIterator(int n)
    {
        return map.descendingKeySetWithLimit(n).iterator();
    }

    public boolean containsKey(RowCacheKey key)
    {
        return map.containsKey(key);
    }
    
    public void onEviction(final RowCacheKey k, final IRowCacheEntry v)
    {
        if (v instanceof RowCacheSentinel)
            return;
    }
}
