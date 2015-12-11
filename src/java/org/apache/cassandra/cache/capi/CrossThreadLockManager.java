package org.apache.cassandra.cache.capi;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrossThreadLockManager<K> {
    private static final Logger logger = LoggerFactory.getLogger(CrossThreadLockManager.class);

    private static int NUM_OF_SLOT = 1023;
    @SuppressWarnings("unchecked")
    private final Map<K, Integer>[] lockEntrySlots = new HashMap[NUM_OF_SLOT];

    CrossThreadLockManager() {
        for (int i = 0; i < NUM_OF_SLOT; ++i)
            lockEntrySlots[i] = new HashMap<>();
    }

    public int size() {
        int ret = 0;
        for (int i = 0; i < NUM_OF_SLOT; ++i)
            ret += lockEntrySlots[i].size();
        return ret;
    }

    private Map<K, Integer> getEntrySlot(K k) {
        try {
            int hashCode = k.hashCode();

            int index = hashCode % lockEntrySlots.length;
            if (index < 0)
                index *= -1;

            return lockEntrySlots[index];
        } catch (ArrayIndexOutOfBoundsException ex) {
            throw new IllegalStateException("hash=" + k.hashCode() + ", slots=" + lockEntrySlots.length, ex);
        }
    }

    public boolean readLock(K k) {
        //logger.info("rlock:" + k + "(" + k.hashCode() + ":" + (Math.abs(k.hashCode()) % lockEntrySlots.length) + ")");
        while (true) {
            Map<K, Integer> slot = getEntrySlot(k);
            synchronized (slot) {
                Integer lock = slot.get(k);
                if (lock == null) {
                    slot.put(k, 1);
                    return true;
                }
                if (lock == -1) {
                    return false;
                } else {
                    slot.put(k, lock + 1);
                    return true;
                }
            }
        }
    }

    public boolean writeLock(K k) {
        //logger.info("wlock:" + k + "(" + k.hashCode() + ":" + (Math.abs(k.hashCode()) % lockEntrySlots.length) + ")");
        while (true) {
            Map<K, Integer> slot = getEntrySlot(k);
            synchronized (slot) {
                Integer lock = slot.get(k);
                if (lock == null) {
                    slot.put(k, -1);
                    return true;
                }
                //logger.info("wlock-wait:" + k + "(" + k.hashCode() + ":" + (k.hashCode() % lockEntrySlots.length) + ")");
                //logger.info("wlock-again:" + k + "(" + k.hashCode() + ":" + (k.hashCode() % lockEntrySlots.length) + ")");
                return false;
            }
        }
    }

    public void release(K k) {
        //logger.info("release:" + k + ":" + k.hashCode() + ":" + Math.abs(k.hashCode()) + ":"+ "(" + (Math.abs(k.hashCode()) % lockEntrySlots.length) + ")");
        Map<K, Integer> slot = getEntrySlot(k);
        synchronized (slot) {
            Integer lock = slot.get(k);
            if (lock == null)
                throw new IllegalStateException("too much release");
            if (lock == -1) {
                slot.remove(k);
            } else {
                --lock;
                if (lock == 0) {
                    slot.remove(k);
                    //System.out.println("release: " + k);
                } else {
                    slot.put(k, lock);
                }
            }
        }
    }

    public void releaseRead(K k) {
        //logger.info("release:" + k + ":" + k.hashCode() + ":" + Math.abs(k.hashCode()) + ":"+ "(" + (Math.abs(k.hashCode()) % lockEntrySlots.length) + ")");
        Map<K, Integer> slot = getEntrySlot(k);
        synchronized (slot) {
            Integer lock = slot.get(k);
            if (lock == null)
                throw new IllegalStateException("too much release");
            if (lock == -1) {
                logger.error("lock for write. releae for read.");
                slot.remove(k);
            } else {
                --lock;
                if (lock == 0) {
                    slot.remove(k);
                    //System.out.println("release: " + k);
                } else {
                    slot.put(k, lock);
                }
            }
        }
    }

    public void releaseWrite(K k) {
        //logger.info("release:" + k + ":" + k.hashCode() + ":" + Math.abs(k.hashCode()) + ":"+ "(" + (Math.abs(k.hashCode()) % lockEntrySlots.length) + ")");
        Map<K, Integer> slot = getEntrySlot(k);
        synchronized (slot) {
            Integer lock = slot.get(k);
            if (lock == null)
                throw new IllegalStateException("too much release");
            if (lock == -1) {
                slot.remove(k);
            } else {
                --lock;
                logger.error("lock for read. releae for write.");
                if (lock == 0) {
                    slot.remove(k);
                    //System.out.println("release: " + k);
                } else {
                    slot.put(k, lock);
                }
            }
        }
    }

    // private final ConcurrentHashMap<K, AtomicInteger> locks = new ConcurrentHashMap<>();
    //
    // public int size() {
    // return locks.size();
    // }
    //
    // public void readLock(K k) {
    // AtomicInteger lock = locks.get(k);
    // AtomicInteger tmp;
    // while (true) {
    // if (lock == null) {
    // lock = new AtomicInteger(1);
    // tmp = locks.putIfAbsent(k, lock);
    // if (tmp == null)
    // return;
    // lock = tmp;
    // }
    //
    // if (lock.get() < 0) {
    // synchronized (lock) {
    // while (lock.get() < 0) {
    // try {
    // lock.wait();
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    // }
    // }
    // }
    //
    // int count = lock.get();
    // if (count >= 0 && lock.compareAndSet(count, count + 1))
    // return;
    //
    // lock = locks.get(k);
    // }
    // }
    //
    // public void release(K k) {
    // AtomicInteger lock = locks.get(k);
    // if (lock == null)
    // throw new IllegalStateException();
    //
    // if (lock.get() > 0)
    // releaseReadlock(k, lock);
    // else
    // releaseWritelock(k, lock);
    //
    // }
    //
    // private void releaseReadlock(K k, AtomicInteger lock) {
    // int count = lock.decrementAndGet();
    // if (count == 0) {
    // if (lock.compareAndSet(0, -1)) {
    // locks.remove(k);
    // lock.set(0);
    // synchronized (lock) {
    // lock.notifyAll();
    // }
    // return;
    // }
    // }
    // }
    //
    // private void releaseWritelock(K k, AtomicInteger lock) {
    // locks.remove(k);
    // lock.set(0);
    // synchronized (lock) {
    // lock.notifyAll();
    // }
    // }
    //
    // public void writeLock(K k) {
    // AtomicInteger lock = locks.get(k);
    // AtomicInteger tmp;
    // while (true) {
    // if (lock == null) {
    // lock = new AtomicInteger(-1);
    // tmp = locks.putIfAbsent(k, lock);
    // if (tmp == null)
    // return;
    // lock = tmp;
    // }
    //
    // while (true) {
    // synchronized (lock) {
    // tmp = locks.get(k);
    // if (tmp == null) {
    // lock = null;
    // break;
    // }
    // if (tmp == lock) {
    // try {
    // lock.wait();
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    // } else {
    // lock = tmp;
    // }
    // }
    // }
    // }
    // }

}
