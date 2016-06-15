package org.apache.cassandra.cache.capi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CrossThreadLockManager<K> {

    static class LockState {
        Object owner;
        boolean write;

        LockState(Object owner, boolean write) {
            this.owner = owner;
            this.write = write;
        }
    }

    private static int NUM_OF_SLOT = 1023;
    @SuppressWarnings("unchecked")
    private final Map<K, List<LockState>>[] lockEntrySlots = new HashMap[NUM_OF_SLOT];

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

    private Map<K, List<LockState>> getEntrySlot(K k) {
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

    public boolean readLock(Object owner, K k) {
        return readLock(owner, k, false);
    }

    public boolean readLock(Object owner, K k, boolean block) {
        while (true) {
            Map<K, List<LockState>> slot = getEntrySlot(k);
            synchronized (slot) {
                List<LockState> lockStates = slot.get(k);
                if (lockStates == null) {
                    lockStates = new ArrayList<>();
                    lockStates.add(new LockState(owner, false));
                    slot.put(k, lockStates);
                    return true;
                }
                if (lockStates.size() == 1 && lockStates.get(0).write) {
                    if (!block)
                        return false;
                    try {
                        slot.wait();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                } else {
                    lockStates.add(new LockState(owner, false));
                    return true;
                }
            }
        }
    }

    public boolean writeLock(Object owner, K k) {
        return writeLock(owner, k, false);
    }

    public boolean writeLock(Object owner, K k, boolean block) {
        while (true) {
            Map<K, List<LockState>> slot = getEntrySlot(k);
            synchronized (slot) {
                List<LockState> lockStates = slot.get(k);
                if (lockStates == null) {
                    lockStates = new ArrayList<>();
                    lockStates.add(new LockState(owner, true));
                    slot.put(k, lockStates);
                    return true;
                }
                if (!block)
                    return false;
                try {
                    slot.wait();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    @SuppressWarnings("serial")
    public static class AlreadyReleased extends Exception {

        AlreadyReleased(String msg) {
            super(msg);
        }
    }

    public void release(Object owner, K k) throws AlreadyReleased {
        Map<K, List<LockState>> slot = getEntrySlot(k);
        synchronized (slot) {
            List<LockState> lockStates = slot.get(k);
            if (lockStates == null)
                throw new AlreadyReleased("already released: key=" + k);
            if (lockStates.size() == 1 && lockStates.get(0).write) {
                slot.remove(k);
            } else {
                LockState lockState = null;
                for (LockState tmp : lockStates) {
                    if (tmp.owner == owner) {
                        lockState = tmp;
                        break;
                    }
                }
                if (lockState == null)
                    throw new AlreadyReleased("already released: key=" + k);

                lockStates.remove(lockState);
                if (lockStates.isEmpty()) {
                    slot.remove(k);
                }
            }
            slot.notifyAll();
        }
    }
}
