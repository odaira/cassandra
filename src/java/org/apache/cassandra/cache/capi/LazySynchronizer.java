package org.apache.cassandra.cache.capi;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LazySynchronizer {
    int cap;
    AtomicBoolean lastCheckpoint = new AtomicBoolean(false);
    AtomicLong processed = new AtomicLong(0L);
    AtomicLong marked = new AtomicLong(0L);

    public LazySynchronizer(int cap) {
        this.cap = cap;
    }

    public void init(int cap) {
        this.cap = cap;
        lastCheckpoint.set(false);
        processed.set(0L);
        marked.set(0L);
    }

    public AtomicBoolean checkpoint() {
        AtomicBoolean checkpoint;

        long count = marked.incrementAndGet() - 1L;
        if (count % (long) cap == 0L) {
            if (count != 0L) {
                synchronized (lastCheckpoint) {
                    while (!lastCheckpoint.get())
                        try {
                            lastCheckpoint.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                }
            }
            checkpoint = new AtomicBoolean(false);
            lastCheckpoint = checkpoint;
        } else {
            checkpoint = null;
        }
        return checkpoint;
    }

    public void processed(AtomicBoolean checkpoint) {
        if (checkpoint != null) {
            synchronized (checkpoint) {
                checkpoint.set(true);
                checkpoint.notify();
            }
        }
        processed.incrementAndGet();
    }

    public void synchAll() {
        while (processed.get() != marked.get())
            try {
                Thread.sleep(10L);
            } catch (InterruptedException ex) {
                throw new IllegalStateException(ex);
            }
    }
}
