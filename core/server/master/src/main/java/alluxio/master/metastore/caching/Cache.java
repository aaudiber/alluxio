/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.metastore.caching;

import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

public abstract class Cache<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(Cache.class);

  private final ConcurrentHashMap<K, Entry> mMap = new ConcurrentHashMap<>();

  private final int mMaxSize;
  private final int mHighWaterMark;
  private final int mLowWaterMark;

  private final EvictionThread mEvictionThread;
//  private final Object mCacheFullMonitor = new Object();

  public Cache(int maxSize, int highWaterMark, int lowWaterMark) {
    mMaxSize = maxSize;
    mHighWaterMark = highWaterMark;
    mLowWaterMark = lowWaterMark;
    mEvictionThread = new EvictionThread();
    mEvictionThread.setDaemon(true);
    mEvictionThread.start();
  }

  protected abstract Optional<V> load(K key);
  protected abstract Optional<LockResource> tryLock(K key);
  protected abstract void evictToBackingStore(K key, V value);
  protected abstract void removeFromBackingStore(K key);

  public Optional<V> get(K key) {
    blockIfCacheFull();
    Entry entry = mMap.computeIfAbsent(key, this::loadEntry);
    if (entry.mValue == null) { // Indicates that the inode was removed.
      return Optional.empty();
    }
    checkCacheSize();
    if (entry == null) {
      return Optional.empty();
    }
    entry.mAccessed = true;
    return Optional.of(entry.mValue);
  }

  public void put(K key, V value) {
    blockIfCacheFull();
    mMap.compute(key, (prevKey, prevValue) -> {
      if (prevValue == null) {
        return new Entry(key, value);
      }
      prevValue.mValue = value;
      prevValue.mAccessed = true;
      prevValue.mDirty = true;
      return prevValue;
    });
    checkCacheSize();
  }

  public void remove(K key) {
    // Set the entry so that it will be removed from the backing store when it is encountered by
    // the eviction thread.
    mMap.compute(key, (k, v) -> {
      if (v == null) {
        v = new Entry(key, null);
      } else {
        v.mValue = null;
      }
      v.mAccessed = false;
      v.mDirty = true;
      return v;
    });
  }

  public void clear() {
    mMap.clear();
  }

  private void blockIfCacheFull() {
    while (mMap.size() >= mMaxSize) {
      LOG.info("map size: {}, max size: {}, high water: {}", mMap.size(), mMaxSize, mHighWaterMark);
      synchronized (mEvictionThread) {
        mEvictionThread.notify();
      }
      CommonUtils.sleepMs(100);
//      synchronized (mCacheFullMonitor) {
//        mCacheFullMonitor.wait();
//      }
    }
  }

  private void checkCacheSize() {
    if (mMap.size() >= mHighWaterMark && mEvictionThread.mIsSleeping) {
      synchronized (mEvictionThread) {
        mEvictionThread.notify();
      }
    }
  }

  @Nullable
  private Entry loadEntry(K key) {
    Optional<V> value = load(key);
    if (value.isPresent()) {
      return new Entry(key, value.get());
    }
    return null;
  }

  private class EvictionThread extends Thread {
    private final TemporalAmount mWarnInterval = Duration.ofSeconds(30);

    public volatile boolean mIsSleeping = false;

    private Iterator<Entry> mEvictionHead = Collections.emptyIterator();
    private Instant mNextAllowedSizeWarning = Instant.EPOCH;

    private EvictionThread() {
      super("eviction-thread");
    }

    @Override
    public void run() {
      while (true) {
        while (mMap.size() <= mLowWaterMark) {
          synchronized (mEvictionThread) { // Same as synchronized (this)
            if (mMap.size() <= mLowWaterMark) {
              try {
                mEvictionThread.mIsSleeping = true;
                mEvictionThread.wait();
                mEvictionThread.mIsSleeping = false;
              } catch (InterruptedException e) {
                return;
              }
            }
          }
        }
        // TODO(andrew): Implement batch eviction
        evictEntry();
        if (mMap.size() >= mMaxSize) {
          Instant now = Instant.now();
          if (now.isAfter(mNextAllowedSizeWarning)) {
            LOG.warn(
                "Cache is full. Consider increasing the cache size or lowering the high "
                    + "water mark. size:{} maxSize:{} highWaterMark:{} lowWaterMark:{}",
                mMap.size(), mMaxSize, mHighWaterMark, mLowWaterMark);
            mNextAllowedSizeWarning = now.plus(mWarnInterval);
          }
        }
//        if (mMap.size() > mHighWaterMark) {
//          synchronized (mCacheFullMonitor) {
//            mCacheFullMonitor.notifyAll();
//          }
//        }
      }
    }

    private void evictEntry() {
      boolean evicted = false;
      while (!evicted) {
        if (!mEvictionHead.hasNext()) {
          mEvictionHead = mMap.values().iterator();
        }
        Entry candidate = mEvictionHead.next();
        if (candidate == null) {
          return; // cache is empty
        }
        evicted = tryEvictEntry(candidate);
      }
    }

    private boolean tryEvictEntry(Entry candidate) {
      if (candidate.mAccessed) {
        candidate.mAccessed = false;
        return false;
      }
      if (candidate.mDirty) {
        Optional<LockResource> lockOpt = tryLock(candidate.mKey);
        if (!lockOpt.isPresent()) {
          return false;
        }
        try (LockResource lr = lockOpt.get()) {
          if (candidate.mValue == null) {
            removeFromBackingStore(candidate.mKey);
          } else {
            evictToBackingStore(candidate.mKey, candidate.mValue);
          }
          candidate.mDirty = false;
        }
      }
      return null == mMap.computeIfPresent(candidate.mKey, (key, value) -> {
        if (value.mDirty) {
          return value; // Inode must have been written since we evicted.
        }
        return null;
      });
    }
  }

  private class Entry {
    private K mKey;
    private V mValue;
    private volatile boolean mAccessed = true;
    private volatile boolean mDirty = true;

    private Entry(K key, V value) {
      mKey = key;
      mValue = value;
    }
  }
}
