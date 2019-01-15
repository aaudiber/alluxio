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

package alluxio.master.metastore.rocks;

import alluxio.conf.InstancedConfiguration;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.metastore.BlockStore;
import alluxio.master.metastore.caching.CachingInodeStore;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.Metastore;

import org.rocksdb.RocksDBException;

/**
 * Metastore backed by RocksDB.
 */
public class RocksMetastore implements Metastore {
  private final InodeStore mInodeStore;
  private final BlockStore mBlockStore;

  /**
   * Constructs a new RocksMetastore.
   *
   * @param lockManager inode lock manager
   */
  public RocksMetastore(InodeLockManager lockManager, InstancedConfiguration conf) {
    try {
      mInodeStore = new CachingInodeStore(new RocksInodeStore(conf), lockManager, conf);
      mBlockStore = new RocksBlockStore(conf);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public InodeStore getInodeStore() {
    return mInodeStore;
  }

  @Override
  public BlockStore getBlockStore() {
    return mBlockStore;
  }
}