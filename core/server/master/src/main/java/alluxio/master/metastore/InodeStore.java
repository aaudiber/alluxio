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

package alluxio.master.metastore;

import alluxio.PropertyKey;
import alluxio.conf.InstancedConfiguration;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.metastore.java.HeapInodeStore;
import alluxio.master.metastore.rocks.RocksInodeStore;

import org.rocksdb.RocksDBException;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Inode metadata storage.
 *
 * The inode store manages metadata about individual inodes, as well as the parent-child
 * relationships between them.
 */
public interface InodeStore extends ReadOnlyInodeStore {
  /**
   * @param id an inode id
   * @return the inode with the given id, if it exists
   */
  Optional<MutableInode<?>> getMutable(long id);

  @Override
  default Optional<Inode> get(long id) {
    return getMutable(id).map(inode -> Inode.wrap(inode));
  }

  /**
   * Removes an inode from the inode store. The edge leading to it will also be removed.
   *
   * @param inode an inode to remove
   */
  void remove(InodeView inode);

  /**
   * Adds the given inode, or overwrites it if it exists.
   *
   * @param inode the inode to write
   */
  void writeInode(MutableInode<?> inode);

  /**
   * Removes all inodes and edges.
   */
  void clear();

  /**
   * Makes an inode the child of the specified parent. The added child must already exist in the
   * inode store.
   *
   * @param parentId the parent id
   * @param inode the child inode
   */
  void addChild(long parentId, InodeView inode);

  /**
   * Removes a child from a parent inode.
   *
   * @param parentId the parent inode id
   * @param name the child name
   */
  void removeChild(long parentId, String name);

  static void main(String[] args) throws RocksDBException {
    InstancedConfiguration diskConf = InstancedConfiguration.newBuilder()
        .build();
    InstancedConfiguration ramdiskConf = InstancedConfiguration.newBuilder()
        .setProperty(PropertyKey.MASTER_METASTORE_DIR, "/Volumes/ramdisk")
        .setProperty(PropertyKey.MASTER_METASTORE_ROCKS_IN_MEMORY, true)
        .build();
    for (InodeStore store : Arrays.asList(
        new HeapInodeStore(),
        new RocksInodeStore(diskConf),
        new RocksInodeStore(ramdiskConf)
    )) {
      int numInodes = 1_000_000;
      long s = System.currentTimeMillis();
      writeInodes(store, 0, numInodes);
      System.out.printf("Wrote %s inodes in %sms%n", numInodes, System.currentTimeMillis() - s);
      // Random read benchmark
      for (int iter = 0; iter < 5; iter++) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1_000_000; i++) {
          long id = ThreadLocalRandom.current().nextLong(numInodes);
          store.get(id);
        }
        System.out.printf("Completed 1 million random reads in %sms%n",
            System.currentTimeMillis() - start);
      }
    }
  }

  static void writeInodes(InodeStore store, int startId, int count) {
    for (int i = startId; i < startId + count; i++) {
      MutableInodeDirectory dir =
          MutableInodeDirectory.create(i, 0, "x", CreateDirectoryOptions.defaults());
      store.writeInode(dir);
    }
  }
}
