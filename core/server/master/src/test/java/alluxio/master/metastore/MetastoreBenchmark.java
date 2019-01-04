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

import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.metastore.java.HeapInodeStore;

import org.junit.Test;

public class MetastoreBenchmark {
  private InodeStore mInodeStore;
  private long mCount = 0;

  @Test
  public void run() throws Exception {
    mInodeStore = new HeapInodeStore();

    MutableInodeFile inode = MutableInodeFile.create(0, 0, "normal_name", 0, CreateFileOptions.defaults());
    mInodeStore.writeInode(inode);

    int warmup = 10_000;
    int iterations = 1_000_000;
    query(warmup, inode.getId());

    query(iterations, inode.getId());
//    queryTime(5_000, inode.getId());
    System.out.printf("Ran %s iterations in 5 seconds %n", mCount);
  }

  private void query(int n, long inodeId) {
    for (int i = 0; i < n; i++) {
      mInodeStore.getMutable(inodeId);
    }
  }

  private void queryTime(int time, long inodeId) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() < start + time) {
      for (int i = 0; i < 10_000; i++) {
        if (mInodeStore.getMutable(inodeId).get().isFile()) {
          mCount++;
        }
      }
    }
  }
}
