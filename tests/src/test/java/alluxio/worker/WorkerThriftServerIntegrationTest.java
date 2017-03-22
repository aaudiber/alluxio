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

package alluxio.worker;

import alluxio.AlluxioURI;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.block.options.LockBlockOptions;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

/**
 *
 */
public final class WorkerThriftServerIntegrationTest {

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
      .setProperty(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCK_READERS, 1)
      .setProperty(PropertyKey.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS, 50)
      .setProperty(PropertyKey.WORKER_BLOCK_THREADS_MIN, 20)
      .setProperty(PropertyKey.WORKER_BLOCK_THREADS_MAX, 20)
      .build();

  @Test
  public void threadTimeout() throws Exception {
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient();
    FileSystemTestUtils.createByteFile(fs, new AlluxioURI("/file"), CreateFileOptions.defaults(), 1);
    URIStatus status = fs.getStatus(new AlluxioURI("/file"));
    Long blockId = status.getBlockIds().get(0);
    BlockWorkerClient client1 = FileSystemContext.INSTANCE.createBlockWorkerClient(mLocalAlluxioClusterResource.get().getWorkerAddress());
    BlockWorkerClient client2 = FileSystemContext.INSTANCE.createBlockWorkerClient(mLocalAlluxioClusterResource.get().getWorkerAddress());
    client1.lockBlock(blockId, LockBlockOptions.defaults());
    for (int i = 0; i < 20; i++) {
      try {
        System.out.println(i);
        client2.lockBlock(blockId, LockBlockOptions.defaults());
      } catch (IOException e) {
        // This is expected to time out.
      }
    }
    // Now make sure the worker can still serve requests
    client1.unlockBlock(blockId);
  }
}
