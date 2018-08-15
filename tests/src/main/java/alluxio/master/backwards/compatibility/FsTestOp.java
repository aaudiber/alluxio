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

package alluxio.master.backwards.compatibility;

import alluxio.client.file.FileSystem;

public abstract class FsTestOp implements TestOp {
  @Override
  public void apply(Clients clients) throws Exception {
    apply(clients.getFs());
  }

  protected abstract void apply(FileSystem fs) throws Exception;

  @Override
  public void check(Clients clients) throws Exception {
    check(clients.getFs());
  }

  protected abstract void check(FileSystem fs) throws Exception;
}
