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

package alluxio.master.journal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Base class for Alluxio classes with journaled state.
 */
public interface Checkpointed {
  /**
   * @return a name for this checkpointed class. The name is used in checkpoints, so it must not
   *         change
   */
  String getName();

  /**
   * Writes a checkpoint of all state to the given output stream
   *
   * @param output the output stream to write to
   */
  void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException;

  /**
   * Restores state from a checkpoint.
   *
   * @param input an input stream with checkpoint data
   */
  void restoreFromCheckpoint(InputStream input) throws IOException;
}
