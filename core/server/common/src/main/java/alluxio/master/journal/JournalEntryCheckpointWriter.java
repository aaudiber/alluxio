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

import alluxio.proto.journal.Journal.JournalEntry;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A checkpoint writer which writes journal entries.
 */
public class JournalEntryCheckpointWriter implements CheckpointWriter {
  private final long mNextSequenceNumber;
  private final OutputStream mOutput;

  /**
   * Constructs a new checkpoint writer.
   *
   * @param output the output stream for writing checkpoint data
   */
  public JournalEntryCheckpointWriter(OutputStream output) {
    mNextSequenceNumber = 0;
    mOutput = output;
  }

  /**
   * Writes a journal entry to the checkpoint
   *
   * @param entry the entry to write
   */
  public void write(JournalEntry entry) throws IOException {
    entry.toBuilder().setSequenceNumber(mNextSequenceNumber).build().writeDelimitedTo(mOutput);
  }

  @Override
  public void close() throws IOException {
    mOutput.close();
  }
}
