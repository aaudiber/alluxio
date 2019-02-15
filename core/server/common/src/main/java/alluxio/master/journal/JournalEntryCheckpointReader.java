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
import java.io.InputStream;
import java.util.Optional;

/**
 * A checkpoint writer which writes journal entries.
 */
public class JournalEntryCheckpointReader implements CheckpointWriter {
  private final JournalInputStreamImpl mEntryStream;

  /**
   * Constructs a new checkpoint reader.
   *
   * @param input the input stream for reading checkpoint data
   */
  public JournalEntryCheckpointReader(InputStream input) {
    mEntryStream = new JournalInputStreamImpl(input);
  }

  /**
   * Reads the next journal entry.
   *
   * @return the next journal entry, or empty if there are no more journal entries left to read
   */
  public Optional<JournalEntry> read() throws IOException {
    return Optional.ofNullable(mEntryStream.read());
  }

  @Override
  public void close() throws IOException {
    mEntryStream.close();
  }
}