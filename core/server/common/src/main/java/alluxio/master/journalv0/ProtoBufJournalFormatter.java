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

package alluxio.master.journalv0;

import alluxio.master.journal.JournalInputStreamImpl;
import alluxio.proto.journal.Journal.JournalEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Reads and writes protocol buffer journal entries. The entries contain headers describing their
 * length. This framing is handled entirely by {@link JournalEntry#writeDelimitedTo(OutputStream)}
 * and {@link JournalEntry#parseDelimitedFrom(InputStream)}. This class is thread-safe.
 */
@ThreadSafe
public final class ProtoBufJournalFormatter implements JournalFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(ProtoBufJournalFormatter.class);

  /**
   * Constructs a new {@link ProtoBufJournalFormatter}.
   */
  public ProtoBufJournalFormatter() {}

  @Override
  public void serialize(JournalEntry entry, OutputStream outputStream) throws IOException {
    entry.writeDelimitedTo(outputStream);
  }

  @Override
  public JournalInputStream deserialize(final InputStream inputStream) throws IOException {
    return new JournalInputStreamImpl(inputStream);
  }
}
