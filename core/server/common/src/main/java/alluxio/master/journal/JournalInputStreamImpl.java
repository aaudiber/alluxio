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

import alluxio.master.journalv0.JournalInputStream;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.proto.ProtoUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Stream for reading journal entries.
 */
public class JournalInputStreamImpl implements JournalInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(JournalInputStreamImpl.class);

  private final byte[] mBuffer = new byte[1024];
  private final InputStream mInput;

  private long mLatestSequenceNumber;

  /**
   * @param input the raw stream to read bytes from
   */
  public JournalInputStreamImpl(InputStream input) {
    mInput = input;
  }

  @Override
  public JournalEntry read() throws IOException {
    int firstByte = mInput.read();
    if (firstByte == -1) {
      return null;
    }
    // All journal entries start with their size in bytes written as a varint.
    int size = ProtoUtils.readRawVarint32(firstByte, mInput);
    byte[] buffer = size <= mBuffer.length ? mBuffer : new byte[size];
    // Total bytes read so far for journal entry.
    int totalBytesRead = 0;
    while (totalBytesRead < size) {
      // Bytes read in last read request.
      int latestBytesRead = mInput.read(buffer, totalBytesRead, size - totalBytesRead);
      if (latestBytesRead < 0) {
        break;
      }
      totalBytesRead += latestBytesRead;
    }
    if (totalBytesRead < size) {
      LOG.warn("Journal entry was truncated. Expected to read " + size + " bytes but only got "
          + totalBytesRead);
      return null;
    }

    JournalEntry entry = JournalEntry.parseFrom(new ByteArrayInputStream(buffer, 0, size));
    if (entry != null) {
      mLatestSequenceNumber = entry.getSequenceNumber();
    }
    return entry;
  }

  @Override
  public void close() throws IOException {
    mInput.close();
  }

  @Override
  public long getLatestSequenceNumber() {
    return mLatestSequenceNumber;
  }
}
