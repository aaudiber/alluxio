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
import java.io.OutputStream;
import java.util.function.Supplier;

/**
 * Base class for Alluxio classes with journaled state.
 */
public abstract class Journaled {
  private final String mName;

  /**
   * @param name a name for this journaled class. The name is used in checkpoints, so it must not
   *        change
   */
  public Journaled(String name) {
    mName = name;
  }

  /**
   * @return a name for this journaled class. The name is used in checkpoints, so it must not change
   */
  public final String getName() {
    return mName;
  }

  /**
   * Writes a checkpoint of all state to the given output stream
   *
   * @param output the output stream to write to
   */
  public abstract void toCheckpoint(OutputStream output) throws IOException;

  /**
   * Restores state from a checkpoint.
   *
   * @param input an input stream with checkpoint data
   */
  public abstract void restoreFromCheckpoint(InputStream input) throws IOException;

  /**
   * Applies and journals a journal entry.
   *
   * All journal appends should go through this method.
   *
   * @param context journal context
   * @param entry the entry to apply and journal
   */
  protected final void applyAndJournal(Supplier<JournalContext> context, JournalEntry entry) {
    apply(entry);
    context.get().append(entry);
  }

  /**
   * Updates state according to the given JournalEntry.
   *
   * @param entry the entry to apply
   */
  protected abstract void apply(JournalEntry entry);
}
