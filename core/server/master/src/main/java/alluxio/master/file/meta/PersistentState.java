package alluxio.master.file.meta;

import alluxio.proto.journal.Journal.JournalEntry;

public interface PersistentState {
  void apply(JournalEntry entry);
  void reset();
}
