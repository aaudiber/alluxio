package alluxio.master.file.meta;

import alluxio.collections.ConcurrentHashSet;
import alluxio.collections.FieldIndex;
import alluxio.collections.IndexDefinition;
import alluxio.collections.UniqueFieldIndex;
import alluxio.master.file.state.InodesView;
import alluxio.master.journal.JournalContext;
import alluxio.proto.journal.File.AsyncPersistRequestEntry;
import alluxio.proto.journal.File.CompleteFileEntry;
import alluxio.proto.journal.File.DeleteFileEntry;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.File.InodeLastModificationTimeEntry;
import alluxio.proto.journal.File.PersistDirectoryEntry;
import alluxio.proto.journal.File.ReinitializeFileEntry;
import alluxio.proto.journal.File.RenameEntry;
import alluxio.proto.journal.File.SetAclEntry;
import alluxio.proto.journal.File.UpdateInodeDirectoryEntry;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.File.UpdateInodeFileEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.util.StreamUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class InodeTreePersistentState {
  private static final Logger LOG = LoggerFactory.getLogger(InodeTreePersistentState.class);

  private static final IndexDefinition<Inode<?>> ID_INDEX = new IndexDefinition<Inode<?>>(true) {
    @Override
    public Object getFieldValue(Inode<?> o) {
      return o.getId();
    }
  };

  /// Persistent State

  private final FieldIndex<Inode<?>> mInodes = new UniqueFieldIndex<>(ID_INDEX);

  /// State derived from persistent state

  /** Unmodifiable view of mInodes. */
  private final InodesView mInodesView = new InodesView(mInodes);
  /** The root of the entire file system. */
  private InodeDirectory mRoot = null;
  private final TtlBucketList mTtlBuckets = new TtlBucketList();
  /** A set of inode ids representing pinned inode files. */
  private final Set<Long> mPinnedInodeFileIds = new ConcurrentHashSet<>(64, 0.90f, 64);

  /**
   * @return the inodes of the inode tree, indexed by id
   */
  public InodesView getInodesView() {
    return mInodesView;
  }

  public InodeDirectoryView getRoot() {
    return mRoot;
  }

  /**
   * @return the TTL bucket list
   */
  public TtlBucketList getTtlBucketList() {
    // TODO: replace with unmodifiable view
    return mTtlBuckets;
  }

  /**
   * @return the pinned inode file ids;
   */
  public Set<Long> getPinnedInodeFileIds() {
    // TODO: replace with unmodifiable view
    return mPinnedInodeFileIds;
  }

  /**
   * Applies a journal entry to the inode tree state. This method should only be used during journal
   * replay. Otherwise, use one of the applyAndJournal methods.
   *
   * @param entry the entry
   */
  public void apply(JournalEntry entry) {
    LOG.info("{}\n", entry);
    if (entry.hasDeleteFile()) apply(entry.getDeleteFile());
    if (entry.hasInodeDirectory()) apply(entry.getInodeDirectory());
    if (entry.hasInodeFile()) apply(entry.getInodeFile());
    if (entry.hasRename()) apply(entry.getRename());
    if (entry.hasSetAcl()) apply(entry.getSetAcl());
    if (entry.hasUpdateInode()) apply(entry.getUpdateInode());
    if (entry.hasUpdateInodeDirectory()) apply(entry.getUpdateInodeDirectory());
    if (entry.hasUpdateInodeFile()) apply(entry.getUpdateInodeFile());

    // Deprecated entries
    if (entry.hasAsyncPersistRequest()) apply(entry.getAsyncPersistRequest());
    if (entry.hasCompleteFile()) apply(entry.getCompleteFile());
    if (entry.hasInodeLastModificationTime()) apply(entry.getInodeLastModificationTime());
    if (entry.hasPersistDirectory()) apply(entry.getPersistDirectory());
    if (entry.hasReinitializeFile()) apply(entry.getReinitializeFile());
  }

  public void applyAndJournal(Supplier<JournalContext> context, DeleteFileEntry entry) {
    apply(entry);
    context.get().append(JournalEntry.newBuilder().setDeleteFile(entry).build());
  }

  /**
   * @return whether the inode was successfully renamed. Returns false if another inode was
   *         concurrently added with the same name. On false return, no state is changed,
   *         and no journal entry is written
   */
  public boolean applyAndJournal(Supplier<JournalContext> context, RenameEntry entry) {
    if (applyRename(entry)) {
      context.get().append(JournalEntry.newBuilder().setRename(entry).build());
      return true;
    }
    return false;
  }

  public void applyAndJournal(Supplier<JournalContext> context, SetAclEntry entry) {
    apply(entry);
    context.get().append(JournalEntry.newBuilder().setSetAcl(entry).build());
  }

  public void applyAndJournal(Supplier<JournalContext> context, UpdateInodeEntry entry) {
    apply(entry);
    context.get().append(JournalEntry.newBuilder().setUpdateInode(entry).build());
  }

  public void applyAndJournal(Supplier<JournalContext> context, UpdateInodeDirectoryEntry entry) {
    apply(entry);
    context.get().append(JournalEntry.newBuilder().setUpdateInodeDirectory(entry).build());
  }

  public void applyAndJournal(Supplier<JournalContext> context, UpdateInodeFileEntry entry) {
    apply(entry);
    context.get().append(JournalEntry.newBuilder().setUpdateInodeFile(entry).build());
  }

  /**
   * @return whether the inode was successfully added. Returns false if another inode was
   *         concurrently added with the same name. On false return, no state is changed,
   *         and no journal entry is written
   */
  public boolean applyAndJournal(Supplier<JournalContext> context, Inode<?> inode) {
    if (applyInode(inode)) {
      context.get().append(inode.toJournalEntry());
      return true;
    }
    return false;
  }

  ////
  /// Apply Implementations
  ////

  private void apply(DeleteFileEntry entry) {
    long id = entry.getId();
    Inode<?> inode = mInodes.getFirst(id);
    InodeDirectory parent = (InodeDirectory) mInodes.getFirst(inode.getParentId());
    LOG.info("Deleted {}", id);
    mInodes.remove(inode);
    parent.removeChild(inode);
    parent.setLastModificationTimeMs(entry.getOpTimeMs());
    inode.setDeleted(true);
    mPinnedInodeFileIds.remove(id);
  }

  private void apply(InodeDirectoryEntry entry) {
    Preconditions.checkState(applyInode(InodeDirectory.fromJournalEntry(entry)));
  }

  private void apply(InodeFileEntry entry) {
    Preconditions.checkState(applyInode(InodeFile.fromJournalEntry(entry)));
  }

  /**
   * @return whether the inode was successfully added. Returns false if another inode was
   *         concurrently added with the same name. On false return, no state is changed, and no
   *         journal entry is written
   **/
  private boolean applyInode(Inode<?> inode) {
    if (inode.isDirectory() && inode.getName().equals(InodeTree.ROOT_INODE_NAME)) {
      // This is the root inode. Clear all the state, and set the root.
      mInodes.clear();
      mInodes.add(inode);
      mPinnedInodeFileIds.clear();
      mRoot = (InodeDirectory) inode;
      return true;
    }
    mInodes.add(inode);
    InodeDirectory parent = (InodeDirectory) mInodes.getFirst(inode.getParentId());
    if (!parent.addChild(inode)) {
      LOG.info("Failed to add {}", inode.getId());
      mInodes.remove(inode);
      return false;
    }
    // Update indexes.
    if (inode.isFile() && inode.isPinned()) {
      mPinnedInodeFileIds.add(inode.getId());
    }
    // Add the file to TTL buckets, the insert automatically rejects files w/ Constants.NO_TTL
    mTtlBuckets.insert(inode);
    return true;
  }

  private void apply(RenameEntry entry) {
    Preconditions.checkState(applyRename(entry));
  }

  private boolean applyRename(RenameEntry entry) {
    Inode<?> inode = mInodes.getFirst(entry.getId());
    String oldName = inode.getName();
    InodeDirectory parent = (InodeDirectory) mInodes.getFirst(inode.getParentId());
    parent.removeChild(inode);

    inode.setName(entry.getNewName());
    InodeDirectory newParent = (InodeDirectory) mInodes.getFirst(entry.getNewParentId());
    if (!newParent.addChild(inode)) {
      // Parents index their children by name, so we need to update the name before adding/removing.
      // In the future, we should consider indexing by ID instead to simplify this code and also
      // save memory.
      inode.setName(oldName);
      parent.addChild(inode);
      return false;
    }
    inode.setParentId(entry.getNewParentId());
    parent.setLastModificationTimeMs(entry.getOpTimeMs());
    newParent.setLastModificationTimeMs(entry.getOpTimeMs());
    return true;
  }

  private void apply(SetAclEntry entry) {
    Inode<?> inode = mInodes.getFirst(entry.getId());
    List<AclEntry> entries = StreamUtils.map(AclEntry::fromProto, entry.getEntriesList());
    switch (entry.getAction()) {
      case REPLACE:
        // fully replace the acl for the path
        inode.replaceAcl(entries);
        break;
      case MODIFY:
        inode.setAcl(entries);
        break;
      case REMOVE:
        try {
          inode.removeAcl(entries);
        } catch (IOException e) {
          // TODO: check that removeAcl is legal before applying the journal entry.
          throw new RuntimeException(e);
        }
        break;
      case REMOVE_ALL:
        inode.removeExtendedAcl();
        break;
      case REMOVE_DEFAULT:
        inode.setDefaultACL(new DefaultAccessControlList());
        break;
      default:
    }
  }

  private void apply(UpdateInodeEntry entry) {
    Inode<?> inode = mInodes.getFirst(entry.getId());
    inode.updateFromEntry(entry);
    if (entry.hasTtl()) {
      mTtlBuckets.remove(inode);
      mTtlBuckets.insert(inode);
    }
  }

  private void apply(UpdateInodeDirectoryEntry entry) {
    Inode<?> inode = mInodes.getFirst(entry.getId());
    Preconditions.checkState(inode.isDirectory(),
        "Encountered non-directory id in update directory entry %s", entry);
    InodeDirectory dir = (InodeDirectory) inode;

    dir.updateFromEntry(entry);
  }

  private void apply(UpdateInodeFileEntry entry) {
    Inode<?> inode = mInodes.getFirst(entry.getId());
    Preconditions.checkState(inode.isFile(),
        "Encountered non-file id in update file entry %s", entry);
    InodeFile file = (InodeFile) inode;

    file.updateFromEntry(entry);
  }

  ////
  /// Deprecated Entries
  ////

  private void apply(AsyncPersistRequestEntry entry) {
    apply(JournalEntry.newBuilder()
        .setUpdateInode(UpdateInodeEntry.newBuilder()
            .setId(entry.getFileId())
            .setPersistenceState(PersistenceState.TO_BE_PERSISTED.name())
        ).build());
  }

  private void apply(CompleteFileEntry entry) {
    apply(JournalEntry.newBuilder()
        .setUpdateInode(UpdateInodeEntry.newBuilder()
            .setId(entry.getId())
            .setLastModificationTimeMs(entry.getOpTimeMs())
            .setUfsFingerprint(entry.getUfsFingerprint())
        ).build());
    apply(JournalEntry.newBuilder()
        .setUpdateInodeFile(UpdateInodeFileEntry.newBuilder()
            .setId(entry.getId())
            .setLength(entry.getLength())
            .addAllBlocks(entry.getBlockIdsList())
        ).build());
  }

  private void apply(InodeLastModificationTimeEntry entry) {
    // This entry is deprecated, use UpdateInode instead.
    apply(JournalEntry.newBuilder()
        .setUpdateInode(UpdateInodeEntry.newBuilder()
            .setId(entry.getId())
            .setLastModificationTimeMs(entry.getLastModificationTimeMs())
        ).build());
  }

  private void apply(PersistDirectoryEntry entry) {
    // This entry is deprecated, use UpdateInode instead.
    apply(JournalEntry.newBuilder()
        .setUpdateInode(UpdateInodeEntry.newBuilder()
            .setId(entry.getId())
            .setPersistenceState(PersistenceState.PERSISTED.name())
        ).build());
  }

  private void apply(ReinitializeFileEntry entry) {
    throw new UnsupportedOperationException("Lineage is not currently supported");
  }

  public void reset() {
    mRoot = null;
    mInodes.clear();
    mPinnedInodeFileIds.clear();
  }
}
