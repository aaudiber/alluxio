package alluxio.master.file.state;

import alluxio.collections.FieldIndex;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeView;

public class InodesView {
  /** Use UniqueFieldIndex directly for ID index rather than using IndexedSet. */
  private final FieldIndex<Inode<?>> mInodes;

  public InodesView(FieldIndex<Inode<?>> inodes) {
    mInodes = inodes;
  }

  /**
   * Returns whether an inode exists with the specified ID.
   *
   * @param id the id
   * @return true if there is one such inode, otherwise false
   */
  public boolean containsId(long id) {
    return mInodes.containsField(id);
  }

  /**
   * Gets a view of the inode with the specified ID.
   *
   * @param id the id
   * @return the inode or null if there is no such inode
   */
  public InodeView getById(long id) {
    return mInodes.getFirst(id);
  }

  /**
   * @return the number of objects in this index set
   */
  public int size() {
    return mInodes.size();
  }

}
