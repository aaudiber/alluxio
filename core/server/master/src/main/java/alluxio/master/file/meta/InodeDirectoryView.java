package alluxio.master.file.meta;

import alluxio.exception.InvalidPathException;

import java.util.Set;

import javax.annotation.Nullable;

public interface InodeDirectoryView extends InodeView {

  /**
   * @param name the name of the child
   * @return the inode with the given name, or null if there is no child with that name
   */
  InodeView getChild(String name);

  /**
   * @param name the name of the child
   * @param lockList the lock list to add the lock to
   * @return the read-locked inode with the given name, or null if there is no such child
   * @throws InvalidPathException if the path to the child is invalid
   */
  @Nullable
  InodeView getChildReadLock(String name, InodeLockList lockList) throws
      InvalidPathException;

  /**
   * @param name the name of the child
   * @param lockList the lock list to add the lock to
   * @return the write-locked inode with the given name, or null if there is no such child
   * @throws InvalidPathException if the path to the child is invalid
   */
  @Nullable
  InodeView getChildWriteLock(String name, InodeLockList lockList) throws
      InvalidPathException;

  /**
   * @return an unmodifiable set of the children inodes
   */
  Set<InodeView> getChildren();

  /**
   * @return the ids of the children
   */
  Set<Long> getChildrenIds();

  /**
   * @return the number of children in the directory
   */
  int getNumberOfChildren();

  /**
   * @return true if the inode is a mount point, false otherwise
   */
  boolean isMountPoint();

  /**
   * @return true if we have loaded all the direct children's metadata once
   */
  boolean isDirectChildrenLoaded();

  /**
   * Before calling this method, the caller should hold at least a READ LOCK on the inode.
   *
   * @return true if we have loaded all the direct and indirect children's metadata once
   */
  boolean areDescendantsLoaded();
}
