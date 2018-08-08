package alluxio.master.file.meta;

import alluxio.exception.BlockInfoException;

import java.util.List;

public interface InodeFileView extends InodeView {

  /**
   * @return a duplication of all the block ids of the file
   */
  List<Long> getBlockIds();

  /**
   * @return the block size in bytes
   */
  long getBlockSizeBytes();

  /**
   * @return the length of the file in bytes. This is not accurate before the file is closed
   */
  long getLength();

  /**
   * @return the id of a new block of the file
   */
  long getNextBlockId();

  /**
   * @return the block container ID for this inode file
   */
  long getBlockContainerId();

  /**
   * Gets the block id for a given index.
   *
   * @param blockIndex the index to get the block id for
   * @return the block id for the index
   * @throws BlockInfoException if the index of the block is out of range
   */
  long getBlockIdByIndex(int blockIndex) throws BlockInfoException;

  /**
   * @return true if the file is cacheable, false otherwise
   */
  boolean isCacheable();

  /**
   * @return true if the file is complete, false otherwise
   */
  boolean isCompleted();
}
