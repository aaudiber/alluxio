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

package alluxio.master.metastore;

import alluxio.master.metastore.BlockStore.BlockMeta;

import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The block store keeps track of block sizes and block locations.
 *
 * BlockStores require external synchronization for operations on the same block id. Operations on
 * different block ids can be performed concurrently.
 */
@NotThreadSafe
public interface BlockStore extends Iterable<BlockMeta> {
  /**
   * @param id a block id
   * @return the block's metadata, or none if the block does not exist
   */
  Optional<BlockMeta> getBlock(long id);

  /**
   * Adds block metadata to the block store.
   *
   * @param meta the block metadata
   */
  void putBlock(BlockMeta meta);

  /**
   * @param id a block id to remove
   */
  void removeBlock(long id);

  /**
   * Removes all metadata from the block store.
   */
  void clear();

  /**
   * @param id a block id
   * @return the locations of the block
   */
  List<BlockLocationMeta> getLocations(long id);

  /**
   * Adds a new block location.
   *
   * @param id a block id
   * @param location a block location
   */
  void addLocation(long id, BlockLocationMeta location);

  /**
   * Removes a block location.
   *
   * @param blockId a block id
   * @param workerId a worker id
   */
  void removeLocation(long blockId, long workerId);

  /**
   * Immutable block metadata. This is only metadata about the block itself, and doesn't include
   * metadata relative to the cluster state, such as which workers hold the block and whether the
   * block is lost.
   */
  class BlockMeta {
    private final long mId;
    private final long mLength;

    public BlockMeta(long id, long length) {
      mId = id;
      mLength = length;
    }

    public long getId() {
      return mId;
    }

    public long getLength() {
      return mLength;
    }
  }

  /**
   * Immutable location metadata describing a block's location in a certain storage tier of a
   * certain worker.
   */
  class BlockLocationMeta {
    private final long mWorkerId;
    private final String mTierAlias;

    /**
     * @param workerId a worker id
     * @param tierAlias a tier alias
     */
    public BlockLocationMeta(long workerId, String tierAlias) {
      mWorkerId = workerId;
      mTierAlias = tierAlias;
    }

    /**
     * @return the worker id
     */
    public long getWorkerId() {
      return mWorkerId;
    }

    /**
     * @return the tier alias
     */
    public String getTierAlias() {
      return mTierAlias;
    }
  }
}
