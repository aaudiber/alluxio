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

package alluxio.master.metastore.java;

import alluxio.master.metastore.BlockStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class requires external synchronization for operations on the same block id. Operations on
 * different block ids can be performed concurrently.
 */
@NotThreadSafe
public class HeapBlockStore implements BlockStore {
  public final Map<Long, BlockMeta> mBlocks = new ConcurrentHashMap<>();
  public final Map<Long, Map<Long, BlockLocationMeta>> mBlockLocations = new ConcurrentHashMap<>();
  public final Map<Long, Set<Long>> mWorkerBlocks = new ConcurrentHashMap<>();

  @Override
  public Optional<BlockMeta> getBlock(long id) {
    return Optional.ofNullable(mBlocks.get(id));
  }

  @Override
  public void putBlock(BlockMeta meta) {
    mBlocks.put(meta.getId(), meta);
  }

  @Override
  public void removeBlock(long id) {
    mBlocks.remove(id);
  }

  @Override
  public Iterator<BlockMeta> iterator() {
    return mBlocks.values().iterator();
  }

  @Override
  public void clear() {
    mBlocks.clear();
  }

  @Override
  public List<BlockLocationMeta> getLocations(long blockid) {
    return new ArrayList<>(mBlockLocations.get(blockid).values());
  }

  @Override
  public void addLocation(long blockId, BlockLocationMeta location) {
    mBlockLocations.computeIfAbsent(blockId, x -> new HashMap<>());
    mBlockLocations.get(blockId).put(location.getWorkerId(), location);
  }

  @Override
  public void removeLocation(long blockId, long workerId) {
    mBlockLocations.get(blockId).remove(workerId);
  }
}
