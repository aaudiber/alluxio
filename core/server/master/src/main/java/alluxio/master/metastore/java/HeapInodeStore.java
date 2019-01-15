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

import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.InodeView;
import alluxio.master.metastore.InodeStore;
import alluxio.util.StreamUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * FileStore implementation using on-heap data structures.
 */
public class HeapInodeStore implements InodeStore {
  private final Map<Long, MutableInode<?>> mInodes = new ConcurrentHashMap<>();
  // Map from inode id to children of that inode. The internal maps are ordered by child name.
  private final Map<Long, Map<String, MutableInode<?>>> mEdges = new ConcurrentHashMap<>();

  @Override
  public void remove(Long inodeId) {
    mInodes.remove(inodeId);
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    mInodes.putIfAbsent(inode.getId(), inode);
  }

  @Override
  public void addChild(long parentId, InodeView child) {
    mEdges.putIfAbsent(parentId, new ConcurrentSkipListMap<>());
    mEdges.get(parentId).put(child.getName(), mInodes.get(child.getId()));
  }

  @Override
  public void addChild(long parentId, String childName, Long childId) {
    addChild(parentId, get(childId).get());
  }

  @Override
  public void removeChild(long parentId, String name) {
    children(parentId).remove(name);
  }

  @Override
  public long estimateSize() {
    return mInodes.size();
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long id) {
    return Optional.ofNullable(mInodes.get(id));
  }

  @Override
  public Iterable<Long> getChildIds(Long inodeId) {
    return StreamUtils.map(MutableInode::getId, children(inodeId).values());
  }

  @Override
  public Iterable<? extends Inode> getChildren(Long inodeId) {
    return StreamUtils.map(Inode::wrap, children(inodeId).values());
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String child) {
    return Optional.ofNullable(children(inodeId).get(child)).map(MutableInode::getId);
  }

  @Override
  public Optional<Inode> getChild(Long inodeId, String child) {
    return Optional.ofNullable(children(inodeId).get(child)).map(Inode::wrap);
  }

  @Override
  public boolean hasChildren(InodeDirectoryView dir) {
    return !children(dir.getId()).isEmpty();
  }

  @Override
  public void clear() {
    mInodes.clear();
    mEdges.clear();
  }

  private Map<String, MutableInode<?>> children(long id) {
    return mEdges.getOrDefault(id, Collections.emptyMap());
  }
}
