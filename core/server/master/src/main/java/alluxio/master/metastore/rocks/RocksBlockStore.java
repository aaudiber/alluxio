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

package alluxio.master.metastore.rocks;

import alluxio.master.metastore.BlockStore;
import alluxio.util.CommonUtils;
import alluxio.util.io.FileUtils;

import com.google.common.primitives.Longs;
import org.apache.commons.io.Charsets;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Block store backed by RocksDB.
 */
public class RocksBlockStore implements BlockStore {
  private static final String BASE_DB_PATH = "/tmp/rocks";

  private String mDbPath;
  private RocksDB mDb;
  private ColumnFamilyHandle mDefaultColumn;
  private ColumnFamilyHandle mBlockMetaColumn;
  private ColumnFamilyHandle mBlockLocationsColumn;

  public RocksBlockStore() throws RocksDBException {
    RocksDB.loadLibrary();
    initDb();
  }

  @Override
  public Optional<BlockMeta> getBlock(long id) {
    byte[] length;
    try {
      length = mDb.get(mBlockMetaColumn, Longs.toByteArray(id));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    if (length == null) {
      return Optional.empty();
    }
    return Optional.of(new BlockMeta(id, Longs.fromByteArray(length)));
  }

  @Override
  public void putBlock(BlockMeta meta) {
    try {
      mDb.put(mBlockMetaColumn, Longs.toByteArray(meta.getId()),
          Longs.toByteArray(meta.getLength()));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeBlock(long id) {
    try {
      mDb.delete(mBlockMetaColumn, Longs.toByteArray(id));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    try {
      initDb();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<BlockLocationMeta> getLocations(long id) {
    RocksIterator iter =
        mDb.newIterator(mBlockLocationsColumn, new ReadOptions().setPrefixSameAsStart(true));
    iter.seek(Longs.toByteArray(id));
    List<BlockLocationMeta> locations = new ArrayList<>();
    for (; iter.isValid(); iter.next()) {
      byte[] key = iter.key();
      long workerId =
          Longs.fromBytes(key[8], key[9], key[10], key[11], key[12], key[13], key[14], key[15]);
      String tier = new String(iter.value(), Charsets.UTF_8);
      locations.add(new BlockLocationMeta(workerId, tier));
    }
    return locations;
  }

  @Override
  public void addLocation(long id, BlockLocationMeta location) {
    byte[] key = toByteArray(id, location.getWorkerId());
    try {
      mDb.put(mBlockLocationsColumn, key, location.getTierAlias().getBytes(Charsets.UTF_8));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeLocation(long blockId, long workerId) {
    byte[] key = toByteArray(blockId, workerId);
    try {
      mDb.delete(mBlockLocationsColumn, key);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<BlockMeta> iterator() {
    RocksIterator iter =
        mDb.newIterator(mBlockMetaColumn, new ReadOptions().setPrefixSameAsStart(true));
    iter.seekToFirst();
    return new Iterator<BlockMeta>() {
      @Override
      public boolean hasNext() {
        return iter.isValid();
      }

      @Override
      public BlockMeta next() {
        try {
          return new BlockMeta(Longs.fromByteArray(iter.key()), Longs.fromByteArray(iter.value()));
        } finally {
          iter.next();
        }
      }
    };
  }

  private byte[] toByteArray(long long1, long long2) {
    byte[] key = new byte[16];
    for (int i = 7; i >= 0; i--) {
      key[i] = (byte) (long1 & 0xffL);
      long1 >>= 8;
    }
    for (int i = 15; i >= 8; i--) {
      key[i] = (byte) (long2 & 0xffL);
      long2 >>= 8;
    }
    return key;
  }

  private void initDb() throws RocksDBException {
    if (mDb != null) {
      mDefaultColumn.close();
      mBlockMetaColumn.close();
      mBlockLocationsColumn.close();
      mDb.close();
      try {
        FileUtils.deletePathRecursively(mDbPath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    new File(BASE_DB_PATH).mkdirs();

    ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()
        .useFixedLengthPrefixExtractor(8);

    List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
        new ColumnFamilyDescriptor("block-meta".getBytes(), cfOpts),
        new ColumnFamilyDescriptor("block-locations".getBytes(), cfOpts)
    );

    DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);

    // a list which will hold the handles for the column families once the db is opened
    List<ColumnFamilyHandle> columns = new ArrayList<>();
    mDbPath = newDbPath();
    mDb = RocksDB.open(options, mDbPath, cfDescriptors, columns);
    mDefaultColumn = columns.get(0);
    mBlockMetaColumn = columns.get(1);
    mBlockLocationsColumn = columns.get(2);
  }

  private static String newDbPath() {
    return BASE_DB_PATH + "/db-" + System.nanoTime() + "-" + CommonUtils.randomAlphaNumString(3);
  }
}
