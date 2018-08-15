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

package alluxio.master.backcompat;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.ProjectConstants;
import alluxio.master.backcompat.ops.AsyncPersist;
import alluxio.master.backcompat.ops.CreateDirectory;
import alluxio.master.backcompat.ops.CreateFile;
import alluxio.master.backcompat.ops.DeleteFile;
import alluxio.master.backcompat.ops.Mount;
import alluxio.master.backcompat.ops.PersistDirectory;
import alluxio.master.backcompat.ops.PersistFile;
import alluxio.master.backcompat.ops.RenameFile;
import alluxio.master.backcompat.ops.SetAcl;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.security.LoginUser;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.List;

/**
 * Generates journals for consumption by the BackwardsCompatibilityIntegrationTest.
 *
 * This class
 * 1. starts a new cluster
 * 2. runs all the test operations listed in OPS
 * 3. takes a journal backup at stores it to src/test/resources/old_journals
 * 4. stops the cluster
 * 5. copies the journal to src/test/resources/old_journals
 *
 * Later, BackwardsCompatibilityIntegrationTest will start clusters from the backups and copies
 * in old_journals, and validate that the operations' checks pass.
 */
public final class BackwardsCompatibilityJournalGenerator {
  // Path relative to tests/src/test
  public static final String OLD_JOURNALS_RESOURCE = "src/test/resources/old_journals";
  // Path is relative to alluxio home directory
  private static final String OLD_JOURNALS = PathUtils.concatPath("tests", OLD_JOURNALS_RESOURCE);

  public static final List<TestOp> OPS = ImmutableList.<TestOp>builder()
      .add(new CreateDirectory(),
          new CreateFile(),
          new Mount(),
          new AsyncPersist(),
          new DeleteFile(),
          new PersistFile(),
          new PersistDirectory(),
          new RenameFile(),
          new SetAcl()
      ).build();

  /**
   * Generates journal files to be used by the backwards compatibility test. The files are named
   * based on the current version defined in ProjectConstants.VERSION. Run this with each release,
   * and commit the created journal and snapshot into the git repository.
   *
   * @param args no args expected
   */
  public static void main(String[] args) throws Exception {
    if (!LoginUser.get().getName().equals("root")) {
      System.err
          .printf("Journals must be generated as root so that they can be replayed by root\n");
      System.exit(-1);
    }
    File journalDst = new File(OLD_JOURNALS,
        String.format("journal-%s", ProjectConstants.VERSION));
    if (journalDst.exists()) {
      System.err.printf("%s already exists, delete it first\n", journalDst.getAbsolutePath());
      System.exit(-1);
    }
    File backupDst = new File(OLD_JOURNALS,
        String.format("backup-%s", ProjectConstants.VERSION));
    if (backupDst.exists()) {
      System.err.printf("%s already exists, delete it first\n", backupDst.getAbsolutePath());
      System.exit(-1);
    }
    MultiProcessCluster cluster =
        MultiProcessCluster.newBuilder(PortCoordination.BACKWARDS_COMPATIBILITY)
            .setClusterName("BackwardsCompatibility")
            .setNumMasters(1)
            .setNumWorkers(1)
            .build();
    try {
      cluster.start();
      cluster.notifySuccess();
      cluster.waitForAllNodesRegistered(10 * Constants.SECOND_MS);
      for (TestOp op : OPS) {
        op.apply(cluster.getClients());
      }
      AlluxioURI backup = cluster.getMetaMasterClient()
          .backup(new File(OLD_JOURNALS).getAbsolutePath(), true)
          .getBackupUri();
      FileUtils.moveFile(new File(backup.getPath()), backupDst);
      cluster.stopMasters();
      FileUtils.copyDirectory(new File(cluster.getJournalDir()), journalDst);
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      cluster.destroy();
    }
    System.out.printf("Artifacts successfully generated at %s and %s\n",
        journalDst.getAbsolutePath(), backupDst.getAbsolutePath());
  }
}
