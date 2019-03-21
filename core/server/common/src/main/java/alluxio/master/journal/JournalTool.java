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

package alluxio.master.journal;

import alluxio.AlluxioURI;
import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.NoopMaster;
import alluxio.master.journal.checkpoint.CompoundCheckpointReader;
import alluxio.master.journal.checkpoint.CompoundCheckpointReader.Entry;
import alluxio.master.journal.JournalReader.State;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.InodeProtosCheckpointReader;
import alluxio.master.journal.checkpoint.JournalCheckpointReader;
import alluxio.master.journal.checkpoint.LongsCheckpointReader;
import alluxio.master.journal.checkpoint.TarballCheckpointReader;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalReader;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.meta.InodeMeta;
import alluxio.util.io.PathUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Tool for converting a ufs journal to a human-readable format.
 *
 * <pre>
 * java -cp \
 *   assembly/server/target/alluxio-assembly-server-<ALLUXIO-VERSION>-jar-with-dependencies.jar \
 *   alluxio.master.journal.JournalTool -master FileSystemMaster -outputDir my-journal
 * </pre>
 */
@NotThreadSafe
public final class JournalTool {
  private static final Logger LOG = LoggerFactory.getLogger(JournalTool.class);
  /** Separator to place at the end of each journal entry. */
  private static final String ENTRY_SEPARATOR = StringUtils.repeat('-', 80);
  private static final int EXIT_FAILED = -1;
  private static final int EXIT_SUCCEEDED = 0;
  private static final Options OPTIONS = new Options()
      .addOption("help", false, "Show help for this command.")
      .addOption("master", true,
          "The name of the master (e.g. FileSystemMaster, BlockMaster). "
              + "Set to FileSystemMaster by default.")
      .addOption("outputDir", true,
          "The output directory to write journal content to. Default: journal_dump-${timestamp}");

  private static boolean sHelp;
  private static String sMaster;
  private static String sOutputDir;
  private static String sCheckpointsDir;
  private static String sJournalEntryFile;

  private JournalTool() {} // prevent instantiation

  /**
   * Dumps a ufs journal in human-readable format.
   *
   * @param args arguments passed to the tool
   */
  public static void main(String[] args) throws IOException {
    if (!parseInputArgs(args)) {
      usage();
      System.exit(EXIT_FAILED);
    }
    if (sHelp) {
      usage();
      System.exit(EXIT_SUCCEEDED);
    }

    dumpJournal();
  }

  private static void dumpJournal() throws IOException {
    Files.createDirectories(Paths.get(sOutputDir));
    UfsJournal journal =
        new UfsJournalSystem(getJournalLocation(), 0).createJournal(new NoopMaster(sMaster));
    PrintStream entryStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(sJournalEntryFile)));
    try (JournalReader reader = new UfsJournalReader(journal, 0, true)) {
      boolean done = false;
      while (!done) {
        State state = reader.advance();
        switch (state) {
          case CHECKPOINT:
            try (CheckpointInputStream checkpoint = reader.getCheckpoint()) {
              Path dir = Paths.get(sCheckpointsDir + "-" + reader.getNextSequenceNumber());
              Files.createDirectories(dir);
              readCheckpoint(checkpoint, dir);
            }
            break;
          case LOG:
            JournalEntry entry = reader.getEntry();
            entryStream.println(ENTRY_SEPARATOR);
            entryStream.print(entry);
            break;
          case DONE:
            done = true;
            break;
          default:
            throw new RuntimeException("Unknown state: " + state);
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to read next journal entry.", e);
    }
  }

  private static void readCheckpoint(CheckpointInputStream checkpoint, Path path)
      throws IOException {
    System.out.printf("Reading checkpoint of type %s to %s%n", checkpoint.getType().name(), path);
    switch (checkpoint.getType()) {
      case COMPOUND:
        readCompoundCheckpoint(checkpoint, path);
        break;
      case JOURNAL_ENTRY:
        readJournalCheckpoint(checkpoint, path);
        break;
      case ROCKS:
        readRocksCheckpoint(checkpoint, path);
        break;
      case LONGS:
        readLongsCheckpoint(checkpoint, path);
        break;
      case INODE_PROTOS:
        readProtosCheckpoint(checkpoint, path);
      default:
        System.out.println("Unknown checkpoint type: " + checkpoint.getType());
    }
  }

  private static void readCompoundCheckpoint(CheckpointInputStream checkpoint, Path path) throws IOException {
    Files.createDirectories(path);
    CompoundCheckpointReader reader = new CompoundCheckpointReader(checkpoint);
    Optional<Entry> entryOpt;
    while ((entryOpt = reader.nextCheckpoint()).isPresent()) {
      Entry entry = entryOpt.get();
      System.out.printf("Reading checkpoint for %s to %s%n", entry.getName(), path.toAbsolutePath());
      Path checkpointPath = path.resolve(entry.getName().toString());
      readCheckpoint(entry.getStream(), checkpointPath);
    }
  }

  private static void readJournalCheckpoint(CheckpointInputStream checkpoint, Path path) throws IOException {
    JournalCheckpointReader reader = new JournalCheckpointReader(checkpoint);
    try (PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
      Optional<JournalEntry> entry;
      while ((entry = reader.nextEntry()).isPresent()) {
        out.write(ENTRY_SEPARATOR + "\n");
        out.write(entry.get() + "\n");
      }
    }
  }

  private static void readRocksCheckpoint(CheckpointInputStream checkpoint, Path path) throws IOException {
    TarballCheckpointReader reader = new TarballCheckpointReader(checkpoint);
    reader.unpackToDirectory(path);
  }

  private static void readLongsCheckpoint(CheckpointInputStream checkpoint, Path path) throws IOException {
    PrintStream out =
        new PrintStream(new BufferedOutputStream(new FileOutputStream(path.toFile())));
    LongsCheckpointReader reader = new LongsCheckpointReader(checkpoint);
    Optional<Long> longOpt;
    while ((longOpt = reader.nextLong()).isPresent()) {
      out.printf("%d ", longOpt.get());
    }
    out.close();
    return;
  }

  private static void readProtosCheckpoint(CheckpointInputStream checkpoint, Path path)
      throws IOException {
    InodeProtosCheckpointReader reader = new InodeProtosCheckpointReader(checkpoint);
    try (PrintWriter out = new PrintWriter(new FileOutputStream(path.toFile()))) {
      Optional<InodeMeta.Inode> entry;
      System.out.printf("Reading inode meta checkpoint to %s%n", path.toAbsolutePath());
      while ((entry = reader.read()).isPresent()) {
        out.write(ENTRY_SEPARATOR + "\n");
        out.write(entry.get() + "\n");
      }
    }
  }

  /**
   * @return the journal location
   */
  private static URI getJournalLocation() {
    String journalDirectory = ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
      journalDirectory += AlluxioURI.SEPARATOR;
    }
    try {
      return new URI(journalDirectory);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parses the input args with a command line format, using
   * {@link org.apache.commons.cli.CommandLineParser}.
   *
   * @param args the input args
   * @return true if parsing succeeded
   */
  private static boolean parseInputArgs(String[] args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(OPTIONS, args);
    } catch (ParseException e) {
      System.out.println("Failed to parse input args: " + e);
      return false;
    }
    sHelp = cmd.hasOption("help");
    sMaster = cmd.getOptionValue("master", "FileSystemMaster");
    sOutputDir = cmd.getOptionValue("outputDir", "journal_dump-" + System.currentTimeMillis());
    sCheckpointsDir = PathUtils.concatPath(sOutputDir, "checkpoints");
    sJournalEntryFile = PathUtils.concatPath(sOutputDir, "edits.txt");
    return true;
  }

  /**
   * Prints the usage.
   */
  private static void usage() {
    new HelpFormatter().printHelp(
        "java -cp alluxio-" + RuntimeConstants.VERSION
            + "-jar-with-dependencies.jar alluxio.master.journal.JournalTool",
        "Read an Alluxio journal and write it to a directory in a human-readable format.", OPTIONS,
        "", true);
  }
}
