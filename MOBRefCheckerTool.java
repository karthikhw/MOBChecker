
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This utility help us to find mob references
 */
public class MOBRefCheckerTool extends Configured implements Tool {

  private static final Logger LOG =
      LoggerFactory.getLogger(MOBRefCheckerTool.class.getName());
  private FileSystem fileSystem;
  private Configuration configuration;
  private Connection hconnection;
  private final List<FileStatus> mobStoreFiles = new ArrayList<FileStatus>();
  private final Set<String> mobPointers = new HashSet<String>();
  private final Map<String, List<String>> snapshotManifestFiles =
      new HashMap<String, List<String>>();
  private static final byte[] MOB_FILE_REFS = "MOB_FILE_REFS".getBytes();
  private static Path HBASE_HOME_DIR = null;

  /**
   * Store file list from Archive
   */
  public void getStoreFiles(FileStatus fileStatus, boolean mob) throws IOException {
    boolean ismob = mob;
    if (fileStatus.isDirectory()) {
      FileStatus[] fstatus = fileSystem.listStatus(fileStatus.getPath());
      for (FileStatus file : fstatus) {
        getStoreFiles(file, ismob);
      }
    }
    if (fileStatus.isFile()) {
      if (ismob) {
        mobStoreFiles.add(fileStatus);
      } else {

        try {
          boolean isHfile = HFile.isHFileFormat(fileSystem, fileStatus);
          if (isHfile) {
            HFile.Reader reader = HFile.createReader(fileSystem, fileStatus.getPath(), new CacheConfig(configuration),
                configuration);
            Map<byte[], byte[]> fileInfo = reader.loadFileInfo();
            boolean isMobRef = fileInfo.containsKey(MOB_FILE_REFS);
            if (isMobRef) {
              mobPointers.add(Bytes.toStringBinary(fileInfo.get(MOB_FILE_REFS)));
            }
          }
        } catch (FileNotFoundException e) {
          LOG.error("Error in storefilereader "+ e.getMessage());
        } catch (Exception f) {
          LOG.error("Error in storefilereader "+ f.getMessage());
        }
      }
    }
  }

  private List<String> getFilesReferencedBySnapshot(SnapshotDescription snapshotDescription)
      throws IOException {
    final List<String> files = new ArrayList<String>();
    Path snapshotDir = SnapshotDescriptionUtils
        .getCompletedSnapshotDir(snapshotDescription.getName(), HBASE_HOME_DIR);
    SnapshotDescription sd = SnapshotDescriptionUtils.readSnapshotInfo(fileSystem, snapshotDir);
    SnapshotManifest manifest = SnapshotManifest.open(configuration, fileSystem, snapshotDir, sd);
    // For each region referenced by the snapshot
    for (SnapshotRegionManifest rm : manifest.getRegionManifests()) {
      // For each column family in this region
      for (SnapshotRegionManifest.FamilyFiles ff : rm.getFamilyFilesList()) {
        // And each store file in that family
        for (SnapshotRegionManifest.StoreFile sf : ff.getStoreFilesList()) {
          files.add(sf.getName());
        }
      }
    }
    return files;
  }



  private void loadSnapshotManifestInMemory(TableName tableName) throws IOException {
    final Admin admin = hconnection.getAdmin();
    List<SnapshotDescription> snapshotList = admin.listSnapshots();
    for (SnapshotDescription snapshotDesc : snapshotList) {
      if (snapshotDesc.getTable().equals(tableName.getNameAsString())) {
        snapshotManifestFiles
            .put(snapshotDesc.getName(), getFilesReferencedBySnapshot(snapshotDesc));
      }
    }
  }

  private boolean isFileReferencedInSnapshot(FileStatus fileStatus) {
    final String fileName = fileStatus.getPath().getName();
    Iterator<Map.Entry<String, List<String>>> snapshotItr =
        snapshotManifestFiles.entrySet().iterator();
    while (snapshotItr.hasNext()) {
      Map.Entry<String, List<String>> snapshot = snapshotItr.next();
      boolean exist = snapshot.getValue().contains(fileName);
      if (exist) {
        LOG.info("Found '{}' in snapshot '{}'", fileName, snapshot.getKey());
        return exist;
      }
    }
    return false;
  }



  private void printUsage() {
    System.err.println(
        "Usage:\n" + "--------------------------\n hbase " + MOBRefCheckerTool.class.getName()
            + "   <tableName>  <-report|-delete>  <deletefiles-olderthan-X-days>");
    System.err.println(" tableName        The table name");
    System.err.println(" -report          Run report");
  }

  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      printUsage();
      return -1;
    }
    final TableName tableName = TableName.valueOf(args[0]);
    configuration = getConf();
    fileSystem = FileSystem.get(configuration);
    hconnection = ConnectionFactory.createConnection(configuration);
    HBASE_HOME_DIR = new Path(configuration.get(HConstants.HBASE_DIR));
    final Admin admin = hconnection.getAdmin();
    if (!admin.tableExists(tableName)) {
      throw new TableNotFoundException(tableName);
    }



    final Path mobPath = MobUtils.getMobRegionPath(configuration,tableName);

    final FileStatus[] mobFileList = fileSystem.listStatus(mobPath);


    LOG.info("No of MOB ref pointer is "+mobPointers.size());

    LOG.info("MOB ref pointers "+mobPointers.toString());

    LOG.info("Loading mob files");
    for (FileStatus file : mobFileList) {
      getStoreFiles(file, true);
    }
    Path tableDir = FSUtils.getTableDir(HBASE_HOME_DIR, tableName);
    FileStatus[] tableFileList = fileSystem.listStatus(tableDir);
    LOG.info("Loading mob file pointer");
    for (FileStatus file : tableFileList) {
      getStoreFiles(file, false);
    }
    LOG.info("Loading snapshot manifest");
    //Load snapshot manifests
    loadSnapshotManifestInMemory(tableName);

    LOG.info("Starting mob tool");
    for (FileStatus fileStatus : mobStoreFiles) {

      LOG.debug("Validating file '{}'", fileStatus.getPath());


      boolean isReferencedWSnapshot = isFileReferencedInSnapshot(fileStatus);
      if (isReferencedWSnapshot) {
        LOG.info("File '{}' is ignored, having a snapspshot reference", fileStatus.getPath());
        continue;
      }

      boolean isMobPointer = mobPointers.contains(fileStatus.getPath().getName());
      if (isMobPointer) {
        LOG.info("File '{}' is ignored, having a table reference", fileStatus.getPath());
        continue;
      }
    }

    LOG.info("Mob Ref Checker is completed");
    return 0;
  }

  public static void main(String[] args) throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new MOBRefCheckerTool(), args);
    System.exit(ret);
  }

}