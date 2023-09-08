package org.opencb.opencga.storage.hadoop.app;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.tools.ant.types.Commandline;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class HBaseMain extends AbstractMain {

    public static final String MOVE_TABLE_REGIONS = "move-table-regions";
    public static final String REASSIGN_TABLES_WITH_REGIONS_ON_DEAD_SERVERS = "reassign-tables-with-regions-on-dead-servers";
    public static final String CHECK_TABLES_WITH_REGIONS_ON_DEAD_SERVERS = "check-tables-with-regions-on-dead-servers";
    public static final String BALANCE_TABLE_REGIONS = "balance-table-regions";
    public static final String LIST_TABLES = "list-tables";
    public static final String LIST_SNAPSHOTS = "list-snapshots";
    public static final String REGIONS_PER_TABLE = "regions-per-table";
    public static final String CLONE_TABLES = "clone-tables";
    public static final String SNAPSHOT_TABLES = "snapshot-tables";
    public static final String SNAPSHOT_TABLE = "snapshot-table";
    public static final String DELETE_SNAPSHOTS = "delete-snapshots";
    public static final String CLONE_SNAPSHOTS = "clone-snapshots";
    public static final String EXPORT_SNAPSHOTS = "export-snapshots";
    public static final String EXEC = "exec";
    public static final String DISABLE_TABLE = "disable-table";
    public static final String DROP_TABLE = "drop-table";
    public static final String ENABLE_TABLE = "enable-table";
    public static final String DELETE_TABLE = "delete-table";

    protected static final Logger LOGGER = LoggerFactory.getLogger(HBaseMain.class);
    private final HBaseManager hBaseManager;

    public static void main(String[] args) throws Exception {
        new HBaseMain().run(args);
    }

    public HBaseMain() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.setInt("hbase.client.sync.wait.timeout.msec", 30 * 60000); // 30min
        hBaseManager = new HBaseManager(configuration);
    }


    @Override
    public void run(String[] args) throws Exception {
        String command = getArg(args, 0, "help");

        switch (command) {
            case REASSIGN_TABLES_WITH_REGIONS_ON_DEAD_SERVERS:
                reassignTablesWithRegionsOnDeadServers(args);
                break;
            case CHECK_TABLES_WITH_REGIONS_ON_DEAD_SERVERS:
                checkTablesWithRegionsOnDeadServers(args);
                break;
            case MOVE_TABLE_REGIONS:
                moveTableRegions(args);
                break;
            case BALANCE_TABLE_REGIONS:
                balanceTableRegions(getArg(args, 1), getArgsMap(args, 2, "maxMoves"));
                break;
            case "tables":
            case LIST_TABLES:
                print(listTables(getArg(args, 1, "")).stream().map(TableName::getNameWithNamespaceInclAsString).iterator());
                break;
            case "snapshots":
            case LIST_SNAPSHOTS: {
                ObjectMap objectMap = getArgsMap(args, 1, "filter", "tree");
                String filter = objectMap.getString("filter");
                Map<String, TreeMap<Date, String>> tablesMap = listSnapshots(filter, true);
                if (objectMap.getBoolean("tree")) {
                    for (Map.Entry<String, TreeMap<Date, String>> entry : tablesMap.entrySet()) {
                        println(entry.getKey());
                        for (Map.Entry<Date, String> snpEntry : entry.getValue().entrySet()) {
                            println(" - " + snpEntry.getValue() + "      (" + snpEntry.getKey() + ")");
                        }
                    }
                } else {
                    for (Map.Entry<String, TreeMap<Date, String>> entry : tablesMap.entrySet()) {
                        for (Map.Entry<Date, String> snpEntry : entry.getValue().entrySet()) {
                            println(snpEntry.getValue() + "      (" + snpEntry.getKey() + ")");
                        }
                    }
                }
                break;
            }
            case REGIONS_PER_TABLE:
                regionsPerTable(getArg(args, 1));
                break;
            case CLONE_TABLES: {
                ObjectMap argsMap = getArgsMap(args, 3, "keepSnapshots", "snapshotSuffix", "dryRun");
                cloneTables(getArg(args, 1), getArg(args, 2),
                        argsMap.getBoolean("keepSnapshots"),
                        argsMap.getString("snapshotSuffix", "_SNAPSHOT_" + TimeUtils.getTime()),
                        argsMap.getBoolean("dryRun"));
                break;
            }
            case SNAPSHOT_TABLES: {
                ObjectMap argsMap = getArgsMap(args, 2, "snapshotSuffix", "dryRun", "skipTablesWithSnapshot");
                String tableNamePattern = getArg(args, 1) + ".*";
                snapshotTables(tableNamePattern,
                        argsMap.getString("snapshotSuffix", "_SNAPSHOT_" + TimeUtils.getTime()),
                        argsMap.getBoolean("dryRun"), argsMap.getBoolean("skipTablesWithSnapshot"));
                break;
            }
            case SNAPSHOT_TABLE: {
                ObjectMap argsMap = getArgsMap(args, 2, "snapshotSuffix", "dryRun", "skipTablesWithSnapshot");
                snapshotTables(getArg(args, 1),
                        argsMap.getString("snapshotSuffix", "_SNAPSHOT_" + TimeUtils.getTime()),
                        argsMap.getBoolean("dryRun"), argsMap.getBoolean("skipTablesWithSnapshot"));
                break;
            }
            case DELETE_SNAPSHOTS: {
                ObjectMap argsMap = getArgsMap(args, 2, "dryRun", "skipMissing");
                deleteSnapshots(Arrays.asList(getArg(args, 1).split(",")), argsMap.getBoolean("dryRun"), argsMap.getBoolean("skipMissing"));
                break;
            }
            case CLONE_SNAPSHOTS: {
                ObjectMap argsMap = getArgsMap(args, 2, "tablePrefixChange", "dryRun", "onExistingTables");
                cloneSnapshots(getArg(args, 1),
                        argsMap.getString("tablePrefixChange"),
                        argsMap.getBoolean("dryRun"),
                        argsMap.getString("onExistingTables", "fail").toLowerCase()
                        );
                break;
            }
            case EXPORT_SNAPSHOTS: {
                ObjectMap argsMap = getArgsMap(args, 1, "dryRun", "snapshot", "copy-to", "copy-to-local", "copy-from", "target",
                        "mappers", "overwrite", "D");
                exportSnapshot(null,
                        argsMap.getString("snapshot"),
                        argsMap.getString("copy-to"),
                        argsMap.getBoolean("copy-to-local"),
                        argsMap.getString("copy-from"),
                        argsMap.getString("target"),
                        argsMap.getString("mappers"),
                        argsMap.getBoolean("overwrite"),
                        argsMap.getBoolean("dryRun"),
                        argsMap);
                break;
            }
            case EXEC: {
                exec(getArg(args, 1), Arrays.asList(args).subList(2, args.length));
                break;
            }
            case DISABLE_TABLE: {
                ObjectMap argsMap = getArgsMap(args, 2, "dryRun");
                disableTables(getArg(args, 1), argsMap.getBoolean("dryRun"));
                break;
            }
            case DROP_TABLE: {
                ObjectMap argsMap = getArgsMap(args, 2, "dryRun");
                dropTables(getArg(args, 1), argsMap.getBoolean("dryRun"));
                break;
            }
            case ENABLE_TABLE: {
                ObjectMap argsMap = getArgsMap(args, 2, "dryRun");
                enableTables(getArg(args, 1), argsMap.getBoolean("dryRun"));
                break;
            }
            case DELETE_TABLE: {
                ObjectMap argsMap = getArgsMap(args, 2, "dryRun");
                deleteTable(getArg(args, 1), argsMap.getBoolean("dryRun"));
                break;
            }
            case "help":
            default:
                System.out.println("Commands:");
                System.out.println("  " + LIST_TABLES + " [<table-name-regex>]");
                System.out.println("  " + LIST_SNAPSHOTS + " [--filter <snapshot-name-regex>] [--tree]");
                System.out.println("  " + CHECK_TABLES_WITH_REGIONS_ON_DEAD_SERVERS + " [ <table-name-regex> ]");
                System.out.println("      Get the list of regions on dead servers for all selected tables");
                System.out.println("  " + REASSIGN_TABLES_WITH_REGIONS_ON_DEAD_SERVERS + " <table-name-regex>");
                System.out.println("      Reassign all regions from tables with regions on dead servers");
                System.out.println("        (see " + CHECK_TABLES_WITH_REGIONS_ON_DEAD_SERVERS + ") by creating a temporary snapshot");
                System.out.println("  " + MOVE_TABLE_REGIONS + " <table-name-regex>");
                System.out.println("      Move all regions from selected tables to new random nodes.");
//                System.out.println("  " + BALANCE_TABLE_REGIONS + " <table-name> [--maxMoves N]"); // FIXME
//                System.out.println("  " + REGIONS_PER_TABLE + " <table-name>"); // FIXME
                System.out.println("  " + CLONE_TABLES + " <table-name-prefix> <new-table-name-prefix> "
                                        + "[--keepSnapshots] [--dryRun] [--snapshotSuffix <snapshotNameSuffix>]");
                System.out.println("      Clone all selected tables by creating an intermediate snapshot.");
                System.out.println("        Optionally remove the intermediate snapshot.");
                System.out.println("  " + SNAPSHOT_TABLES + " <table-name-prefix> [--dryRun] [--snapshotSuffix <snapshotNameSuffix>] "
                                        + "[--skipTablesWithSnapshot]");
                System.out.println("  " + SNAPSHOT_TABLE  + " <table-name> [--dryRun] [--snapshotSuffix <snapshotNameSuffix>] "
                                        + "[--skipTablesWithSnapshot]");
                System.out.println("  " + DELETE_SNAPSHOTS + " <snapshots-list> [--dryRun] [--skipMissing]");
                System.out.println("      Create a snapshot for all selected tables.");
                System.out.println("  " + CLONE_SNAPSHOTS + " <snapshot-name-regex> [--dryRun] "
                                        + "[--tablePrefixChange <oldPrefix>:<newPrefix>] "
                                        + "[--onExistingTables [fail|skip|drop] ]");
                System.out.println("      Clone all snapshots into tables matching the regex. "
                                        + "Generated tables can have a table prefix change.");
                System.out.println("  " + EXPORT_SNAPSHOTS + " \n"
                        + "          --dryRun <arg>         Dry run.\n"
                        + "          --snapshot <arg>       Snapshot to restore.\n"
                        + "          --copy-to <arg>        Remote destination hdfs://\n"
                        + "          --copy-to-local        Flag to indicate that must copy to local hbase.rootdir (for imports)\n"
                        + "          --copy-from <arg>      Input folder hdfs:// (default hbase.rootdir)\n"
                        + "          --target <arg>         Target name for the snapshot.\n"
//                        + "          --no-checksum-verify   Do not verify checksum, use name+length only.\n"
//                        + "          --no-target-verify     Do not verify the integrity of the exported snapshot.\n"
                        + "          --overwrite            Rewrite the snapshot manifest if already exists.\n"
//                        + "          --chuser <arg>         Change the owner of the files to the specified one.\n"
//                        + "          --chgroup <arg>        Change the group of the files to the specified one.\n"
//                        + "          --chmod <arg>          Change the permission of the files to the specified one.\n"
//                        + "          --bandwidth <arg>      Limit bandwidth to this value in MB/second.\n"
                        + "          --mappers <arg>        Number of mappers to use during the copy (mapreduce.job.maps).\n"
                        + "          -Dkey=value            Other key-value fields");
                System.out.println("      Export a given snapshot an external location.");
                System.out.println("  " + EXEC + "[hadoop|yarn|hbase|hdfs]");
                System.out.println("      Execute a MR job on the hadoop cluster. Use \"exec yarn jar ....\"");
                System.out.println("  " + DISABLE_TABLE + " <table-name-regex> [--dryRun]");
                System.out.println("      Disable all tables matching the regex.");
                System.out.println("  " + DROP_TABLE + " <table-name-regex> [--dryRun]");
                System.out.println("      Drop all tables matching the regex. Must be disabled first.");
                System.out.println("  " + ENABLE_TABLE + " <table-name-regex> [--dryRun]");
                System.out.println("      Enable all tables matching the regex. Ignore enabled tables.");
                System.out.println("  " + DELETE_TABLE + " <table-name-regex> [--dryRun]");
                System.out.println("      Disable (if needed) and drop all tables matching the regex.");
                break;
        }

    }

    private void exec(String tool, List<String> args) throws Exception {
        Path opencgaHome = Paths.get(System.getProperty("app.home"));
        String storageConfigurationPath = opencgaHome.resolve("conf").resolve("storage-configuration.yml").toString();
        StorageConfiguration storageConfiguration;
        try (FileInputStream is = new FileInputStream(storageConfigurationPath)) {
            storageConfiguration = StorageConfiguration.load(is);
        }

        HadoopVariantStorageEngine engine = new HadoopVariantStorageEngine();
        engine.setConfiguration(storageConfiguration, HadoopVariantStorageEngine.STORAGE_ENGINE_ID, "");

        MRExecutor mrExecutor = engine.getMRExecutor();
        int exitError = mrExecutor.run(tool, args.toArray(new String[0]));
        if (exitError != 0) {
            throw new Exception("Exec failed with exit number '" + exitError + "'");
        }
    }

    private void exportSnapshot(String storageConfigurationPath, String snapshot, String copyTo, boolean copyToLocal,
                                String copyFrom, String target,
                                String mappers, boolean overwrite, boolean dryRun, ObjectMap options) throws Exception {
        if (storageConfigurationPath == null) {
            Path opencgaHome = Paths.get(System.getProperty("app.home"));
            storageConfigurationPath = opencgaHome.resolve("conf").resolve("storage-configuration.yml").toString();
        }
        StorageConfiguration storageConfiguration;
        try (FileInputStream is = new FileInputStream(storageConfigurationPath)) {
            storageConfiguration = StorageConfiguration.load(is);
        }

        List<String> args = new LinkedList<>();
        args.add(org.apache.hadoop.hbase.snapshot.ExportSnapshot.class.getName());
        for (Map.Entry<String, Object> entry : options.get("D", ObjectMap.class, new ObjectMap()).entrySet()) {
            args.add("-D" + entry.getKey() + "=" + entry.getValue().toString());
        }
        args.add("--snapshot");
        args.add(snapshot);

        args.add("--copy-to");
        if (StringUtils.isNotEmpty(copyTo)) {
            args.add(copyTo);
            if (copyToLocal) {
                throw new Exception("Incompatible arguments `--copy-to` and `--copy-to-local`. Use only one of them");
            }
        } else if (copyToLocal) {
            args.add(hBaseManager.getConf().get(HConstants.HBASE_DIR));
        } else {
            throw new Exception("Missing copy destination. Add either `--copy-to` or `--copy-to-local`");
        }
        if (StringUtils.isNotEmpty(copyFrom)) {
            args.add("--copy-from");
            args.add(copyFrom);
        }
        if (StringUtils.isNotEmpty(target)) {
            args.add("--target");
            args.add(target);
        }
        if (overwrite) {
            args.add("--overwrite");
        }
        if (StringUtils.isNotEmpty(mappers)) {
            args.add("--mappers");
            args.add(mappers);
        }

        if (dryRun) {
            System.out.println("hbase " + Commandline.toString(args.toArray(new String[0])));
        } else {
            HadoopVariantStorageEngine engine = new HadoopVariantStorageEngine();
            engine.setConfiguration(storageConfiguration, HadoopVariantStorageEngine.STORAGE_ENGINE_ID, "");

            MRExecutor mrExecutor = engine.getMRExecutor();
            int exitError = mrExecutor.run("hbase", args.toArray(new String[0]));
            if (exitError != 0) {
                throw new Exception("ExportSnapshot failed with exit number '" + exitError + "'");
            }
        }
    }

    private void regionsPerTable(String tableNameStr) throws Exception {
//        TableName tableName = getTable(tableNameStr);
//        hBaseManager.act(tableName.getNameAsString(), (table, admin) -> {
//            List<ServerName> servers = new ArrayList<>(admin.getClusterStatus().getServers());
//            Map<String, Integer> regionsPerServer = new HashMap<>();
//
//            List<Pair<RegionInfo, ServerName>> tableRegionsAndLocations = getTableRegionsAndLocations(tableName, admin);
//
//            System.out.println("#REGION\tSERVER\tSTART_KEY\tEND_KEY");
//            for (Pair<RegionInfo, ServerName> pair : tableRegionsAndLocations) {
//                RegionInfo region = pair.getFirst();
//                ServerName server = pair.getSecond();
//                regionsPerServer.merge(server.getServerName(), 1, Integer::sum);
//
//                System.out.println(region.getEncodedName()
//                        + "\t" + server.getServerName()
//                        + "\t" + Bytes.toStringBinary(region.getStartKey())
//                        + "\t" + Bytes.toStringBinary(region.getEndKey()));
//            }
//
//            System.out.println("");
//            System.out.println("#SERVER\tREGIONS");
//            for (ServerName server : servers) {
//                System.out.println(server.getServerName() + "\t" + regionsPerServer.getOrDefault(server.getServerName(), 0));
//            }
//
//
//
//            return null;
//        });
    }

//    private List<Pair<RegionInfo, ServerName>> getTableRegionsAndLocations(TableName tableName, Admin admin) throws IOException {
//        List<Pair<RegionInfo, ServerName>> tableRegionsAndLocations;
////        try (ZooKeeperWatcher zkw = new ZooKeeperWatcher(admin.getConfiguration(), "hbase-main", null)) {
////            tableRegionsAndLocations = MetaTableAccessor
////                    .getTableRegionsAndLocations(zkw, admin.getConnection(), tableName);
////        }
//        tableRegionsAndLocations = MetaTableAccessor
//                .getTableRegionsAndLocations(admin.getConnection(), tableName);
//        return tableRegionsAndLocations;
//    }

    private void reassignTablesWithRegionsOnDeadServers(String[] args) throws Exception {
        String tableNameFilter = getArg(args, 1);
        List<TableName> tables = listTables(tableNameFilter);
        List<String> corruptedTables = checkTablesWithRegionsOnDeadServers(tables);
        for (String corruptedTable : corruptedTables) {
            reassignTableRegions(corruptedTable);
        }
        System.out.println("# Fixed " + corruptedTables.size() + " corrupted tables");
        System.out.println("Done!");
    }

    private void checkTablesWithRegionsOnDeadServers(String[] args) throws Exception {
        String tableNameFilter = getArg(args, 1);
        List<TableName> tables = listTables(tableNameFilter);
        List<String> corruptedTables = checkTablesWithRegionsOnDeadServers(tables);

        System.out.println("# Found " + corruptedTables.size() + " corrupted tables out of " + tables.size());
        for (String corruptedTable : corruptedTables) {
            System.out.println(corruptedTable);
        }
    }

    private void moveTableRegions(String[] args) throws Exception {
        String tableNameFilter = getArg(args, 1);
        List<TableName> tables = listTables(tableNameFilter);
        Map<String, Integer> regionCount = new LinkedHashMap<>();
        for (TableName tableName : tables) {
            int regions = hBaseManager.act(tableName.getNameAsString(), (table, admin) -> {
                List<HRegionInfo> tableRegions = admin.getTableRegions(tableName);
                ProgressLogger progressLogger =
                        new ProgressLogger("Moving regions from table '" + tableName.getNameAsString() + "'", tableRegions.size());
                for (HRegionInfo tableRegion : tableRegions) {
                    admin.move(tableRegion.getEncodedNameAsBytes(), (byte[]) null);
                    progressLogger.increment(1, tableRegion::getRegionNameAsString);
                }

                return tableRegions.size();
            });
            regionCount.put(tableName.getNameAsString(), regions);
        }
        System.out.println("#Moved regions from " + tables.size() + " tables");
        System.out.println("#TABLE\tNUM_REGIONS");
        regionCount.forEach((table, count) -> {
            System.out.println(table + "\t" + count);
        });
    }

    private void balanceTableRegions(String tableNameStr, ObjectMap options) throws Exception {
//        TableName tableName = getTable(tableNameStr);
//
//        int regionCount = hBaseManager.act(tableName.getNameAsString(), (table, admin) -> {
//            int maxMoves = options.getInt("maxMoves", 50000);
//            List<ServerName> servers = new ArrayList<>(admin.getClusterStatus().getServers());
//            List<Pair<RegionInfo, ServerName>> tableRegionsAndLocations = getTableRegionsAndLocations(tableName, admin);
//            int expectedRegionsPerServer = (tableRegionsAndLocations.size() / servers.size()) + 1;
//            Map<String, Integer> regionsPerServer = new HashMap<>();
//            servers.forEach(s -> regionsPerServer.put(s.getServerName(), 0));
//            for (Pair<RegionInfo, ServerName> pair : tableRegionsAndLocations) {
//                regionsPerServer.merge(pair.getSecond().getServerName(), 1, Integer::sum);
//            }
//
//            for (Pair<RegionInfo, ServerName> pair : tableRegionsAndLocations) {
//                if (maxMoves < 0) {
//                    System.out.println("Reached max moves!");
//                    break;
//                }
//
//                String sourceHost = pair.getSecond().getServerName();
//                if (regionsPerServer.get(sourceHost) > expectedRegionsPerServer) {
//                    Collections.shuffle(servers);
//                    Optional<ServerName> targetOptional = servers.stream()
//                            .filter(s -> regionsPerServer.get(s.getServerName()) < expectedRegionsPerServer).findAny();
//                    if (!targetOptional.isPresent()) {
//                        break;
//                    }
//                    String testHost = targetOptional.get().getServerName();
//                    regionsPerServer.merge(sourceHost, -1, Integer::sum);
//                    regionsPerServer.merge(testHost, 1, Integer::sum);
//                    System.out.println("Move region '" + pair.getFirst().getEncodedName() + "' from " + sourceHost + " to " + testHost);
//                    StopWatch stopWatch = StopWatch.createStarted();
//                    admin.move(pair.getFirst().getEncodedNameAsBytes(), Bytes.toBytes(testHost));
//                    System.out.println("Moved in "+TimeUtils.durationToString(stopWatch));
//
//                    maxMoves--;
//                }
//            }
//            return tableRegionsAndLocations.size();
//        });
    }


    private TableName getTable(String tableName) throws IOException {
        List<TableName> tableNames = listTables(tableName);
        if (tableNames.size() > 1) {
            throw new IllegalArgumentIOException("Expected one table for tableName='" + tableName + "' . "
                    + "Found " + tableNames.size() + " : "
                    + tableNames.stream().map(TableName::getNameWithNamespaceInclAsString).collect(Collectors.toList()));
        }
        return tableNames.get(0);
    }

    private Map<String, TreeMap<Date, String>> listSnapshots(String snapshotFilter, boolean failOnMissing) throws Exception {
        try (Admin admin = hBaseManager.getConnection().getAdmin()) {
            Map<String, TreeMap<Date, String>> tablesMap = new TreeMap<>();
            List<?> snapshots = StringUtils.isBlank(snapshotFilter) ? admin.listSnapshots() : admin.listSnapshots(snapshotFilter);
            if (failOnMissing && CollectionUtils.isEmpty(snapshots)) {
                throw new IllegalArgumentException("Snapshot not found!");
            }
            for (Object snapshot : snapshots) {
                Class<?> aClass = snapshot.getClass();
                Date creationDate = Date.from(Instant.ofEpochMilli(((long) aClass.getMethod("getCreationTime").invoke(snapshot))));
                String table = aClass.getMethod("getTable").invoke(snapshot).toString();
                String snapshotName = aClass.getMethod("getName").invoke(snapshot).toString();

                tablesMap.computeIfAbsent(table, k -> new TreeMap<>()).put(creationDate, snapshotName);
            }
            return tablesMap;
        }
    }

    private List<TableName> listTables(String tableNameFilter) throws IOException {
        try (Admin admin = hBaseManager.getConnection().getAdmin()) {
            TableName[] tables;
            if (StringUtils.isEmpty(tableNameFilter) || tableNameFilter.equalsIgnoreCase("all")) {
                tables = admin.listTableNames();
            } else {
                tables = admin.listTableNames(tableNameFilter);
            }

            if (ArrayUtils.isEmpty(tables)) {
                throw new IllegalArgumentException("Table not found!");
            }
            return Arrays.asList(tables);
        }
    }

    private List<String> checkTablesWithRegionsOnDeadServers(List<TableName> tables) throws IOException {
        List<String> corruptedTables = new ArrayList<>();

        try (Admin admin = hBaseManager.getConnection().getAdmin()) {
            ClusterStatus clusterStatus = admin.getClusterStatus();
            HashSet<ServerName> deadServers = new HashSet<>(clusterStatus.getDeadServerNames());

            for (TableName tableName : tables) {
                if (checkTableWithRegionsOnDeadServers(admin, deadServers, tableName)) {
                    corruptedTables.add(tableName.getNameAsString());
                }
            }
        }
        return corruptedTables;
    }

    private boolean checkTableWithRegionsOnDeadServers(Admin admin, HashSet<ServerName> deadServers, TableName tableName)
            throws IOException {
        for (HRegionInfo tableRegion : admin.getTableRegions(tableName)) {
            HRegionLocation regionLocation = MetaTableAccessor.getRegionLocation(hBaseManager.getConnection(), tableRegion);
            if (deadServers.contains(regionLocation.getServerName())) {
                return true;
            }
        }
        return false;
    }

    private void reassignTableRegions(String tableName) throws IOException {
        if (StringUtils.isBlank(tableName)) {
            throw new IllegalArgumentException("Missing tableName");
        }
        String snapshotName = tableName + "_SNAPSHOT_" + TimeUtils.getTime();
        LOGGER.info("About to reassign all regions from table '{}' by creating a temporary snapshot '{}'", tableName, snapshotName);
        LOGGER.info("----------");
        LOGGER.info("disable '{}'", tableName);
        LOGGER.info("snapshot '{}', '{}'", tableName, snapshotName);
        LOGGER.info("drop '{}'", tableName);
        LOGGER.info("clone_snapshot '{}', '{}'", snapshotName, tableName);
        LOGGER.info("delete_snapshot '{}'", snapshotName);
        LOGGER.info("----------");

        hBaseManager.act(tableName, (table, admin) -> {

            if (admin.isTableEnabled(table.getName())) {
                LOGGER.info("Disable table '{}'", tableName);
                runWithTime("Disable table", () -> admin.disableTable(table.getName()));
            } else {
                LOGGER.info("Table '{}' already disabled", tableName);
            }

            LOGGER.info("Create SNAPSHOT '{}' from table '{}'", snapshotName, tableName);
//            runWithTime("Create Snapshot", () -> admin.snapshot(snapshotName, table.getName(),
//            HBaseProtos.SnapshotDescription.Type.FLUSH));
            runWithTime("Create Snapshot", () -> admin.snapshot(snapshotName, table.getName()));

            LOGGER.info("Delete corrupted table '{}'", tableName);
            runWithTime("Delete table", () -> admin.deleteTable(table.getName()));

            LOGGER.info("Clone SNAPSHOT '{}' into table '{}' to reassign all regions", snapshotName, tableName);
            runWithTime("Clone SNAPSHOT", () -> admin.cloneSnapshot(snapshotName, table.getName()));

            LOGGER.info("Delete SNAPSHOT '{}'", snapshotName);
            runWithTime("Delete SNAPSHOT", () -> admin.deleteSnapshot(snapshotName));

            return null;
        });
    }

    private void snapshotTables(String tableNamePattern, String snapshotSuffix, boolean dryRun, boolean skipTablesWithSnapshot)
            throws Exception {
        Connection connection = hBaseManager.getConnection();
        Map<String, TreeMap<Date, String>> tablesWithSnapshots = listSnapshots(tableNamePattern, false);
        Map<String, String> newSnapshots = new HashMap<>();
        try (Admin admin = connection.getAdmin()) {
            for (TableName tableName : listTables(tableNamePattern)) {
                if (skipTablesWithSnapshot && tablesWithSnapshots.containsKey(tableName.getNameAsString())) {
                    TreeMap<Date, String> map = tablesWithSnapshots.get(tableName.getNameAsString());
                    LOGGER.info("Skip snapshot from table '{}' . Already has a snapshot: {}", tableName.getNameAsString(), map);
                    continue;
                }
                String snapshotName = tableName + snapshotSuffix;
                LOGGER.info("Create snapshot '" + snapshotName + "' from table " + tableName);
                if (dryRun) {
                    LOGGER.info("[DRY-RUN] admin.snapshot('{}', '{}');", snapshotName, tableName);
                } else {
                    admin.snapshot(snapshotName, tableName);
                }
                newSnapshots.put(tableName.getNameWithNamespaceInclAsString(), snapshotName);
            }
        }
        print(newSnapshots);
    }


    private void deleteSnapshots(List<String> snapshots, boolean dryRun, boolean skipMissing)
            throws Exception {
        snapshots = new LinkedList<>(snapshots);
        Map<String, SnapshotDescription> allSnapshots = new HashMap<>();
        try (Admin admin = hBaseManager.getConnection().getAdmin()) {
            for (SnapshotDescription snapshotDescription : admin.listSnapshots()) {
                allSnapshots.put(snapshotDescription.getName(), snapshotDescription);
            }
            // Check all snapshots exist
            Iterator<String> iterator = snapshots.iterator();
            while (iterator.hasNext()) {
                String snapshot = iterator.next();
                if (!allSnapshots.containsKey(snapshot)) {
                    if (skipMissing) {
                        LOGGER.info("Snapshot '" + snapshot + "' missing. Skipping!");
                        iterator.remove();
                    } else {
                        throw new IllegalArgumentException("Snapshot '" + snapshot + "' not found! Remove it from the list, "
                                + "or use '--skipMissing' parameter. Available snapshots : " + allSnapshots.keySet());
                    }
                }
            }
            for (String snapshot : snapshots) {
                deleteSnapshot(admin, snapshot, dryRun);
            }
        }
    }

    private void cloneSnapshots(String snapshotNamePrefix, String tablePrefixChange,
                                boolean dryRun,
                                String onExistingTables) throws Exception {
        Map<String, TreeMap<Date, String>> tablesWithSnapshots = listSnapshots(snapshotNamePrefix, true);
        List<String> tableNames = listTables(null).stream().map(TableName::toString).collect(Collectors.toList());
        String oldPrefix = "";
        String newPrefix = "";
        if (!onExistingTables.equals("fail") && !onExistingTables.equals("skip") && !onExistingTables.equals("drop")) {
            throw new IllegalArgumentException("Unknown param onExistingTables '"
                    + onExistingTables + "'. Expected 'fail', 'skip' or 'drop'");
        }

        if (StringUtils.isNotEmpty(tablePrefixChange)) {
            String[] split = tablePrefixChange.split(":");
            if (split.length != 2) {
                throw new IllegalArgumentException("Expect two values separated by `:` in the table prefix change. "
                        + "<oldPrefix>:<newPrefix>");
            }
            oldPrefix = split[0];
            newPrefix = split[1];
        }
        Map<String, String> restores = new LinkedHashMap<>();
        Set<String> tablesToDrop = new LinkedHashSet<>();

        for (Map.Entry<String, TreeMap<Date, String>> entry : tablesWithSnapshots.entrySet()) {
            String sourceTable = entry.getKey();
            TreeMap<Date, String> snapshotsMap = entry.getValue();
            String snapshot = snapshotsMap.lastEntry().getValue();
            if (!sourceTable.startsWith(oldPrefix)) {
                throw new IllegalArgumentException("Unable to restore snapshot '" + snapshot + "'. "
                        + "Its source table doesn't have the prefix '" + oldPrefix + "'");
            }
            String targetTable = StringUtils.replace(sourceTable, oldPrefix, newPrefix, 1);
            if (tableNames.contains(targetTable)) {
                switch (onExistingTables) {
                    case "skip":
                        LOGGER.info("Skip snapshot restore table '{}' from snapshot '{}'", targetTable, snapshot);
                        continue;
                    case "drop":
                        LOGGER.warn("Table '{}' already found. Drop table before restoring from snapshot '{}'", targetTable, snapshot);
                        tablesToDrop.add(targetTable);
                        break;
                    case "fail":
                    default:
                        throw new IllegalStateException("Table '" + targetTable + "' already exists! Unable to restore snapshot");
                }
            }

            LOGGER.info("Clone snapshot '{}' into table '{}'", snapshot, targetTable);
            restores.put(snapshot, targetTable);
        }

        LOGGER.info("------");
        if (dryRun) {
            LOGGER.info("[DRY-RUN] List clone snapshot operations");
        } else {
            LOGGER.info("Executing clone snapshot operations");
        }
        Connection connection = hBaseManager.getConnection();
        try (Admin admin = connection.getAdmin()) {
            for (Map.Entry<String, String> entry : restores.entrySet()) {
                String snapshot = entry.getKey();
                String table = entry.getValue();
                if (tablesToDrop.contains(table)) {
                    disableTable(admin, TableName.valueOf(table), dryRun);
                    dropTable(admin, TableName.valueOf(table), dryRun);
                }
                cloneSnapshot(admin, snapshot, table, dryRun);
            }
        }
    }

    private void cloneTables(String tableNamePrefix, String newTableNamePrefix, boolean keepSnapshot, String snapshotSuffix, boolean dryRun)
            throws IOException {
        if (newTableNamePrefix.startsWith(tableNamePrefix)) {
            throw new IllegalArgumentIOException("New tableNamePrefix can't have the old prefix as a prefix!");
        }
        Connection connection = hBaseManager.getConnection();
        try (Admin admin = connection.getAdmin()) {
            for (TableName tableName : listTables(tableNamePrefix + ".*")) {
                String snapshotName = tableName.getNameAsString() + snapshotSuffix;
                String newTableName = tableName.getNameAsString().replace(tableNamePrefix, newTableNamePrefix);
                createSnapshot(admin, tableName, snapshotName, dryRun);
                cloneSnapshot(admin, snapshotName, newTableName, dryRun);
                if (keepSnapshot) {
                    LOGGER.info("Keep snapshot '" + snapshotName + "'");
                } else {
                    deleteSnapshot(admin, snapshotName, dryRun);
                }
            }
        }
    }

    private void disableTables(String tableNameFilter, boolean dryRun) throws IOException {
        Connection connection = hBaseManager.getConnection();
        try (Admin admin = connection.getAdmin()) {
            List<TableName> enabledTables = listTables(tableNameFilter, false, TableState.State.ENABLED);
            for (TableName tableName : enabledTables) {
                disableTable(admin, tableName, dryRun);
            }
        }
    }

    private void dropTables(String tableNameFilter, boolean dryRun) throws IOException {
        try (Admin admin = hBaseManager.getConnection().getAdmin()) {
            List<TableName> disabledTables = listTables(tableNameFilter, true, TableState.State.DISABLED);
            for (TableName tableName : disabledTables) {
                dropTable(admin, tableName, dryRun);
            }
        }
    }

    private void enableTables(String tableNameFilter, boolean dryRun) throws IOException {
        try (Admin admin = hBaseManager.getConnection().getAdmin()) {
            List<TableName> disabledTables = listTables(tableNameFilter, false, TableState.State.DISABLED);
            for (TableName tableName : disabledTables) {
                enableTable(admin, tableName, dryRun);
            }
        }
    }

    private void deleteTable(String tableNameFilter, boolean dryRun) throws IOException {
        try (Admin admin = hBaseManager.getConnection().getAdmin()) {
            List<TableName> tables = listTables(tableNameFilter, false, TableState.State.DISABLED, TableState.State.ENABLED);
            for (TableName tableName : tables) {
                if (admin.isTableEnabled(tableName)) {
                    disableTable(admin, tableName, dryRun);
                }
                dropTable(admin, tableName, dryRun);
            }
        }
    }

    private void disableTable(Admin admin, TableName tableName, boolean dryRun) throws IOException {
        System.out.println("disable '" + tableName.getNameWithNamespaceInclAsString() + "'");
        if (dryRun) {
            LOGGER.info("[DRY-RUN] admin.disableTable(" + tableName.getNameWithNamespaceInclAsString() + ")");
        } else {
            if (admin.isTableEnabled(tableName)) {
                admin.disableTable(tableName);
            }
        }
    }

    private void dropTable(Admin admin, TableName tableName, boolean dryRun) throws IOException {
        System.out.println("drop '" + tableName.getNameWithNamespaceInclAsString() + "'");
        if (dryRun) {
            LOGGER.info("[DRY-RUN] admin.deleteTable(" + tableName.getNameWithNamespaceInclAsString() + ")");
        } else {
            admin.deleteTable(tableName);
        }
    }

    private void enableTable(Admin admin, TableName tableName, boolean dryRun) throws IOException {
        System.out.println("enable '" + tableName.getNameWithNamespaceInclAsString() + "'");
        if (dryRun) {
            LOGGER.info("[DRY-RUN] admin.enableTable(" + tableName.getNameWithNamespaceInclAsString() + ")");
        } else {
            admin.enableTable(tableName);
        }
    }

    private void createSnapshot(Admin admin, TableName tableName, String snapshotName, boolean dryRun) throws IOException {
        LOGGER.info("Create snapshot '" + snapshotName + "' from table " + tableName.getNameAsString());
        if (dryRun) {
            LOGGER.info("[DRY-RUN] admin.snapshot('{}', '{}');", snapshotName, tableName);
        } else {
            admin.snapshot(snapshotName, tableName);
        }
    }

    private void cloneSnapshot(Admin admin, String snapshot, String table, boolean dryRun) throws IOException {
        LOGGER.info("Clone snapshot '" + snapshot + "' into table " + table);
        System.out.println("clone_snapshot '" + snapshot + "' , '" + table + "'");
        if (dryRun) {
            LOGGER.info("[DRY-RUN] admin.cloneSnapshot('{}', TableName.valueOf('{}'));", snapshot, table);
        } else {
            admin.cloneSnapshot(snapshot, TableName.valueOf(table));
        }
    }

    private void deleteSnapshot(Admin admin, String snapshotName, boolean dryRun) throws IOException {
        LOGGER.info("Delete snapshot '" + snapshotName + "'");
        System.out.println("delete_snapshot '" + snapshotName + "'");
        if (dryRun) {
            LOGGER.info("[DRY-RUN] admin.deleteSnapshot('{}');", snapshotName);
        } else {
            admin.deleteSnapshot(snapshotName);
        }
    }

    private List<TableName> listTables(String tableNameFilter, boolean failOnOthers, TableState.State... expectedStates)
            throws IOException {
        Connection connection = hBaseManager.getConnection();
        Map<TableState.State, List<TableName>> tables = new HashMap<>();
        List<TableName> allTableNames = listTables(tableNameFilter);
        for (TableName tableName : allTableNames) {
            TableState tableState = MetaTableAccessor.getTableState(connection, tableName);
            tables.computeIfAbsent(tableState.getState(), k -> new LinkedList<>()).add(tableName);
        }
        List<TableName> expectedTables =new LinkedList<>();
        for (TableState.State expectedState : expectedStates) {
            List<TableName> list = tables.remove(expectedState);
            if (list != null) {
                expectedTables.addAll(list);
            }
        }

        if (!tables.isEmpty()) {
            LOGGER.warn("Found tables in non expected status.");
            for (Map.Entry<TableState.State, List<TableName>> entry : tables.entrySet()) {
                LOGGER.warn(" - {} : {} tables : ", entry.getKey(),  entry.getValue().size());
                for (TableName tableName : entry.getValue()) {
                    LOGGER.warn("   - {}", tableName.getNameWithNamespaceInclAsString());
                }
            }
            if (failOnOthers) {
                throw new RuntimeException("Found tables in non expected status.");
            }
        }
        return expectedTables;
    }
}
