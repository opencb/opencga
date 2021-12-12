package org.opencb.opencga.storage.hadoop.app;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
                Map<String, TreeMap<Date, String>> tablesMap = listSnapshots(filter);
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
                snapshotTables(getArg(args, 1),
                        argsMap.getString("snapshotSuffix", "_SNAPSHOT_" + TimeUtils.getTime()),
                        argsMap.getBoolean("dryRun"), argsMap.getBoolean("skipTablesWithSnapshot"));
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
                System.out.println("      Create a sapshot for all selected tables.");
                break;
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

    private Map<String, TreeMap<Date, String>> listSnapshots(String snapshotFilter) throws Exception {
        try (Admin admin = hBaseManager.getConnection().getAdmin()) {
            Map<String, TreeMap<Date, String>> tablesMap = new TreeMap<>();
            List<?> snapshots = StringUtils.isBlank(snapshotFilter) ? admin.listSnapshots() : admin.listSnapshots(snapshotFilter);
            if (CollectionUtils.isEmpty(snapshots)) {
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

    private void snapshotTables(String tableNamePrefix, String snapshotSuffix, boolean dryRun, boolean skipTablesWithSnapshot)
            throws Exception {
        Connection connection = hBaseManager.getConnection();
        Map<String, TreeMap<Date, String>> tablesWithSnapshots = listSnapshots(tableNamePrefix + ".*");
        try (Admin admin = connection.getAdmin()) {
            for (TableName tableName : listTables(tableNamePrefix + ".*")) {
                if (skipTablesWithSnapshot && tablesWithSnapshots.containsKey(tableName.getNameAsString())) {
                    TreeMap<Date, String> map = tablesWithSnapshots.get(tableName.getNameAsString());
                    LOGGER.info("Skip snapshot from table '{}' . Already has a snapshot: {}", tableName.getNameAsString(), map);
                    continue;
                }
                String snapshotName = tableName + snapshotSuffix;
                LOGGER.info("Create snapshot '" + snapshotName + "' from table " + tableName);
                if (dryRun) {
                    LOGGER.info("admin.snapshot('{}', '{}');", snapshotName, tableName);
                } else {
                    admin.snapshot(snapshotName, tableName);
                }
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
                LOGGER.info("Create snapshot '" + snapshotName + "' from table " + tableName.getNameAsString());
                if (dryRun) {
                    LOGGER.info("admin.snapshot('{}', '{}');", snapshotName, tableName);
                } else {
                    admin.snapshot(snapshotName, tableName);
                }
                LOGGER.info("Clone snapshot '" + snapshotName + "' into table " + newTableName);
                if (dryRun) {
                    LOGGER.info("admin.cloneSnapshot('{}', '{}')", snapshotName, newTableName);
                } else {
                    admin.cloneSnapshot(snapshotName, TableName.valueOf(tableName.getNamespaceAsString(), newTableName));
                }
                if (keepSnapshot) {
                    LOGGER.info("Keep snapshot '" + snapshotName + "'");
                } else {
                    LOGGER.info("Delete snapshot '" + snapshotName + "'");
                    if (dryRun) {
                        LOGGER.info("admin.deleteSnapshot('{}');", snapshotName);
                    } else {
                        admin.deleteSnapshot(snapshotName);
                    }
                }
            }
        }
    }

}
