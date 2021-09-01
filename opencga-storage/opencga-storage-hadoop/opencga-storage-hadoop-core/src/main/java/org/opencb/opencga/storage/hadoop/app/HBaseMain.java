package org.opencb.opencga.storage.hadoop.app;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
//import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
//import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.util.Pair;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class HBaseMain extends AbstractMain {


    public static final String REASSIGN_TABLE_REGIONS = "reassign-table-regions";
    public static final String MOVE_TABLE_REGIONS = "move-table-regions";
    public static final String CHECK_TABLES_WITH_REGIONS_ON_DEAD_SERVERS = "check-tables-with-regions-on-dead-servers";
    public static final String BALANCE_TABLE_REGIONS = "balance-table-regions";
    public static final String LIST_TABLES = "list-tables";
    public static final String REGIONS_PER_TABLE = "regions-per-table";

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
            case REASSIGN_TABLE_REGIONS:
                reasignTableRegions(args);
                break;
            case CHECK_TABLES_WITH_REGIONS_ON_DEAD_SERVERS:
                checkTablesWithRegionsOnDeadServers(args);
                break;
            case MOVE_TABLE_REGIONS:
                moveTableRegions(args);
                break;
            case BALANCE_TABLE_REGIONS:
                balanceTableRegions(getArg(args, 1), getArgsMap(args, 2));
                break;
            case LIST_TABLES:
                print(listTables(getArg(args, 1, "")).stream().map(TableName::getNameWithNamespaceInclAsString).iterator());
                break;
            case REGIONS_PER_TABLE:
                regionsPerTable(getArg(args, 1));
                break;
            case "help":
            default:
                System.out.println("Commands:");
                System.out.println("  " + LIST_TABLES + " [<table-name-regex>]");
                System.out.println("  " + CHECK_TABLES_WITH_REGIONS_ON_DEAD_SERVERS + " [ <table-name-regex> ]");
                System.out.println("  " + REASSIGN_TABLE_REGIONS + " <table-name-regex>");
                System.out.println("  " + MOVE_TABLE_REGIONS + " <table-name-regex>");
                System.out.println("  " + BALANCE_TABLE_REGIONS + " <table-name> [--maxMoves N]");
                System.out.println("  " + REGIONS_PER_TABLE + " <table-name>");
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

    private void reasignTableRegions(String[] args) throws Exception {
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


}
