package org.opencb.opencga.storage.hadoop.app;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchemaManager;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

public class VariantEngineUtilsMain extends AbstractMain {

    protected static final Logger LOGGER = LoggerFactory.getLogger(VariantEngineUtilsMain.class);

    public static void main(String[] args) {
        try {
            new VariantEngineUtilsMain().run(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void run(String[] args) throws Exception {
        new VariantEngineUtilsCommandExecutor().exec(args);
    }

    public static class VariantEngineUtilsCommandExecutor extends NestedCommandExecutor {
        private HBaseManager hBaseManager;

        public VariantEngineUtilsCommandExecutor() {
            this("");
        }

        public VariantEngineUtilsCommandExecutor(String context) {
            super(context);
            addSubCommand(Arrays.asList("dbNames", "dbs", "dbnames"),
                    "[--prefix {databasePrefix}] [--namespace {namespace}] : List variant storage engine databases", this::databases);
            addSubCommand(Arrays.asList("tables", "table"),
                    "--dbName <DB_NAME> [--namespace {namespace}] [--excludeArchive] : List tables", this::tables);
            addSubCommand(Arrays.asList("delete"),
                    "--dbName <DB_NAME> [--namespace {namespace}] [--dryRun] : Delete all tables from HBase and Phoenix for a given dbName",
                    this::delete);
        }

        @Override
        protected void setup(String command, String[] args) throws Exception {
            hBaseManager = new HBaseManager(HBaseConfiguration.create());
        }

        @Override
        protected void cleanup(String command, String[] args) throws Exception {
            hBaseManager.close();
        }

        private void databases(String[] args) throws IOException {
            ObjectMap map = getArgsMap(args, 0, "prefix", "namespace");
            String prefix = map.getString("prefix");
            String namespace = checkNamespace(map.getString("namespace"), prefix);
            prefix = removeNamespace(prefix);
            List<String> dbNames = new LinkedList<>();
            Pattern pattern;
            if (StringUtils.isNotEmpty(namespace)) {
                if (prefix == null) {
                    prefix = namespace + ":";
                } else {
                    prefix = namespace + ":" + prefix;
                }
            }
            if (StringUtils.isEmpty(prefix)) {
                pattern = null;
            } else {
                pattern = Pattern.compile(prefix + ".*");
            }
            for (TableName tableName : hBaseManager.getConnection().getAdmin().listTableNames(pattern)) {
                if (HBaseVariantTableNameGenerator.isValidMetaTableName(tableName.getNameWithNamespaceInclAsString())) {
                    dbNames.add(HBaseVariantTableNameGenerator.getDBNameFromMetaTableName(tableName.getNameWithNamespaceInclAsString()));
                }
            }
            print(dbNames);
        }

        private String checkNamespace(String namespace, String prefix) {
            if (prefix == null) {
                return namespace;
            } else if (prefix.contains(":")) {
                String namespaceFromPrefix = prefix.split(":")[0];
                if (StringUtils.isEmpty(namespace)) {
                    return namespaceFromPrefix;
                } else if (namespaceFromPrefix.equals(namespace)) {
                    return namespace;
                } else {
                    throw new IllegalArgumentException("Namespace '" + namespace + "' and from prefix '" + prefix + "' are different."
                    + " Either remove the namespace from the prefix, or use the same value");
                }
            } else {
                return namespace;
            }
        }

        private String removeNamespace(String prefix) {
            if (prefix.contains(":")) {
                String[] split = prefix.split(":", 2);
                return split[1];
            } else {
                return prefix;
            }
        }

        private void tables(String[] args) throws IOException {
            ObjectMap map = getArgsMap(args, "dbName", "namespace", "excludeArchive");
            String dbName = requireArgument("dbName", map);
            String namespace = checkNamespace(map.getString("namespace"), dbName);
            dbName = removeNamespace(dbName);

            boolean excludeArchive = map.getBoolean("excludeArchive");

            List<String> tables = getTables(namespace, dbName, excludeArchive);
            print(tables);
        }

        private List<String> getTables(String namespace, String dbName, boolean excludeArchive) throws IOException {
            List<String> tables = new LinkedList<>();
            Pattern pattern;
            if (StringUtils.isEmpty(namespace)) {
                pattern = Pattern.compile(dbName + "_.*");
            } else {
                pattern = Pattern.compile(namespace + ":" + dbName + "_.*");
            }
            try (Admin admin = hBaseManager.getConnection().getAdmin()) {
                for (TableName tableName : admin.listTableNames(pattern)) {
                    if (HBaseVariantTableNameGenerator.isValidTable(namespace, dbName, tableName.getNameAsString())) {
                        if (HBaseVariantTableNameGenerator.isValidArchiveTableName(dbName, tableName.getNameAsString())) {
                            if (excludeArchive) {
                                LOGGER.info("Exclude archive table '{}'", tableName.getNameWithNamespaceInclAsString());
                            } else {
                                tables.add(tableName.getNameWithNamespaceInclAsString());
                            }
                        } else {
                            tables.add(tableName.getNameWithNamespaceInclAsString());
                        }
                    }
                }
            }
            return tables;
        }

        private void delete(String[] args) throws IOException, SQLException, ClassNotFoundException {
            ObjectMap map = getArgsMap(args, "dbName", "namespace", "dryRun");
            String dbName = requireArgument("dbName", map);
            String namespace = checkNamespace(map.getString("namespace"), dbName);
            dbName = removeNamespace(dbName);
            boolean dryRun = map.getBoolean("dryRun");

            List<String> tables = getTables(namespace, dbName, false);

            try (Admin admin = hBaseManager.getConnection().getAdmin()) {
                // Drop Phoenix View
                for (String table : tables) {
                    if (HBaseVariantTableNameGenerator.isValidVariantsTable(table)) {
                        if (dryRun) {
                            LOGGER.info("[DRY-RUN] drop phoenix view '{}'", table);
                        } else {
                            LOGGER.info("Drop phoenix view '{}'", table);
                            VariantPhoenixSchemaManager.dropTable(hBaseManager, table, true);
                        }
                    }
                }

                // Drop hbase tables
                for (String table : tables) {
                    TableName tableName = TableName.valueOf(table);
                    if (admin.isTableDisabled(tableName)) {
                        LOGGER.info("Table '{}' already disabled", tableName);
                    } else {
                        if (dryRun) {
                            LOGGER.info("[DRY-RUN] - disable hbase table '{}'", tableName);
                        } else {
                            LOGGER.info("Disable hbase table '{}'", tableName);
                            admin.disableTable(tableName);
                        }
                    }
                    if (dryRun) {
                        LOGGER.info("[DRY-RUN] - drop hbase table '{}'", tableName);
                    } else {
                        LOGGER.info("Drop hbase table '{}'", tableName);
                        admin.deleteTable(tableName);
                    }
                }
            }
        }
    }

    private static class HBaseTablesCommandExecutor extends NestedCommandExecutor {
        private HBaseManager hBaseManager;

        HBaseTablesCommandExecutor() {
            addSubCommand("list", "", args -> {
                List<String> tables = new LinkedList<>();
                for (TableName tableName : hBaseManager.getConnection().getAdmin().listTableNames()) {
                    if (HBaseVariantTableNameGenerator.isValidMetaTableName(tableName.getNameWithNamespaceInclAsString())) {
                        tables.add(tableName.getNameWithNamespaceInclAsString());
                    }
                }
                print(tables);
            });
        }

        @Override
        protected void setup(String command, String[] args) throws Exception {
            hBaseManager = new HBaseManager(HBaseConfiguration.create());
        }

        @Override
        protected void cleanup(String command, String[] args) throws Exception {
            hBaseManager.close();
        }
    }
}
