package org.opencb.opencga.storage.hadoop.app;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
                    "[--prefix {databasePrefix}] : List variant storage engine databases", this::databases);
            addSubCommand(Arrays.asList("tables", "table"),
                    "--dbName <DB_NAME> [--excludeArchive] : List tables", this::tables);
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
            ObjectMap map = getArgsMap(args, 0, "prefix");
            String prefix = map.getString("prefix");
            List<String> dbNames = new LinkedList<>();
            Pattern pattern;
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

        private void tables(String[] args) throws IOException {
            ObjectMap map = getArgsMap(args, 0, "dbName", "excludeArchive");
            String dbName = map.getString("dbName");
            if (StringUtils.isEmpty(dbName)) {
                throw new IllegalArgumentException("Missing --dnName");
            }
            LOGGER.info("args : " + map.toJson());
            boolean excludeArchive = map.getBoolean("excludeArchive");

            List<String> tables = new LinkedList<>();
            for (TableName tableName : hBaseManager.getConnection().getAdmin().listTableNames(Pattern.compile(dbName + ".*"))) {
                if (HBaseVariantTableNameGenerator.isValidArchiveTableName(tableName.getNameAsString())) {
                    if (excludeArchive) {
                        LOGGER.info("Exclude archive table '{}'", tableName.getNameWithNamespaceInclAsString());
                    } else {
                        tables.add(tableName.getNameWithNamespaceInclAsString());
                    }
                } else {
                    tables.add(tableName.getNameWithNamespaceInclAsString());
                }
            }
            print(tables);
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
