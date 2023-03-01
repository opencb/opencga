package org.opencb.opencga.storage.hadoop.app;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.schema.PTableType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchemaManager;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class PhoenixMain extends AbstractMain {

    protected static final Logger LOGGER = LoggerFactory.getLogger(PhoenixMain.class);
    public static void main(String[] args) {
        PhoenixMain main = new PhoenixMain();
        try {
            main.run(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void run(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        String command = safeArg(args, 0, "help");
        String dbName = safeArg(args, 1);

        final HBaseManager hBaseManager;
        VariantHadoopDBAdaptor dbAdaptor = null;

        ObjectMap argsMap = getArgsMap(args, 2);
        if (argsMap.getBoolean("help", false)) {
            command = "help";
        }
        if (command.equals("help")) {
            hBaseManager = null;
        } else if (command.equals("list-tables")) {
            hBaseManager = new HBaseManager(configuration);
            hBaseManager.getConnection();
        } else {
            hBaseManager = new HBaseManager(configuration);
            hBaseManager.getConnection();
            HBaseVariantTableNameGenerator tableNameGenerator = new HBaseVariantTableNameGenerator(dbName, configuration);
            dbAdaptor = new VariantHadoopDBAdaptor(hBaseManager, configuration, tableNameGenerator, new ObjectMap());
            if (!hBaseManager.tableExists(tableNameGenerator.getMetaTableName())) {
                throw new IllegalArgumentException("Metadata table '" + tableNameGenerator.getMetaTableName() + "' does not exist");
            }
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        switch (command) {
            case "list-tables": {
                print(hBaseManager.listTables()
                        .stream()
                        .map(TableName::getNameAsString)
                        .filter(HBaseVariantTableNameGenerator::isVariantTableName)
                );
                break;
            }
            case "create-view":
                createView(dbAdaptor);
                break;

            case "list-columns":
                listColumnsPhoenix(dbAdaptor);
                break;
//            case "list-columns-hb":
//                listColumnsHBase(dbAdaptor);
//                break;

            case "help":
            default:
                System.out.println("Commands:");
                System.out.println("  help");
                System.out.println("  list-tables");
                System.out.println("  list-columns      <databaseName> ");
//                System.out.println("  list-columns-hb   <databaseName> ");
                System.out.println("  create-view       <databaseName> ");
                break;
        }
        System.err.println("--------------------------");
        System.err.println("  Wall time: " + TimeUtils.durationToString(stopWatch));
        System.err.println("--------------------------");
        if (hBaseManager != null) {
            hBaseManager.close();
        }
    }

    private void listColumnsPhoenix(VariantHadoopDBAdaptor dbAdaptor) throws SQLException {
        println("# Print columns from Phoenix");
        try (Connection connection = dbAdaptor.openJdbcConnection()) {
            for (PhoenixHelper.Column column : new PhoenixHelper(dbAdaptor.getConfiguration())
                    .getColumns(connection, dbAdaptor.getVariantTable(), PTableType.VIEW)) {
                println(column.fullColumn() + "\t" + column.sqlType());
            }
        }
    }

//    private void listColumnsHBase(VariantHadoopDBAdaptor dbAdaptor) throws IOException {
//        println("# Print columns from HBase");
//        for (PhoenixHelper.Column column : new PhoenixHelper(dbAdaptor.getConfiguration()).getColumns(dbAdaptor.getHBaseManager(),
//        dbAdaptor.getVariantTable(), PTableType.VIEW)) {
//            println(column.fullColumn() + "\t" + column.sqlType());
//        }
//    }

    private void createView(VariantHadoopDBAdaptor dbAdaptor) throws StorageEngineException, IOException {
        if (!dbAdaptor.getHBaseManager().tableExists(dbAdaptor.getVariantTable())) {
            throw new IllegalStateException("Variants table '" + dbAdaptor.getVariantTable() + "' doesn't exist");
        }
        if (!dbAdaptor.getHBaseManager().tableExists(dbAdaptor.getTableNameGenerator().getMetaTableName())) {
            throw new IllegalStateException("Meta table '" + dbAdaptor.getTableNameGenerator().getMetaTableName() + "' doesn't exist");
        }

        VariantPhoenixSchemaManager schemaManager = new VariantPhoenixSchemaManager(dbAdaptor);

        schemaManager.registerAnnotationColumns();

        for (Map.Entry<String, Integer> entry : dbAdaptor.getMetadataManager().getStudies().entrySet()) {
            String studyName = entry.getKey();
            Integer studyId = entry.getValue();
            LOGGER.info("Create columns for study {}:{}", studyName, studyId);

            schemaManager.registerStudyColumns(studyId);
        }


    }


}
