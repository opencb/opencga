package org.opencb.opencga.storage.hadoop.app;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.annotation.pending.AnnotationPendingVariantsManager;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsManager;
import org.opencb.opencga.storage.hadoop.variant.prune.SecondaryIndexPrunePendingVariantsManager;
import org.opencb.opencga.storage.hadoop.variant.search.SecondaryIndexPendingVariantsManager;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.io.IOException;

public class PendingVariantsMain extends AbstractMain {

    public static void main(String[] args) {
        PendingVariantsMain main = new PendingVariantsMain();
        try {
            main.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        String command = safeArg(args, 0, "help");
        String tableName = safeArg(args, 1);

        final HBaseManager hBaseManager;
        if (command.equals("help")) {
            hBaseManager = null;
        } else {
            hBaseManager = new HBaseManager(configuration);
        }

        ObjectMap argsMap = getArgsMap(args, 2);
        if (argsMap.getBoolean("help", false)) {
            command = "help";
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        switch (command) {
            case "list-tables":
            case "list":
            case "tables": {
                print(hBaseManager.listTables()
                        .stream()
                        .map(TableName::getNameAsString)
                        .filter(t -> HBaseVariantTableNameGenerator.isValidPendingSecondaryIndexTableName(t)
                                || HBaseVariantTableNameGenerator.isValidPendingAnnotationTableName(t)
                                || HBaseVariantTableNameGenerator.isValidPendingSecondaryIndexPruneTableName(t))
                );
                break;
            }
            case "count": {
                try (VariantDBIterator iterator = getPendingVariantsManager(hBaseManager, tableName).iterator(new Query(argsMap))) {
                    int count = 0;
                    while (iterator.hasNext()) {
                        iterator.next();
                        count++;
                    }
                    print(count);
                }
                break;
            }
            case "query": {
                try (VariantDBIterator iterator = getPendingVariantsManager(hBaseManager, tableName).iterator(new Query(argsMap))) {
                    while (iterator.hasNext()) {
                        println(iterator.next().toString());
                    }
                }
                break;
            }
            case "compact": {
                hBaseManager.act(tableName, (table, admin) -> {
                    admin.majorCompact(table.getName());
                    return null;
                });
                println("Compacting!");
                break;
            }
            case "help":
            default:
                System.out.println("Commands:");
                System.out.println("  help");
                System.out.println("  list-tables");
                System.out.println("  count           <tableName> [--region <region>] ...");
                System.out.println("  query           <tableName> [--region <region>] ...");
                break;
        }
        System.err.println("--------------------------");
        System.err.println("  Wall time: " + TimeUtils.durationToString(stopWatch));
        System.err.println("--------------------------");
        if (hBaseManager != null) {
            hBaseManager.close();
        }
    }

    private PendingVariantsManager getPendingVariantsManager(HBaseManager hBaseManager, String table) throws IOException {
        if (HBaseVariantTableNameGenerator.isValidPendingAnnotationTableName(table)) {
            String dbName = HBaseVariantTableNameGenerator.getDBNameFromPendingAnnotationTableName(table);
            HBaseVariantTableNameGenerator tableNameGenerator = new HBaseVariantTableNameGenerator(dbName, hBaseManager.getConf());
            System.err.println("Detect AnnotationPendingVariants table");
            return new AnnotationPendingVariantsManager(hBaseManager, tableNameGenerator);
        } else if (HBaseVariantTableNameGenerator.isValidPendingSecondaryIndexTableName(table)) {
            String dbName = HBaseVariantTableNameGenerator.getDBNameFromPendingSecondaryIndexTableName(table);
            HBaseVariantTableNameGenerator tableNameGenerator = new HBaseVariantTableNameGenerator(dbName, hBaseManager.getConf());
            System.err.println("Detect SecondaryIndexPendingVariants table");
            return new SecondaryIndexPendingVariantsManager(
                    new VariantHadoopDBAdaptor(hBaseManager, hBaseManager.getConf(), tableNameGenerator, new ObjectMap()));
        } else if (HBaseVariantTableNameGenerator.isValidPendingSecondaryIndexPruneTableName(table)) {
            String dbName = HBaseVariantTableNameGenerator.getDBNameFromPendingSecondaryIndexPruneTableName(table);
            HBaseVariantTableNameGenerator tableNameGenerator = new HBaseVariantTableNameGenerator(dbName, hBaseManager.getConf());
            System.err.println("Detect SecondaryIndexPendingPruneVariants table");
            return new SecondaryIndexPrunePendingVariantsManager(
                    new VariantHadoopDBAdaptor(hBaseManager, hBaseManager.getConf(), tableNameGenerator, new ObjectMap()));
        } else {
            throw new IllegalArgumentIOException("Table '" + table + "' is not a pendig vairants table");
        }
    }


}
