package org.opencb.opencga.storage.hadoop.variant.prune;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class HadoopVariantPruneManager {

    private Logger logger = LoggerFactory.getLogger(HadoopVariantPruneManager.class);
    public static final String OPERATION_NAME = "VariantPrune";
    private final HadoopVariantStorageEngine engine;

    public HadoopVariantPruneManager(HadoopVariantStorageEngine engine) {
        this.engine = engine;
    }

    public void prune(boolean dryMode, boolean resume, URI outdir) throws StorageEngineException {
        List<TaskMetadata> tasks = pre(dryMode, resume);
        Thread hook = addHook(tasks);
        try {
            runPrune(dryMode, outdir);
            post(tasks, true);
        } catch (Exception e) {
            try {
                post(tasks, false);
            } catch (Exception e1) {
                e.addSuppressed(e1);
            }
            throw e;
        } finally {
            removeHook(hook);
        }
    }

    private void removeHook(Thread hook) {
        Runtime.getRuntime().removeShutdownHook(hook);
    }

    private Thread addHook(List<TaskMetadata> tasks) {
        Thread hook = new Thread(() -> {
            try {
                post(tasks, false);
            } catch (StorageEngineException e) {
                logger.error("Catch error while running shutdown hook.", e);
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        return hook;
    }

    private void runPrune(boolean dryMode, URI outdir) throws StorageEngineException {

        try {
            String pruneTableName = engine.getDBAdaptor().getTableNameGenerator().getPendingSecondaryIndexPruneTableName();
            new SecondaryIndexPrunePendingVariantsDescriptor()
                    .createTableIfNeeded(pruneTableName, engine.getDBAdaptor().getHBaseManager());
        } catch (IOException e) {
            throw StorageEngineException.ioException(e);
        }

        VariantPruneDriverParams params = new VariantPruneDriverParams().setDryRun(dryMode).setOutput(outdir.toString());

        engine.getMRExecutor().run(VariantPruneDriver.class,
                VariantPruneDriver.buildArgs(engine.getVariantTableName(), params.toObjectMap()),
                "Variant prune on table '" + engine.getVariantTableName() + "'"
        );

        try {
            Path report = Files.list(Paths.get(outdir))
                    .filter(p -> p.getFileName().toString().contains("variant_prune_report"))
                    .findFirst()
                    .orElse(null);
            if (report != null) {
                long count = Files.lines(report).count();
                if (dryMode) {
                    logger.info("Found {} variants to delete", count);
                    checkReportedVariants(report, count);
                } else {
                    logger.info("Deleted {} variants", count);
                }
            } else {
                logger.info("Nothing to delete!");
            }
        } catch (IOException e) {
            throw StorageEngineException.ioException(e);
        }
    }

    private void checkReportedVariants(Path report, long count) throws IOException, StorageEngineException {
        // TODO: If count is too large (e.g. > 10M), do not check all of them

        Iterator<VariantPruneReportRecord> it = Files.lines(report).map(VariantPruneReportRecord::new).iterator();
        ProgressLogger progressLogger = new ProgressLogger("Checking variant to prune", count);
        int batchSize = 100;
        UnmodifiableIterator<List<VariantPruneReportRecord>> batches = Iterators.partition(it, batchSize);
        try (Table table = engine.getDBAdaptor().getHBaseManager().getConnection().getTable(TableName.valueOf(engine.getVariantTableName()))) {
            ParallelTaskRunner<VariantPruneReportRecord, VariantPruneReportRecord> ptr = new ParallelTaskRunner<>(
                    i -> batches.hasNext() ? batches.next() : null,
                    batch -> {
                        List<Get> gets = new ArrayList<>(batch.size());
                        for (VariantPruneReportRecord record : batch) {
                            Get get = new Get(VariantPhoenixKeyFactory.generateVariantRowKey(record.getVariant()));
                            if (record.getType() == VariantPruneReportRecord.Type.PARTIAL) {
                                FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                                for (Integer study : record.getStudies()) {
                                    filter.addFilter(new ColumnPrefixFilter(Bytes.toBytes(VariantPhoenixSchema.buildStudyColumnsPrefix(study))));
                                }
                                get.setFilter(filter);
                            }
                            gets.add(get);
                        }
                        Result[] get = table.get(gets);
                        for (int i = 0; i < get.length; i++) {
                            Result result = get[i];
                            Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow());
                            VariantPruneReportRecord record = batch.get(i);
                            if (!variant.sameGenomicVariant(record.getVariant())) {
                                throw new IllegalStateException("Error checking report! Expected " + record.getVariant() + ", got " + variant);
                            }
                            progressLogger.increment(1, () -> "up to variant " + variant);

                            List<String> columns = new ArrayList<>(result.rawCells().length);
                            List<String> sampleOrFileColumns = new ArrayList<>(result.rawCells().length);
                            for (Cell cell : result.rawCells()) {
                                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                                columns.add(column);
                                if (VariantPhoenixSchema.isSampleDataColumn(column) && VariantPhoenixSchema.isFileColumn(column)) {
                                    sampleOrFileColumns.add(column);
                                }
                            }
                            // TODO: Don't just report, do some checks here
//                            logger.info("Variant : {}, prune type: {} , columns: {} , {}", variant, record.type, columns.size(), columns);
                            if (!sampleOrFileColumns.isEmpty()) {
                                logger.warn("Variant : {}, prune type: {} , columns: {} , {}", variant, record.getType(),
                                        sampleOrFileColumns.size(),
                                        sampleOrFileColumns);
                            }
                        }
                        return batch;
                    },
                    null,
                    ParallelTaskRunner.Config.builder().setBatchSize(batchSize).setNumTasks(8).build()
            );
            try {
                ptr.run();
            } catch (ExecutionException e) {
                throw new StorageEngineException("Error checking variant prune report", e);
            }
//            while (batches.hasNext()) {
//                List<PruneReportRecord> batch = batches.next();
//                List<Get> gets = new ArrayList<>(batch.size());
//                for (PruneReportRecord record : batch) {
//                    Get get = new Get(VariantPhoenixKeyFactory.generateVariantRowKey(record.variant));
//                    if (record.type == PruneReportRecord.Type.PARTIAL) {
//                        FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
//                        for (Integer study : record.studies) {
//                            filter.addFilter(new ColumnPrefixFilter(Bytes.toBytes(VariantPhoenixSchema.buildStudyColumnsPrefix(study))));
//                        }
//                        get.setFilter(filter);
//                    }
//                    gets.add(get);
//                }
//                Result[] get = table.get(gets);
//                for (int i = 0; i < get.length; i++) {
//                    Result result = get[i];
//                    Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow());
//                    PruneReportRecord record = batch.get(i);
//                    if (!variant.sameGenomicVariant(record.variant)) {
//                        throw new IllegalStateException("Error checking report!");
//                    }
//                    List<String> columns = new ArrayList<>(result.rawCells().length);
//                    for (Cell cell : result.rawCells()) {
//                        String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
//                        columns.add(column);
//                    }
//                    logger.info("Variant : {}, prune type: {} , columns: {} , {}", variant, record.type, columns.size(), columns);
//                }
//
//            }

        }
    }

    private List<TaskMetadata> pre(boolean dryMode, boolean resume) throws StorageEngineException {
        VariantStorageMetadataManager mm = engine.getMetadataManager();

        List<TaskMetadata> tasks = new LinkedList<>();
        List<String> studiesWithoutStats = new LinkedList<>();

        // First check no running operations in any study
        for (Integer studyId : mm.getStudies().values()) {
            // Do not allow concurrent operations at all.
            mm.checkTaskCanRun(studyId, OPERATION_NAME, Collections.emptyList(), resume, TaskMetadata.Type.REMOVE, tm -> false);
        }

        // Check that all variant stats are updated
        for (Integer studyId : mm.getStudies().values()) {
            if (!mm.getCohortMetadata(studyId, StudyEntry.DEFAULT_COHORT).isStatsReady()) {
                studiesWithoutStats.add(mm.getStudyName(studyId));
            }
            // FIXME: What if not invalid?
            //   Might happen if some samples were deleted, or when loading split files?
        }
        if (!studiesWithoutStats.isEmpty()) {
            throw new StorageEngineException("Unable to run variant prune operation. "
                    + "Please, run variant stats index on cohort '" + StudyEntry.DEFAULT_COHORT + "' for studies " + studiesWithoutStats);
        }

        // If no dry-mode, add the new tasks
        if (!dryMode) {
            for (Integer studyId : mm.getStudies().values()) {
                // Do not allow concurrent operations at all.
                tasks.add(mm.addRunningTask(studyId, OPERATION_NAME, Collections.emptyList(), resume, TaskMetadata.Type.REMOVE, tm -> false));
            }
        }

        return tasks;
    }

    private void post(List<TaskMetadata> tasks, boolean success) throws StorageEngineException {
        VariantStorageMetadataManager mm = engine.getMetadataManager();
        for (TaskMetadata task : tasks) {
            mm.updateTask(task.getStudyId(), task.getId(), t -> {
                if (success) {
                    t.addStatus(TaskMetadata.Status.READY);
                } else {
                    t.addStatus(TaskMetadata.Status.ERROR);
                }
            });
        }
    }

}
