package org.opencb.opencga.storage.hadoop.variant.prune;

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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.annotation.pending.AnnotationPendingVariantsDescriptor;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsDBCleaner;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchDataDeleter;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class VariantPruneManager {

    public static final int CHECK_DRY_RUN_LIMIT = 1000000;
    private Logger logger = LoggerFactory.getLogger(VariantPruneManager.class);
    public static final String OPERATION_NAME = "VariantPrune";
    private final HadoopVariantStorageEngine engine;

    public VariantPruneManager(HadoopVariantStorageEngine engine) {
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

    private Map<VariantPruneReportRecord.Type, Long> runPrune(boolean dryMode, URI outdir) throws StorageEngineException {

        try {
            if (!dryMode) {
                // Do not create table in dry-mode.
                HBaseVariantTableNameGenerator generator = engine.getDBAdaptor().getTableNameGenerator();
                new SecondaryIndexPrunePendingVariantsDescriptor()
                        .createTableIfNeeded(generator, engine.getDBAdaptor().getHBaseManager());
                new AnnotationPendingVariantsDescriptor()
                        .createTableIfNeeded(generator, engine.getDBAdaptor().getHBaseManager());
            }
        } catch (IOException e) {
            throw StorageEngineException.ioException(e);
        }

        VariantPruneDriverParams params = new VariantPruneDriverParams()
                .setDryRun(dryMode)
                .setOutput(outdir.resolve("variant_prune_report." + TimeUtils.getTime() + ".txt").toString());

        engine.getMRExecutor().run(VariantPruneDriver.class,
                VariantPruneDriver.buildArgs(engine.getVariantTableName(), params.toObjectMap()),
                "Variant prune on table '" + engine.getVariantTableName() + "'"
        );

        Path report;
        try {
            report = Files.list(Paths.get(outdir))
                    .filter(p -> p.getFileName().toString().contains("variant_prune_report"))
                    .findFirst()
                    .orElse(null);
            Map<VariantPruneReportRecord.Type, Long> countByType;
            if (report == null) {
                logger.info("Nothing to delete!");
                countByType = Arrays.stream(VariantPruneReportRecord.Type.values()).collect(Collectors.toMap(k -> k, k -> 0L));
            } else {
                countByType = Files.lines(report)
                        .map(VariantPruneReportRecord::new)
                        .collect(Collectors.groupingBy(
                                VariantPruneReportRecord::getType,
                                Collectors.counting()));
            }

            long totalCount = countByType.values().stream().mapToLong(l -> l).sum();
            if (dryMode) {
                logger.info("Found {} variants to prune, {}", totalCount, countByType);
                checkReportedVariants(report, totalCount);
            } else {
                if (engine.getVariantSearchManager().isAlive(engine.getDBName())) {
                    logger.info("Pruned {} variants, {}", totalCount, countByType);
                    pruneFromSecondaryIndex(countByType.getOrDefault(VariantPruneReportRecord.Type.FULL, 0L));
                    updateSecondaryIndex(countByType.getOrDefault(VariantPruneReportRecord.Type.PARTIAL, 0L));
                }
            }
            return countByType;
        } catch (IOException e) {
            throw StorageEngineException.ioException(e);
        }
    }

    private void checkReportedVariants(Path report, long count) throws IOException, StorageEngineException {

        logger.info("Check dry-run report. Found {} variants to prune", count);
        if (report == null || !report.toFile().exists()) {
            if (count == 0) {
                logger.info("Nothing to check");
                return;
            } else {
                throw new StorageEngineException("Missing variant_prune_report");
            }
        }
        int batchSize = 100;
        int variantsToSkip;
        long variantsToCheck;
        if (count > CHECK_DRY_RUN_LIMIT) {
            logger.warn("Will check only {} out of {} variants", CHECK_DRY_RUN_LIMIT, count);
            variantsToSkip = (int) (count / CHECK_DRY_RUN_LIMIT);
            variantsToCheck = CHECK_DRY_RUN_LIMIT;
        } else {
            variantsToSkip = 0;
            variantsToCheck = count;
        }

        Iterator<VariantPruneReportRecord> it = Files.lines(report).map(VariantPruneReportRecord::new).iterator();
        ProgressLogger progressLogger = new ProgressLogger("Checking variant to prune", variantsToCheck);
        try (Table table = engine.getDBAdaptor().getHBaseManager().getConnection()
                .getTable(TableName.valueOf(engine.getVariantTableName()))) {
            AtomicInteger variantsWithProblems = new AtomicInteger();
            ParallelTaskRunner<VariantPruneReportRecord, VariantPruneReportRecord> ptr = new ParallelTaskRunner<>(
                    i -> {
                        List<VariantPruneReportRecord> records = new ArrayList<>(i);
                        int skippedVariants = 0;
                        while (it.hasNext() && records.size() < i) {
                            VariantPruneReportRecord next = it.next();
                            if (skippedVariants == variantsToSkip) {
                                skippedVariants = 0;
                                records.add(next);
                            } else {
                                skippedVariants++;
                            }
                        }
                        return records;
                    },
                    batch -> {
                        List<Get> gets = new ArrayList<>(batch.size());
                        for (VariantPruneReportRecord record : batch) {
                            Get get = new Get(VariantPhoenixKeyFactory.generateVariantRowKey(record.getVariant()));
                            if (record.getType() == VariantPruneReportRecord.Type.PARTIAL) {
                                FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                                for (Integer study : record.getStudies()) {
                                    filter.addFilter(
                                            new ColumnPrefixFilter(Bytes.toBytes(VariantPhoenixSchema.buildStudyColumnsPrefix(study))));
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
                                throw new IllegalStateException("Error checking report! Expected "
                                        + record.getVariant() + ", got " + variant);
                            }
                            progressLogger.increment(1, () -> "up to variant " + variant);

//                            List<String> columns = new ArrayList<>(result.rawCells().length);
                            List<String> sampleOrFileColumns = new ArrayList<>(result.rawCells().length);
                            for (Cell cell : result.rawCells()) {
                                String column = Bytes.toString(
                                        cell.getQualifierArray(),
                                        cell.getQualifierOffset(),
                                        cell.getQualifierLength());
//                                columns.add(column);
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
                                variantsWithProblems.incrementAndGet();
                            }
                        }
                        return batch;
                    },
                    null,
                    ParallelTaskRunner.Config.builder().setBatchSize(batchSize).setCapacity(2).setNumTasks(4).build()
            );
            try {
                ptr.run();
            } catch (ExecutionException e) {
                throw new StorageEngineException("Error checking variant prune report", e);
            }
            if (variantsWithProblems.get() > 0) {
                throw new StorageEngineException("Error validating variant prune report!"
                        + " Found " + variantsWithProblems.get() + " out of " + variantsToCheck + " checked variants with inconsistencies");
            }
        }
    }

    private void pruneFromSecondaryIndex(long count) throws StorageEngineException {
        logger.info("Deleting {} variants from secondary index", count);
        logger.info("In case of resuming operation, the total number of variants to remove could be larger.");
        SecondaryIndexPrunePendingVariantsManager manager = new SecondaryIndexPrunePendingVariantsManager(engine.getDBAdaptor());

        Task<Variant, Variant> progressTask = new ProgressLogger("Prune variants from secondary index", count)
                .asTask(variant -> "up to variant " + variant);
        PendingVariantsDBCleaner cleaner = manager.cleaner();

        VariantSearchManager searchManager = engine.getVariantSearchManager();
        ParallelTaskRunner<Variant, Variant> ptr = new ParallelTaskRunner<>(
                manager.reader(new Query()),
                progressTask,
                new HadoopVariantSearchDataDeleter(engine.getDBName(), searchManager.getSolrClient(), cleaner),
                ParallelTaskRunner.Config.builder().setNumTasks(1).setBatchSize(searchManager.getInsertBatchSize()).build());

        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error checking variant prune report", e);
        }
    }

    private void updateSecondaryIndex(long count) throws StorageEngineException, IOException {
        logger.info("Updating {} variants from secondary index", count);
        logger.info("In case of resuming operation, the total number of variants to update could be larger.");

        try {
            engine.secondaryIndex();
        } catch (VariantSearchException e) {
            throw new StorageEngineException("Internal search index error", e);
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

        // Discard studies without loaded files.
        // These can't have the stats computed.
        studiesWithoutStats.removeIf(study -> mm.getIndexedFiles(mm.getStudyId(study)).isEmpty());

        if (!studiesWithoutStats.isEmpty()) {
            throw new StorageEngineException("Unable to run variant prune operation. "
                    + "Please, run variant stats index on cohort '" + StudyEntry.DEFAULT_COHORT + "' for studies " + studiesWithoutStats);
        }

        // If no dry-mode, add the new tasks
        if (!dryMode) {
            for (Integer studyId : mm.getStudies().values()) {
                // Do not allow concurrent operations at all.
                tasks.add(mm.addRunningTask(studyId, OPERATION_NAME, Collections.emptyList(), resume,
                        TaskMetadata.Type.REMOVE, tm -> false));
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
