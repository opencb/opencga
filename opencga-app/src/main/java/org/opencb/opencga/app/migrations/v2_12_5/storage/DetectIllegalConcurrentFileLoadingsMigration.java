package org.opencb.opencga.app.migrations.v2_12_5.storage;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Migration(id = "illegal_concurrent_file_loadings" ,
        description = "Detect illegal concurrent file loadings and fix them by setting 'status' to 'INVALID' or 'READY'",
        version = "2.12.5",
        manual = true,
        domain = Migration.MigrationDomain.STORAGE,
        language = Migration.MigrationLanguage.JAVA,
        date = 20240424
)
public class DetectIllegalConcurrentFileLoadingsMigration extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
        for (String project : getVariantStorageProjects()) {
            VariantStorageEngine engine = getVariantStorageEngineByProject(project);
            if (!engine.getStorageEngineId().equals("hadoop")) {
                continue;
            }
            logger.info("Checking project '{}'", project);
            for (String study : engine.getMetadataManager().getStudyNames()) {
                checkStudy(engine, study);
            }
        }
    }

    private void checkStudy(VariantStorageEngine engine, String study) throws StorageEngineException, CatalogException {
        VariantStorageMetadataManager metadataManager = engine.getMetadataManager();

        logger.info("Checking study '{}'", study);
        int studyId = metadataManager.getStudyId(study);

        Set<Set<Integer>> fileSets = getFileWithSharedSamples(engine, studyId);
        Set<Integer> fileIds = fileSets.stream().flatMap(Collection::stream).collect(Collectors.toSet());

        if (fileSets.isEmpty()) {
            logger.info("No concurrent file loadings found in study '{}'", study);
            return;
        }

        Map<Integer, TaskMetadata> fileTasks = new HashMap<>();
        for (TaskMetadata taskMetadata : metadataManager.taskIterable(studyId)) {
            if (taskMetadata.getType() == TaskMetadata.Type.LOAD) {
                for (Integer fileId : taskMetadata.getFileIds()) {
                    if (fileIds.contains(fileId)) {
                        TaskMetadata old = fileTasks.put(fileId, taskMetadata);
                        if (old != null) {
                            throw new IllegalStateException("File '" + fileId + "' is being loaded by more than one task."
                                    + " Tasks '" + old.getName() + "'(" + old.getId() + ") and"
                                    + " '" + taskMetadata.getName() + "'(" + taskMetadata.getId() + ")");
                        }
                    }
                }
            }
        }

        Set<Set<Integer>> fileSetsToInvalidate = new HashSet<>();
        Set<Integer> affectedFiles = new HashSet<>();
        Set<Integer> affectedSamples = new HashSet<>();
        for (Set<Integer> fileSet : fileSets) {
            // Check if any task from this file set overlaps in time
            List<TaskMetadata> tasks = new ArrayList<>();
            for (Integer fileId : fileSet) {
                TaskMetadata task = fileTasks.get(fileId);
                if (task != null) {
                    tasks.add(task);
                }
            }
            if (tasks.size() > 1) {
                logger.info("Found {} tasks loading files {}", tasks.size(), fileSet);
                for (int i = 0; i < tasks.size(); i++) {
                    TaskMetadata task1 = tasks.get(i);
                    Date task1start = task1.getStatus().firstKey();
                    Date task1end = task1.getStatus().lastKey();
                    for (int f = i + 1; f < tasks.size(); f++) {
                        TaskMetadata task2 = tasks.get(f);
                        Date task2start = task2.getStatus().firstKey();
                        Date task2end = task2.getStatus().lastKey();
                        if (task1start.before(task2end) && task1end.after(task2start)) {
                            fileSetsToInvalidate.add(fileSet);
                            affectedFiles.addAll(task1.getFileIds());

                            List<String> task1Files = task1.getFileIds().stream().map(fileId -> "'" + metadataManager.getFileName(studyId, fileId) + "'(" + fileId + ")").collect(Collectors.toList());
                            List<String> task2Files = task2.getFileIds().stream().map(fileId -> "'" + metadataManager.getFileName(studyId, fileId) + "'(" + fileId + ")").collect(Collectors.toList());

                            logger.info("Tasks '{}'({}) and '{}'({}) overlap in time", task1.getName(), task1.getId(), task2.getName(), task2.getId());
                            logger.info("Task1: {} - {} loading files {}", task1start, task1end, task1Files);
                            logger.info("Task2: {} - {} loading files {}", task2start, task2end, task2Files);
                            Set<Integer> task1Samples = task1.getFileIds().stream().flatMap(file -> metadataManager.getSampleIdsFromFileId(studyId, file).stream()).collect(Collectors.toSet());
                            Set<Integer> task2Samples = task2.getFileIds().stream().flatMap(file -> metadataManager.getSampleIdsFromFileId(studyId, file).stream()).collect(Collectors.toSet());
                            for (Integer task1Sample : task1Samples) {
                                if (task2Samples.contains(task1Sample)) {
                                    String sampleName = metadataManager.getSampleName(studyId, task1Sample);
                                    affectedSamples.add(task1Sample);
                                    logger.info("Sample '{}'({}) is shared between tasks", sampleName, task1Sample);
                                }
                            }
                        }
                    }
                }
            }

            Set<Integer> invalidFiles = new HashSet<>();
            List<Integer> invalidSampleIndexes = new ArrayList<>();
            for (Integer sampleId : affectedSamples) {
                String sampleName = metadataManager.getSampleName(studyId, sampleId);
                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
                if (!sampleMetadata.isMultiFileSample()) {
                    logger.warn("Sample '{}'({}) is not a multi-file sample but has multiple files loaded", sampleName, sampleId);
                    for (Integer file : sampleMetadata.getFiles()) {
                        if (metadataManager.isFileIndexed(studyId, file)) {
                            invalidFiles.add(file);
                            logger.info(" - Invalidating file '{}'({}). Must be deleted and then indexed.",
                                    metadataManager.getFileName(studyId, file), file);
                        }
                    }
                } else if (sampleMetadata.getSampleIndexStatus(sampleMetadata.getSampleIndexVersion()) == TaskMetadata.Status.READY) {
                    for (Integer fileId : sampleMetadata.getFiles()) {
                        if (affectedFiles.contains(fileId)) {
                            FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, fileId);
                            String fileName = fileMetadata.getName();

                            long actualCount = engine.get(new VariantQuery().study(study).sample(sampleName).file(fileName),
                                            new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.COUNT, true))
                                    .getNumMatches();

                            File catalogFile = catalogManager.getFileManager().search(study,
                                    new Query(FileDBAdaptor.QueryParams.URI.key(), UriUtils.createUriSafe(fileMetadata.getPath())),
                                    new QueryOptions(), token).first();
                            if (catalogFile == null) {
                                logger.warn("File '{}'({}) not found in catalog", fileName, fileId);
                                logger.warn("Sample '{}'({}) invalidated, as file '{}'({}) is not found in catalog", sampleName, sampleId, fileName, fileId);
                                logger.info(" - Invalidating sample index for sample '{}'({})", sampleName, sampleId);
                                invalidSampleIndexes.add(sampleId);
                                continue;
                            }
                            if (fileMetadata.getSamples().size() == 1) {
                                long expectedCount = 0;
                                for (Map.Entry<String, Long> entry : catalogFile.getQualityControl().getVariant()
                                        .getVariantSetMetrics().getGenotypeCount().entrySet()) {
                                    if (GenotypeClass.MAIN_ALT.test(entry.getKey())) {
                                        expectedCount += entry.getValue();
                                    }
                                }
                                if (expectedCount == 0) {
                                    expectedCount = catalogFile.getQualityControl().getVariant().getVariantSetMetrics().getVariantCount();
                                }
                                if (expectedCount != actualCount) {
                                    invalidSampleIndexes.add(sampleId);
                                    logger.warn("Sample '{}'({}) was expected to have {} variants in the sample index of file '{}'({}) but has {}",
                                            sampleName, sampleId, fileName, fileId, expectedCount, actualCount);
                                    logger.info(" - Invalidating sample index for sample '{}'({})", sampleName, sampleId);
                                }
                            } else {
                                Map<String, Object> pipelineResult = (Map<String, Object>) catalogFile.getAttributes().get("storagePipelineResult");
                                long loadedVariants = ((Number) ((Map<String, Object>) pipelineResult.get("loadStats")).get("loadedVariants")).longValue();
                                if (loadedVariants != actualCount) {
                                    invalidSampleIndexes.add(sampleId);
                                    logger.warn("Sample '{}'({}) was expected to have {} variants in the sample index but has {}",
                                            sampleName, sampleId, loadedVariants, actualCount);
                                    logger.info(" - Invalidating sample index for sample '{}'({})", sampleName, sampleId);
                                }
                            }
                        }
                    }
                } else {
                    invalidSampleIndexes.add(sampleId);
                }
            }

            if (params.getBoolean("dry-run")) {
                logger.info("Dry-run mode. Skipping invalidation of files and samples");
            } else {
                ObjectMap event = new ObjectMap()
                        .append("patch", getAnnotation().patch())
                        .append("description", "affected_invalid_sample")
                        .append("dateStr", TimeUtils.getTime())
                        .append("date", Date.from(Instant.now()));
                for (Integer sampleId : invalidSampleIndexes) {
                    metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                        if (sampleMetadata.getAttributes().containsKey("TASK-6078")) {
                            logger.info("Sample '{}'({}) already has the attribute 'TASK-6078'. Skip", sampleMetadata.getName(), sampleMetadata.getId());
                        } else {
                            for (Integer sampleIndexVersion : sampleMetadata.getSampleIndexVersions()) {
                                sampleMetadata.setSampleIndexStatus(TaskMetadata.Status.NONE, sampleIndexVersion);
                            }
                            sampleMetadata.getAttributes().put("TASK-6078", event);
                        }
                    });
                }
                Set<Integer> invalidSamples = new HashSet<>();
                for (Integer fileId : invalidFiles) {
                    metadataManager.updateFileMetadata(studyId, fileId, fileMetadata -> {
                        invalidSamples.addAll(fileMetadata.getSamples());
                        if (fileMetadata.getAttributes().containsKey("TASK-6078")) {
                            logger.info("File '{}'({}) already has the attribute 'TASK-6078'. Skip", fileMetadata.getName(), fileMetadata.getId());
                        } else {
                            fileMetadata.setIndexStatus(TaskMetadata.Status.INVALID);
                            fileMetadata.getAttributes().put("TASK-6078", event);
                        }
                    });
                }
                for (Integer sampleId : invalidSamples) {
                    metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                        if (sampleMetadata.getAttributes().containsKey("TASK-6078")) {
                            logger.info("Sample '{}'({}) already has the attribute 'TASK-6078'. Skip", sampleMetadata.getName(), sampleMetadata.getId());
                        } else {
                            sampleMetadata.setIndexStatus(TaskMetadata.Status.INVALID);
                            sampleMetadata.getAttributes().put("TASK-6078", event);
                        }
                    });
                }
            }
        }
    }

    private Set<Set<Integer>> getFileWithSharedSamples(VariantStorageEngine engine, int studyId) throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = engine.getMetadataManager();

        Set<Set<Integer>> fileSets = new HashSet<>();
        // Check if there are any sample with more than one file
        for (SampleMetadata sampleMetadata : metadataManager.sampleMetadataIterable(studyId)) {
            if (sampleMetadata.getFiles().size() > 1) {
                if (sampleMetadata.getSplitData() == VariantStorageEngine.SplitData.CHROMOSOME || sampleMetadata.getSplitData() == VariantStorageEngine.SplitData.REGION) {
                    logger.debug("Sample '{}' is split by chromosome or region. Skip", sampleMetadata.getName());
                    continue;
                }
                ArrayList<Integer> sampleFileIds = new ArrayList<>(sampleMetadata.getFiles());

                sampleFileIds.removeIf(fileId -> !metadataManager.isFileIndexed(studyId, fileId));

                if (sampleFileIds.size() > 1) {
                    logger.info("Sample '{}' has more than one indexed file with split data '{}'", sampleMetadata.getName(), sampleMetadata.getSplitData());

                    fileSets.add(new HashSet<>(sampleFileIds));
                }
            }
        }
        return fileSets;
    }
}
