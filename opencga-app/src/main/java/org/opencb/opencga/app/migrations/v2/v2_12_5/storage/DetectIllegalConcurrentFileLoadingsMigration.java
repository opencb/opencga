package org.opencb.opencga.app.migrations.v2.v2_12_5.storage;

import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.common.IndexStatus;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileInternalVariantIndex;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.file.VariantIndexStatus;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Migration(id = "illegal_concurrent_file_loadings" ,
        description = "Detect illegal concurrent file loadings and fix them by setting 'status' to 'INVALID' or 'READY'",
        version = "2.12.5",
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
        boolean repeatMigration = params.getBoolean("repeat-migration");

        logger.info("Checking study '{}'", study);
        int studyId = metadataManager.getStudyId(study);

        Set<Set<Integer>> fileSets = getFileWithSharedSamples(engine, studyId);
        Set<Integer> fileIds = fileSets.stream().flatMap(Collection::stream).collect(Collectors.toSet());

        if (fileSets.isEmpty()) {
            logger.info("No concurrent file loadings found in study '{}'", study);
            return;
        } else {
            logger.info("Found {} sets of files with shared samples in study '{}'", fileSets.size(), study);
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

        for (Set<Integer> fileSet : fileSets) {
            Set<Integer> affectedFiles = new HashSet<>();
            Set<Integer> affectedSamples = new HashSet<>();
            Set<Integer> invalidFiles = new HashSet<>();
            Set<Integer> invalidSampleIndexes = new HashSet<>();

            // Check if any task from this file set overlaps in time
            List<TaskMetadata> tasks = new ArrayList<>();
            for (Integer fileId : fileSet) {
                TaskMetadata task = fileTasks.get(fileId);
                if (task != null) {
                    tasks.add(task);
                }
            }
            if (tasks.size() <= 1) {
                continue;
            } else {
                logger.info("--------------------");
                logger.info("Found {} tasks loading files {} in study {}", tasks.size(), fileSet, study);
                for (int i = 0; i < tasks.size(); i++) {
                    TaskMetadata task1 = tasks.get(i);
                    Date task1start = task1.getStatus().firstKey();
                    Date task1end = task1.getStatus().lastKey();
                    for (int f = i + 1; f < tasks.size(); f++) {
                        TaskMetadata task2 = tasks.get(f);
                        Date task2start = task2.getStatus().firstKey();
                        Date task2end = task2.getStatus().lastKey();
                        if (task1start.before(task2end) && task1end.after(task2start)) {
                            affectedFiles.addAll(fileSet);

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
                } else if (sampleMetadata.getSampleIndexStatus(Optional.ofNullable(sampleMetadata.getSampleIndexVersion()).orElse(-1)) == TaskMetadata.Status.READY) {
                    for (Integer fileId : sampleMetadata.getFiles()) {
                        if (affectedFiles.contains(fileId)) {
                            FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, fileId);
                            String fileName = fileMetadata.getName();

                            long actualCount = engine.get(new VariantQuery().study(study).sample(sampleName).file(fileName),
                                            new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.COUNT, true))
                                    .getNumMatches();

                            File catalogFile = catalogManager.getFileManager().search(study,
                                    new Query(FileDBAdaptor.QueryParams.URI.key(), UriUtils.toUri(fileMetadata.getPath())),
                                    new QueryOptions(), token).first();
                            if (catalogFile == null) {
                                logger.warn("File '{}'({}) not found in catalog", fileName, fileId);
                                logger.warn("Sample '{}'({}) invalidated, as file '{}'({}) is not found in catalog", sampleName, sampleId, fileName, fileId);
                                logger.info(" - Invalidating sample index for sample '{}'({})", sampleName, sampleId);
                                invalidSampleIndexes.add(sampleId);
                                continue;
                            }
                            if (fileMetadata.getSamples().size() == 1 && catalogFile.getSampleIds().size() == 1) {
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
                                            sampleName, sampleId, expectedCount, fileName, fileId, actualCount);
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
                    logger.info("Sample '{}'({}) sample index is not in READY status. Invalidate to ensure rebuild", sampleName, sampleId);
                    logger.info(" - Invalidating sample index for sample '{}'({})", sampleName, sampleId);
                    invalidSampleIndexes.add(sampleId);
                }
            }

            if (params.getBoolean("dry-run")) {
                if (invalidFiles.isEmpty() && invalidSampleIndexes.isEmpty()) {
                    logger.info("Dry-run mode. No files or samples to invalidate");
                } else {
                    logger.info("Dry-run mode. Skipping invalidation of files and samples");

                    Set<Integer> invalidSamples = new HashSet<>();
                    for (Integer fileId : invalidFiles) {
                        invalidSamples.addAll(metadataManager.getSampleIdsFromFileId(studyId, fileId));
                    }

                    logger.info("Study '{}'", study);
                    List<Pair<String, Integer>> invalidFilesPairs = invalidFiles.stream()
                            .map(fileId -> Pair.of(metadataManager.getFileName(studyId, fileId), fileId))
                            .collect(Collectors.toList());
                    logger.info("Affected files: {}", invalidFilesPairs);
                    List<Pair<String, Integer>> invalidSamplesPairs = invalidSamples.stream()
                            .map(sampleId -> Pair.of(metadataManager.getSampleName(studyId, sampleId), sampleId))
                            .collect(Collectors.toList());
                    logger.info("Affected samples: {}", invalidSamplesPairs);
                    List<Pair<String, Integer>> invalidSampleIndexesPairs = invalidSampleIndexes.stream()
                            .map(sampleId -> Pair.of(metadataManager.getSampleName(studyId, sampleId), sampleId))
                            .collect(Collectors.toList());
                    logger.info("Affected sample indexes: {}", invalidSampleIndexesPairs);
                }
            } else {
                ObjectMap event = new ObjectMap()
                        .append("patch", getAnnotation().patch())
                        .append("description", "affected_invalid_sample")
                        .append("dateStr", TimeUtils.getTime())
                        .append("date", Date.from(Instant.now()));
                for (Integer sampleId : invalidSampleIndexes) {
                    invalidateSecondarySampleIndex(study, sampleId, event, metadataManager, studyId, repeatMigration);
                }
                Set<Integer> invalidSamples = new HashSet<>();
                for (Integer fileId : invalidFiles) {
                    invalidateFileIndex(study, fileId, event, metadataManager, studyId, invalidSamples, repeatMigration);
                }
                for (Integer sampleId : invalidSamples) {
                    invalidateSampleIndex(study, sampleId, event, metadataManager, studyId, repeatMigration);
                }

                Set<Integer> allSampleIds = new HashSet<>();
                allSampleIds.addAll(affectedSamples);
                allSampleIds.addAll(invalidSamples);
                List<String> allSampleNames = allSampleIds.stream()
                        .map(sampleId -> metadataManager.getSampleName(studyId, sampleId))
                        .collect(Collectors.toList());

                if (!allSampleNames.isEmpty()) {
                    new CatalogStorageMetadataSynchronizer(catalogManager, metadataManager)
                            .synchronizeCatalogSamplesFromStorage(study, allSampleNames, token);
                }

            }
        }
    }

    private void invalidateFileIndex(String study, Integer fileId, ObjectMap event, VariantStorageMetadataManager metadataManager, int studyId, Set<Integer> invalidSamples, boolean repeatMigration) throws StorageEngineException, CatalogException {
        ObjectMap thisEvent = new ObjectMap(event);
        String filePath = metadataManager.updateFileMetadata(studyId, fileId, fileMetadata -> {
            invalidSamples.addAll(fileMetadata.getSamples());
            if (fileMetadata.getAttributes().containsKey("TASK-6078") && !repeatMigration) {
                logger.info("File '{}'({}) already has the attribute 'TASK-6078'. Skip",
                        fileMetadata.getName(), fileMetadata.getId());
            } else {
                Map<String, TaskMetadata.Status> oldStatus = new HashMap<>(fileMetadata.getStatus());
                fileMetadata.setIndexStatus(TaskMetadata.Status.INVALID);
                fileMetadata.getAttributes().put("TASK-6078", thisEvent.append("oldStatus", oldStatus));
            }
        }).getPath();
        String fileUri = Paths.get(filePath).toUri().toString();
        Query query = new Query(FileDBAdaptor.QueryParams.URI.key(), fileUri);
        File file = catalogManager.getFileManager()
                .search(study, query, new QueryOptions(), token)
                .first();
        catalogManager.getFileManager().update(study, file.getId(),
                new FileUpdateParams().setAttributes(new ObjectMap("TASK-6078", thisEvent)), QueryOptions.empty(), token);
        catalogManager.getFileManager().updateFileInternalVariantIndex(study, file, new FileInternalVariantIndex()
                .setStatus(new VariantIndexStatus(IndexStatus.INVALID, "Invalid status - TASK-6078 - affected_invalid_sample - "
                        + "File must be deleted and then indexed")), token);
    }

    private void invalidateSecondarySampleIndex(String study, Integer sampleId, ObjectMap event, VariantStorageMetadataManager metadataManager, int studyId, boolean repeatMigration) throws StorageEngineException, CatalogException {
        ObjectMap thisEvent = new ObjectMap(event);
        String sampleName = metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
            if (sampleMetadata.getAttributes().containsKey("TASK-6078") && !repeatMigration) {
                logger.info("Sample '{}'({}) already has the attribute 'TASK-6078'. Skip",
                        sampleMetadata.getName(), sampleMetadata.getId());
            } else {
                Map<String, TaskMetadata.Status> oldStatus = new HashMap<>(sampleMetadata.getStatus());
                Map<String, Object> oldAttributes = new HashMap<>(sampleMetadata.getAttributes());

                for (Integer v : sampleMetadata.getSampleIndexVersions()) {
                    sampleMetadata.setSampleIndexStatus(TaskMetadata.Status.NONE, v);
                }
                for (Integer v : sampleMetadata.getSampleIndexAnnotationVersions()) {
                    sampleMetadata.setSampleIndexAnnotationStatus(TaskMetadata.Status.NONE, v);
                }
                for (Integer v : sampleMetadata.getFamilyIndexVersions()) {
                    sampleMetadata.setFamilyIndexStatus(TaskMetadata.Status.NONE, v);
                }

                sampleMetadata.setIndexStatus(TaskMetadata.Status.INVALID);
                thisEvent.append("oldStatus", oldStatus);
                thisEvent.append("oldAttributes", oldAttributes);
                thisEvent.append("newStatus", sampleMetadata.getStatus());
                sampleMetadata.getAttributes().put("TASK-6078", thisEvent);
            }
        }).getName();
        catalogManager.getSampleManager().update(study, sampleName,
                new SampleUpdateParams().setAttributes(new ObjectMap("TASK-6078", thisEvent)), QueryOptions.empty(), token);
    }

    private void invalidateSampleIndex(String study, Integer sampleId, ObjectMap event, VariantStorageMetadataManager metadataManager, int studyId, boolean repeatMigration) throws StorageEngineException, CatalogException {
        ObjectMap thisEvent = new ObjectMap(event);
        String sampleName = metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
            if (sampleMetadata.getAttributes().containsKey("TASK-6078") && !repeatMigration) {
                logger.info("Sample '{}'({}) already has the attribute 'TASK-6078'. Skip",
                        sampleMetadata.getName(), sampleMetadata.getId());
            } else {
                Map<String, TaskMetadata.Status> oldStatus = new HashMap<>(sampleMetadata.getStatus());
                Map<String, Object> oldAttributes = new HashMap<>(sampleMetadata.getAttributes());
                sampleMetadata.setIndexStatus(TaskMetadata.Status.INVALID);

                for (Integer v : sampleMetadata.getSampleIndexVersions()) {
                    sampleMetadata.setSampleIndexStatus(TaskMetadata.Status.NONE, v);
                }
                for (Integer v : sampleMetadata.getSampleIndexAnnotationVersions()) {
                    sampleMetadata.setSampleIndexAnnotationStatus(TaskMetadata.Status.NONE, v);
                }
                for (Integer v : sampleMetadata.getFamilyIndexVersions()) {
                    sampleMetadata.setFamilyIndexStatus(TaskMetadata.Status.NONE, v);
                }
                thisEvent.append("oldStatus", oldStatus);
                thisEvent.append("oldAttributes", oldAttributes);
                thisEvent.append("newStatus", sampleMetadata.getStatus());
                sampleMetadata.getAttributes().put("TASK-6078", thisEvent);
            }
        }).getName();
        catalogManager.getSampleManager().update(study, sampleName,
                new SampleUpdateParams().setAttributes(new ObjectMap("TASK-6078", thisEvent)), QueryOptions.empty(), token);
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
