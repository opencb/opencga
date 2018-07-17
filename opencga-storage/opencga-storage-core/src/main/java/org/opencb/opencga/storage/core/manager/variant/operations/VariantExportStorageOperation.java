/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.manager.variant.operations;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.monitor.executors.AbstractExecutor;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.models.StudyInfo;
import org.opencb.opencga.storage.core.manager.variant.metadata.CatalogVariantMetadataFactory;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantMetadataImporter;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.manager.variant.operations.VariantFileIndexerStorageOperation.DEFAULT_COHORT_DESCRIPTION;

/**
 * Created on 06/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantExportStorageOperation extends StorageOperation {

    public VariantExportStorageOperation(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, StorageEngineFactory.get(storageConfiguration),
                LoggerFactory.getLogger(VariantExportStorageOperation.class));
    }

    public List<URI> exportData(List<StudyInfo> studyInfos, Query query, VariantOutputFormat outputFormat, String outputStr,
                                String sessionId, ObjectMap options)
            throws IOException, StorageEngineException, CatalogException {
        if (options == null) {
            options = new ObjectMap();
        }

        List<URI> newFiles = new ArrayList<>();

        if (studyInfos.isEmpty()) {
            logger.warn("Nothing to do!");
            return Collections.emptyList();
        }
        Thread hook = null;
        URI outputFile = null;
        final Path outdir;
        if (!VariantWriterFactory.isStandardOutput(outputStr)) {
            URI outdirUri = null;
            try {
                outdirUri = UriUtils.createUri(outputStr);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
            String outputFileName = null;
            if (StringUtils.isEmpty(outdirUri.getScheme()) || outdirUri.getScheme().equals("file")) {
                java.io.File file = Paths.get(outdirUri).toFile();
                if (!file.exists() || !file.isDirectory()) {
                    outputFileName = outdirUri.resolve(".").relativize(outdirUri).toString();
                    outdirUri = outdirUri.resolve(".");
                } else {
                    try {
                        outdirUri = UriUtils.createDirectoryUri(outputStr);
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException(e);
                    }
                    List<Region> regions = Region.parseRegions(query.getString(VariantQueryParam.REGION.key()));
                    outputFileName = buildOutputFileName(studyInfos.stream().map(StudyInfo::getStudyFQN).collect(Collectors.toList()),
                            regions);
                }
                outputFile = outdirUri.resolve(outputFileName);
                outdir = Paths.get(outdirUri);

                outdirMustBeEmpty(outdir, options);

                hook = buildHook(outdir);
                writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.RUNNING, "Job has just started"));
                Runtime.getRuntime().addShutdownHook(hook);
            } else {
                outdir = null;
                outputFile = outdirUri;
            }
        } else {
            outdir = null;
        }



        // Up to this point, catalog has not been modified
        try {
            DataStore dataStore = studyInfos.get(0).getDataStores().get(File.Bioformat.VARIANT);
            for (StudyInfo studyInfo : studyInfos) {
                if (!studyInfo.getDataStores().get(File.Bioformat.VARIANT).equals(dataStore)) {
                    throw new StorageEngineException("Unable to export variants from studies in different databases");
                }
            }

//            String outputFileName = buildOutputFileName(Collections.singletonList(study.getAlias()), regions, outputFormatStr);
            String studyFqn = studyInfos.get(0).getStudyFQN();
            String catalogOutDirId = getCatalogOutdirId(studyFqn, options, sessionId);

//            for (StudyInfo studyInfo : studyInfos) {
//                StudyConfiguration studyConfiguration = updateStudyConfiguration(sessionId, studyInfo.getStudyId(), dataStore);
//            }

            VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
            VariantMetadataFactory metadataExporter =
                    new CatalogVariantMetadataFactory(catalogManager, variantStorageEngine.getDBAdaptor(), sessionId);

            variantStorageEngine.exportData(outputFile, outputFormat, metadataExporter, query, new QueryOptions(options));

            if (catalogOutDirId != null && outdir != null) {
                copyResults(outdir, studyFqn, catalogOutDirId, sessionId);
            }
            if (outdir != null) {
                java.io.File[] files = outdir.toFile().listFiles((dir, name) -> !name.equals(AbstractExecutor.JOB_STATUS_FILE));
                if (files != null) {
                    for (java.io.File file : files) {
                        newFiles.add(file.toURI());
                    }
                }
            }

            if (outdir != null) {
                writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.DONE, "Job completed"));
            }
        } catch (Exception e) {
            // Error!
            logger.error("Error exporting variants.", e);
            if (outdir != null) {
                writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.ERROR, "Job with error : " + e.getMessage()));
            }
            throw new StorageEngineException("Error exporting variants.", e);
        } finally {
            // Remove hook
            if (hook != null) {
                Runtime.getRuntime().removeShutdownHook(hook);
            }
        }

        return newFiles;
    }


    public void importData(StudyInfo studyInfo, URI inputUri, String sessionId) throws IOException, StorageEngineException {

        VariantMetadataImporter variantMetadataImporter;
        variantMetadataImporter = new CatalogVariantMetadataImporter(studyInfo.getStudyFQN(), inputUri, sessionId);

        try {
            DataStore dataStore = studyInfo.getDataStores().get(File.Bioformat.VARIANT);
            VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
            ObjectMap options = variantStorageEngine.getOptions();
            VariantMetadata metadata;
            StudyConfiguration studyConfiguration;
            try (StudyConfigurationManager scm = variantStorageEngine.getStudyConfigurationManager()) {
                metadata = variantMetadataImporter.importMetaData(inputUri, scm);
                studyConfiguration = scm.getStudyConfiguration(((int) studyInfo.getStudyUid()), null).first();
            }

            variantStorageEngine.importData(inputUri, metadata, Collections.singletonList(studyConfiguration), options);

        } catch (Exception e) {
            logger.error("Error importing data");
            throw e;
        }

    }


    private String buildOutputFileName(List<String> studyNames, List<Region> regions) {
        String studies = String.join("_", studyNames);
        if (regions == null || regions.size() != 1) {
            return studies + ".export";
        } else {
            return studies + '.' + regions.get(0).toString() + ".export";
        }
    }

    private final class CatalogVariantMetadataImporter extends VariantMetadataImporter {
        private final URI inputUri;
        private final String sessionId;
        private final String studyStr;

        private CatalogVariantMetadataImporter(String studyStr, URI inputUri, String sessionId) {
            this.inputUri = inputUri;
            this.sessionId = sessionId;
            this.studyStr = studyStr;
        }

        @Override
        protected void processStudyConfiguration(StudyConfiguration studyConfiguration) {
//            studyConfiguration.setStudyId((int) studyFqn);

            try {
                // Create Samples
                Map<String, Integer> samplesMap = new HashMap<>();
                Map<Integer, Integer> samplesIdMap = new HashMap<>();
                String source = inputUri.resolve(".").relativize(inputUri).getPath();
                String description = "Sample data imported from " + source;
                for (Map.Entry<String, Integer> entry : studyConfiguration.getSampleIds().entrySet()) {
                    Sample sample = catalogManager.getSampleManager().create(studyStr, entry.getKey(), source, description,
                            null, false, null, new HashMap<>(), Collections.emptyMap(), QueryOptions.empty(), sessionId).first();
                    samplesMap.put(sample.getId(), (int) sample.getUid());
                    samplesIdMap.put(entry.getValue(), (int) sample.getUid());
                }

                // Create cohorts
                Map<String, Integer> newCohortIds = new HashMap<>();
                Map<Integer, Set<Integer>> newCohorts = new HashMap<>();

                for (Integer cohortId : studyConfiguration.getCalculatedStats()) {
                    String cohortName = studyConfiguration.getCohortIds().inverse().get(cohortId);
                    Set<Integer> sampleIds = studyConfiguration.getCohorts().get(cohortId);
                    List<Sample> newSampleList = new ArrayList<>();
                    for (Integer sampleId : sampleIds) {
                        if (samplesIdMap.containsKey(sampleId)) {
                            newSampleList.add(new Sample().setUid(samplesIdMap.get(sampleId)));
                        }
                    }

                    if (cohortName.equals(StudyEntry.DEFAULT_COHORT)) {
                        description = DEFAULT_COHORT_DESCRIPTION;
                    } else {
                        description = "Cohort data imported from " + source;
                    }
                    Cohort cohort = catalogManager.getCohortManager().create(studyConfiguration.getStudyName(), cohortName, Study
                            .Type.COLLECTION, description, newSampleList, null, Collections.emptyMap(), sessionId).first();
                    newCohortIds.put(cohortName, (int) cohort.getUid());
                    newCohorts.put((int) cohort.getUid(), newSampleList.stream().map(Sample::getUid).map(Long::intValue)
                            .collect(Collectors.toSet()));
                    catalogManager.getCohortManager().setStatus(studyStr, cohort.getId(), Cohort.CohortStatus.READY, "", sessionId);
                }
                studyConfiguration.setCohortIds(newCohortIds);
                studyConfiguration.setCohorts(newCohorts);
                studyConfiguration.setCalculatedStats(newCohorts.keySet());

                // Update Sample Ids
                studyConfiguration.setSampleIds(samplesMap);
                for (Map.Entry<Integer, LinkedHashSet<Integer>> entry : studyConfiguration.getSamplesInFiles().entrySet()) {
                    Set<Integer> samples = entry.getValue();
                    LinkedHashSet<Integer> remappedSamples = new LinkedHashSet<>(samples.size());
                    for (Integer sample : samples) {
                        if (samplesIdMap.containsKey(sample)) {
                            remappedSamples.add(samplesIdMap.get(sample));
                        }
                    }
                    studyConfiguration.getSamplesInFiles().put(entry.getKey(), remappedSamples);
                }

                // Create Files
                Map<String, Integer> newFileIds = new HashMap<>();
                for (Map.Entry<String, Integer> entry : studyConfiguration.getFileIds().entrySet()) {
                    String fileName = entry.getKey();
                    Integer oldFileId = entry.getValue();

                    List<Sample> samples = studyConfiguration.getSamplesInFiles()
                            .get(oldFileId)
                            .stream()
                            .map(integer -> new Sample().setUid(((long) integer)))
                            .collect(Collectors.toList());

                    File file = new File(fileName, File.Type.FILE, File.Format.VCF, File.Bioformat.VARIANT, fileName,
                            "File imported from " + source, null, 0, 0);
                    file.setIndex(new FileIndex("", "", new FileIndex.IndexStatus(Status.READY, ""), -1, Collections.emptyMap()));
                    file.setSamples(samples);

                    file = catalogManager.getFileManager().create(studyStr, file, false, null, null, sessionId).first();

                    long fileId = file.getUid();
                    LinkedHashSet<Integer> samplesInFile = studyConfiguration.getSamplesInFiles().remove(oldFileId);
                    studyConfiguration.getSamplesInFiles().put(((int) fileId), samplesInFile);
                    newFileIds.put(fileName, (int) fileId);
                    if (studyConfiguration.getIndexedFiles().remove(oldFileId)) {
                        studyConfiguration.getIndexedFiles().add((int) fileId);
                    }
                }
                studyConfiguration.getFileIds().clear();
                studyConfiguration.getFileIds().putAll(newFileIds);
            } catch (CatalogException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
}
