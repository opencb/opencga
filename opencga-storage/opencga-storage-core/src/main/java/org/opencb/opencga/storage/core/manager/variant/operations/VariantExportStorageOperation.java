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

import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.monitor.executors.AbstractExecutor;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.models.StudyInfo;
import org.opencb.opencga.storage.core.metadata.ExportMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
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
            if (!Paths.get(outdirUri).toFile().exists()) {
                outputFileName = outdirUri.resolve(".").relativize(outdirUri).toString();
                outdirUri = outdirUri.resolve(".");
            } else {
                try {
                    outdirUri = UriUtils.createDirectoryUri(outputStr);
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException(e);
                }
                List<Region> regions = Region.parseRegions(query.getString(VariantQueryParam.REGION.key()));
                outputFileName = buildOutputFileName(studyInfos.stream().map(StudyInfo::getStudyAlias).collect(Collectors.toList()),
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
            Long catalogOutDirId = getCatalogOutdirId(studyInfos.get(0).getStudyId(), options, sessionId);

//            for (StudyInfo studyInfo : studyInfos) {
//                StudyConfiguration studyConfiguration = updateStudyConfiguration(sessionId, studyInfo.getStudyId(), dataStore);
//            }

            VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
            variantStorageEngine.exportData(outputFile, outputFormat, query, new QueryOptions(options));

            if (catalogOutDirId != null && outdir != null) {
                copyResults(outdir, catalogOutDirId, sessionId).stream().map(File::getUri);
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
        variantMetadataImporter = new CatalogVariantMetadataImporter(studyInfo.getStudyId(), inputUri, sessionId);

        try {
            DataStore dataStore = studyInfo.getDataStores().get(File.Bioformat.VARIANT);
            VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
            ObjectMap options = variantStorageEngine.getOptions();
            ExportMetadata exportMetadata;
            try (StudyConfigurationManager scm = variantStorageEngine.getStudyConfigurationManager()) {
                exportMetadata = variantMetadataImporter.importMetaData(inputUri, scm);
            }
            StudyConfiguration oldSC = VariantMetadataImporter.readMetadata(inputUri).getStudies().get(0);
            StudyConfiguration newSC = exportMetadata.getStudies().get(0);
            Map<StudyConfiguration, StudyConfiguration> studiesOldNewMap = Collections.singletonMap(oldSC, newSC);

            variantStorageEngine.importData(inputUri, exportMetadata, studiesOldNewMap, options);

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
        private long studyId;

        private CatalogVariantMetadataImporter(long studyId, URI inputUri, String sessionId) {
            this.inputUri = inputUri;
            this.sessionId = sessionId;
            this.studyId = studyId;
        }

        @Override
        protected void processStudyConfiguration(Map<Integer, List<Integer>> returnedSamples,
                                                 StudyConfiguration studyConfiguration) {
            super.processStudyConfiguration(returnedSamples, studyConfiguration);

            studyConfiguration.setStudyId((int) studyId);

            try {
                // Create Samples
                Map<String, Integer> samplesMap = new HashMap<>();
                Map<Integer, Integer> samplesIdMap = new HashMap<>();
                String source = inputUri.resolve(".").relativize(inputUri).getPath();
                String description = "Sample data imported from " + source;
                for (Map.Entry<String, Integer> entry : studyConfiguration.getSampleIds().entrySet()) {
                    Sample sample = catalogManager.getSampleManager().create(Long.toString(studyId), entry.getKey(), source, description,
                            null, false, null, Collections.emptyMap(), QueryOptions.empty(), sessionId).first();
                    samplesMap.put(sample.getName(), (int) sample.getId());
                    samplesIdMap.put(entry.getValue(), (int) sample.getId());
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
                            newSampleList.add(new Sample().setId(samplesIdMap.get(sampleId)));
                        }
                    }
                    Cohort cohort = catalogManager.getCohortManager().create((long) studyConfiguration.getStudyId(), cohortName, Study
                            .Type.COLLECTION, "", newSampleList, null, Collections.emptyMap(), sessionId).first();
                    newCohortIds.put(cohortName, (int) cohort.getId());
                    newCohorts.put((int) cohort.getId(), newSampleList.stream().map(Sample::getId).map(Long::intValue)
                            .collect(Collectors.toSet()));
                    catalogManager.getCohortManager().setStatus(String.valueOf(cohort.getId()), Cohort.CohortStatus.READY, "", sessionId);
                }
                studyConfiguration.setCohortIds(newCohortIds);
                studyConfiguration.setCohorts(newCohorts);
                studyConfiguration.setCalculatedStats(newCohorts.keySet());

                // Create Files
                //TODO

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

            } catch (CatalogException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
}
