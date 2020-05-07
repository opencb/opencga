/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.variant.manager.operations;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileIndex;
import org.opencb.opencga.core.models.file.FileInternal;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.io.VariantMetadataImporter;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.variant.manager.operations.VariantFileIndexerOperationManager.DEFAULT_COHORT_DESCRIPTION;

public class VariantImportOperationManager extends OperationManager {

    public VariantImportOperationManager(VariantStorageManager variantStorageManager, VariantStorageEngine variantStorageEngine) {
        super(variantStorageManager, variantStorageEngine);
    }

    public void run(String study, URI inputUri, String token) throws Exception {

        VariantMetadataImporter variantMetadataImporter;
        variantMetadataImporter = new CatalogVariantMetadataImporter(study, inputUri, token);

        ObjectMap options = variantStorageEngine.getOptions();
        VariantMetadata metadata;
        try (VariantStorageMetadataManager scm = variantStorageEngine.getMetadataManager()) {
            metadata = variantMetadataImporter.importMetaData(inputUri, scm);
        }

        variantStorageEngine.importData(inputUri, metadata, null, options);
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
            studyConfiguration.setStudyName(studyStr);

            try {
                // Create Samples
                Map<String, Integer> samplesMap = new HashMap<>();
                Map<Integer, Integer> samplesIdMap = new HashMap<>();
                String source = inputUri.resolve(".").relativize(inputUri).getPath();
                String description = "Sample data imported from " + source;
                for (Map.Entry<String, Integer> entry : studyConfiguration.getSampleIds().entrySet()) {
                    Sample sample = catalogManager.getSampleManager().create(studyStr,
                            new Sample(entry.getKey(), null, description, 1), QueryOptions.empty(), sessionId).first();
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
                    Cohort cohort = catalogManager.getCohortManager().create(studyConfiguration.getName(), cohortName, Enums.CohortType.COLLECTION, description, newSampleList, null, Collections.emptyMap(), sessionId).first();
                    newCohortIds.put(cohortName, (int) cohort.getUid());
                    newCohorts.put((int) cohort.getUid(), newSampleList.stream().map(Sample::getUid).map(Long::intValue)
                            .collect(Collectors.toSet()));
                    catalogManager.getCohortManager().setStatus(studyStr, cohort.getId(), CohortStatus.READY, "", sessionId);
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
                            null, "File imported from " + source, new FileInternal(null, new FileIndex("", "",
                            new FileIndex.IndexStatus(Status.READY, ""), -1, Collections.emptyMap()), Collections.emptyMap()), 0, 0);
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
