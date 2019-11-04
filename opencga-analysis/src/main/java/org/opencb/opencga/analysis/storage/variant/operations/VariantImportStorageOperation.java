package org.opencb.opencga.analysis.storage.variant.operations;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.storage.models.StudyInfo;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.io.VariantMetadataImporter;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.storage.variant.operations.VariantFileIndexerStorageOperation.DEFAULT_COHORT_DESCRIPTION;

public class VariantImportStorageOperation extends StorageOperation {

    public VariantImportStorageOperation(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory, LoggerFactory.getLogger(VariantRemoveStorageOperation.class));
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
            try (VariantStorageMetadataManager scm = variantStorageEngine.getMetadataManager()) {
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
            studyConfiguration.setStudyName(studyStr);

            try {
                // Create Samples
                Map<String, Integer> samplesMap = new HashMap<>();
                Map<Integer, Integer> samplesIdMap = new HashMap<>();
                String source = inputUri.resolve(".").relativize(inputUri).getPath();
                String description = "Sample data imported from " + source;
                for (Map.Entry<String, Integer> entry : studyConfiguration.getSampleIds().entrySet()) {
                    Sample sample = catalogManager.getSampleManager().create(studyStr,
                            new Sample(entry.getKey(), source, null, description, 1), QueryOptions.empty(), sessionId).first();
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
                    Cohort cohort = catalogManager.getCohortManager().create(studyConfiguration.getName(), cohortName, Study
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
                            null, "File imported from " + source, null, 0, 0);
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
