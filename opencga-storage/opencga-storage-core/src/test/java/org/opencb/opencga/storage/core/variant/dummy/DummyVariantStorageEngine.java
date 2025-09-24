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

package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.config.DatabaseCredentials;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.config.storage.StorageEngineConfiguration;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Created on 28/11/16.
 *
 * TODO: Use Mockito
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyVariantStorageEngine extends VariantStorageEngine {

    public DummyVariantStorageEngine() {
        super();
    }

    public DummyVariantStorageEngine(StorageConfiguration configuration) {
        super(configuration);
    }
    private Logger logger = LoggerFactory.getLogger(DummyVariantStorageEngine.class);

    public static final String STORAGE_ENGINE_ID = "dummy";

    public static void configure(StorageEngineFactory factory) {
        configure(factory, false);
    }

    public static void configure(StorageEngineFactory factory, boolean clear) {
        StorageConfiguration storageConfiguration = factory.getStorageConfiguration();
        factory.unregisterVariantStorageEngine(DummyVariantStorageEngine.STORAGE_ENGINE_ID);

        storageConfiguration.getVariant().setDefaultEngine(STORAGE_ENGINE_ID);
//        storageConfiguration.getVariant().getEngines().clear();
        storageConfiguration.getVariant().getEngines().removeIf(c -> c.getId().equals(STORAGE_ENGINE_ID));
        storageConfiguration.getVariant().getEngines()
                .add(new StorageEngineConfiguration()
                        .setId(STORAGE_ENGINE_ID)
                        .setEngine(DummyVariantStorageEngine.class.getName())
                        .setOptions(new ObjectMap())
                        .setDatabase(new DatabaseCredentials()));

        if (clear) {
            DummyVariantStorageMetadataDBAdaptorFactory.clear();
        }
    }

    @Override
    public String getStorageEngineId() {
        return STORAGE_ENGINE_ID;
    }

    @Override
    public VariantDBAdaptor getDBAdaptor() throws StorageEngineException {
        return new DummyVariantDBAdaptor(dbName);
    }

    @Override
    public DummyVariantStoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException {
        DummyVariantStoragePipeline pipeline =
                new DummyVariantStoragePipeline(getConfiguration(), STORAGE_ENGINE_ID, getDBAdaptor(), getIOManagerProvider());
        pipeline.init(getOptions());
        return pipeline;
    }

    @Override
    protected VariantAnnotationManager newVariantAnnotationManager(VariantAnnotator annotator) throws StorageEngineException {
        return new DefaultVariantAnnotationManager(annotator, getDBAdaptor(), getIOManagerProvider()){
            @Override
            public void saveAnnotation(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException {
                dbAdaptor.getMetadataManager().updateProjectMetadata(project -> {
                    registerNewAnnotationSnapshot(name, variantAnnotator, project);
                    return project;
                });
            }

            @Override
            public void deleteAnnotation(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException {
                ProjectMetadata.VariantAnnotationMetadata saved = dbAdaptor.getMetadataManager().getProjectMetadata()
                        .getAnnotation().getSaved(name);

                dbAdaptor.getMetadataManager().updateProjectMetadata(project -> {
                    removeAnnotationSnapshot(name, project);
                    return project;
                });
            }
        };
    }

    @Override
    public DataResult<Trio> familyIndex(String study, List<Trio> trios, ObjectMap options) throws StorageEngineException {
        logger.info("Running family index!");
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        int studyId = studyMetadata.getId();
        for (int i = 0; i < trios.size(); i += 3) {
            Integer father = metadataManager.getSampleId(studyId, trios.get(i));
            Integer mother = metadataManager.getSampleId(studyId, trios.get(i + 1));
            Integer child = metadataManager.getSampleId(studyId, trios.get(i + 2));
            metadataManager.updateSampleMetadata(studyId, child, sampleMetadata -> {
                sampleMetadata.setFamilyIndexStatus(TaskMetadata.Status.READY, studyMetadata.getSampleIndexConfigurationLatest().getVersion());
                if (father != null && father > 0) {
                    sampleMetadata.setFather(father);
                }
                if (mother != null && mother > 0) {
                    sampleMetadata.setMother(mother);
                }
            });
        }
        return new DataResult<Trio>().setResults(trios);
    }

    @Override
    public void aggregateFamily(String study, VariantAggregateFamilyParams params, ObjectMap options, URI outdir) throws StorageEngineException {
        logger.info("Running family aggregation!");
        List<String> samples = params.getSamples();
        if (samples == null || samples.size() < 2) {
            throw new IllegalArgumentException("Aggregate family operation requires at least two samples.");
        } else if (new HashSet<>(samples).size() != samples.size()) {
            // Fail if duplicated samples
            throw new IllegalArgumentException("Unable to execute aggregate-family operation with duplicated samples.");
        }
        StudyMetadata studyMetadata = getMetadataManager().getStudyMetadata(study);

        // Try to register the cohort. It might fail if the cohort already exists.
        int internalCohortId = getMetadataManager()
                .registerAggregateFamilySamplesCohort(studyMetadata.getId(), samples, params.isResume(), params.isResume());


        // Update family index status to NONE
        for (String sample : samples) {
            int sampleId = getMetadataManager().getSampleId(studyMetadata.getId(), sample);
            getMetadataManager().updateSampleMetadata(studyMetadata.getId(), sampleId, sm -> {
                Integer version = sm.getFamilyIndexVersion();
                if (version != null) {
                    logger.info("Updating family index status for sample '{}' to {}", sm.getName(), TaskMetadata.Status.NONE);
                    sm.setFamilyIndexStatus(TaskMetadata.Status.NONE, version);
                }
            });
        }


        getMetadataManager().updateCohortMetadata(studyMetadata.getId(), internalCohortId, cohort -> {
            cohort.setStatusByType(TaskMetadata.Status.READY);
        });
        getMetadataManager().removeExtraInternalCohorts(studyMetadata.getId(), internalCohortId);
    }

    @Override
    protected VariantImporter newVariantImporter() throws StorageEngineException {
        return new VariantImporter(getDBAdaptor()) {
            @Override
            public void importData(URI input, VariantMetadata metadata, List<StudyConfiguration> scs)
                    throws StorageEngineException, IOException {
            }
        };
    }

    @Override
    public List<URI> walkData(URI outputFile, VariantWriterFactory.VariantOutputFormat format, Query query, QueryOptions queryOptions, String commandLine) throws StorageEngineException {
        throw new UnsupportedOperationException("Unable to walk data in " + getStorageEngineId());
    }

    @Override
    public void removeFiles(String study, List<String> files, URI outdir) throws StorageEngineException {
        TaskMetadata task = preRemove(study, files, Collections.emptyList());
        logger.info("Deleting files {} from study '{}'", files, study);
        postRemoveFiles(study, task.getFileIds(), Collections.emptyList(), task.getId(), false);
    }

    @Override
    public void removeSamples(String study, List<String> samples, URI outdir) throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        int studyId = metadataManager.getStudyId(study);
        List<Integer> sampleIds = metadataManager.getSampleIds(studyId, samples);

        // Check if any file is being completely deleted
        Set<Integer> partiallyDeletedFiles = metadataManager.getFileIdsFromSampleIds(studyId, sampleIds, true);
        List<String> fullyDeletedFiles = new ArrayList<>();
        List<Integer> fullyDeletedFileIds = new ArrayList<>();
        for (Integer partiallyDeletedFile : partiallyDeletedFiles) {
            LinkedHashSet<Integer> samplesFromFile = metadataManager.getSampleIdsFromFileId(studyId, partiallyDeletedFile);
            if (sampleIds.containsAll(samplesFromFile)) {
                fullyDeletedFileIds.add(partiallyDeletedFile);
                fullyDeletedFiles.add(metadataManager.getFileName(studyId, partiallyDeletedFile));
            }
        }

        TaskMetadata taskMetadata = preRemove(study, fullyDeletedFiles, samples);
        logger.info("Deleting samples {} from study '{}'", samples, study);
        postRemoveFiles(study, fullyDeletedFileIds, sampleIds, taskMetadata.getId(), false);
    }

    @Override
    public void removeStudy(String studyName, URI outdir) throws StorageEngineException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        metadataManager.updateStudyMetadata(studyName, studyMetadata -> {
            metadataManager.removeIndexedFiles(studyMetadata.getId(), metadataManager.getIndexedFiles(studyMetadata.getId()));
            return studyMetadata;
        });
    }

    @Override
    public void loadVariantScore(URI scoreFile, String study, String scoreName, String cohort1, String cohort2,
                                 VariantScoreFormatDescriptor descriptor, ObjectMap options) {
        throw new UnsupportedOperationException("Unable to load VariantScore in " + getStorageEngineId());
    }

    @Override
    public void deleteVariantScore(String study, String scoreName, ObjectMap options) throws StorageEngineException {
        throw new UnsupportedOperationException("Unable to remove VariantScore in " + getStorageEngineId());
    }

    @Override
    public VariantStorageMetadataManager getMetadataManager() throws StorageEngineException {
        return new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory(dbName));
    }

    @Override
    public void testConnection() throws StorageEngineException {
    }
}
