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
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;

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
    public DataResult<List<String>> familyIndex(String study, List<List<String>> trios, ObjectMap options) throws StorageEngineException {
        logger.info("Running family index!");
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        int studyId = metadataManager.getStudyId(study);
        for (int i = 0; i < trios.size(); i += 3) {
            Integer father = metadataManager.getSampleId(studyId, trios.get(i));
            Integer mother = metadataManager.getSampleId(studyId, trios.get(i + 1));
            Integer child = metadataManager.getSampleId(studyId, trios.get(i + 2));
            metadataManager.updateSampleMetadata(studyId, child, sampleMetadata -> {
                sampleMetadata.setFamilyIndexStatus(TaskMetadata.Status.READY);
                if (father != null && father > 0) {
                    sampleMetadata.setFather(father);
                }
                if (mother != null && mother > 0) {
                    sampleMetadata.setMother(mother);
                }
                return sampleMetadata;
            });
        }
        return new DataResult<List<String>>().setResults(trios);
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
    public void removeFiles(String study, List<String> files) throws StorageEngineException {
        TaskMetadata task = preRemove(study, files, Collections.emptyList());
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        postRemoveFiles(study, task.getFileIds(), Collections.emptyList(), task.getId(), false);
    }

    @Override
    public void removeStudy(String studyName) throws StorageEngineException {
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
        return new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
    }

    @Override
    public void testConnection() throws StorageEngineException {
    }
}
