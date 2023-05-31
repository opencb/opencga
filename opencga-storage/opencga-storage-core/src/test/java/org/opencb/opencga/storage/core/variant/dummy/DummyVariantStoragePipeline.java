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

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.dedup.AbstractDuplicatedVariantsResolver;
import org.opencb.opencga.storage.core.variant.dedup.DuplicatedVariantsResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.LOAD_SAMPLE_INDEX;

/**
 * Created on 28/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyVariantStoragePipeline extends VariantStoragePipeline {

    public static final String VARIANTS_LOAD_FAIL = "dummy.variants.load.fail";
    public static final String LOAD_SLEEP = "dummy.variants.load.sleep";
    private final Logger logger = LoggerFactory.getLogger(DummyVariantStoragePipeline.class);

    public DummyVariantStoragePipeline(StorageConfiguration configuration, String storageEngineId, VariantDBAdaptor dbAdaptor, IOConnectorProvider ioConnectorProvider) {
        super(configuration, storageEngineId, dbAdaptor, ioConnectorProvider, new ObjectMap());
    }

    public void init(ObjectMap options) {
        getOptions().clear();
        getOptions().putAll(options);
    }

    @Override
    protected void securePreLoad(StudyMetadata studyMetadata, VariantFileMetadata variantFileMetadata) throws StorageEngineException {
        super.securePreLoad(studyMetadata, variantFileMetadata);

        List<Integer> fileIds = Collections.singletonList(getFileId());
        getMetadataManager().addRunningTask(getStudyId(), "load", fileIds, false, TaskMetadata.Type.LOAD);
    }

    @Override
    public URI load(URI input, URI outdir) throws IOException, StorageEngineException {
        logger.info("Loading file " + input);
        List<Integer> fileIds = Collections.singletonList(getFileId());
        if (getOptions().getInt(LOAD_SLEEP) > 0) {
            try {
                Thread.sleep(getOptions().getInt(LOAD_SLEEP));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StorageEngineException("Interrupted", e);
            }
        }
        DuplicatedVariantsResolverFactory factory = new DuplicatedVariantsResolverFactory(getOptions(), ioConnectorProvider);
        AbstractDuplicatedVariantsResolver resolver = factory.getResolver(UriUtils.fileName(input), outdir);

        VariantFileMetadata variantFileMetadata = variantReaderUtils.readVariantFileMetadata(input);
        ProgressLogger progressLogger = new ProgressLogger("Variants loaded:", variantFileMetadata.getStats().getVariantCount());
        for (Variant variant : variantReaderUtils.getVariantReader(input, variantFileMetadata.toVariantStudyMetadata(""))
                .then(factory.getTask(resolver))) {
            progressLogger.increment(1, () -> "up to variant " + variant);
        }
        getLoadStats().put("duplicatedVariants", resolver.getDuplicatedVariants());
        getLoadStats().put("duplicatedLocus", resolver.getDuplicatedLocus());
        getLoadStats().put("discardedVariants", resolver.getDiscardedVariants());
        if (getOptions().getBoolean(VARIANTS_LOAD_FAIL) || getOptions().getString(VARIANTS_LOAD_FAIL).equals(Paths.get(input).getFileName().toString())) {
            getMetadataManager().atomicSetStatus(getStudyId(), TaskMetadata.Status.ERROR, "load", fileIds);
            throw new StorageEngineException("Error loading file " + input);
        } else {
            getMetadataManager().atomicSetStatus(getStudyId(), TaskMetadata.Status.DONE, "load", fileIds);
        }
        return input;
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageEngineException {
        logger.info("Post load file " + input);

        VariantFileMetadata fileMetadata = readVariantFileMetadata(input);
        fileMetadata.setId(String.valueOf(getFileId()));
        dbAdaptor.getMetadataManager().updateVariantFileMetadata(getStudyId(), fileMetadata);

        boolean loadSampleIndex = YesNoAuto.parse(getOptions(), LOAD_SAMPLE_INDEX.key()).orYes().booleanValue();
        if (loadSampleIndex) {
            VariantStorageMetadataManager metadataManager = getMetadataManager();
            int sampleIndexVersion = metadataManager.getStudyMetadata(getStudyId()).getSampleIndexConfigurationLatest(true).getVersion();
            for (Integer sampleId : metadataManager.getSampleIdsFromFileId(getStudyId(), getFileId())) {
                // Worth to check first to avoid too many updates in scenarios like 1000G
                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(getStudyId(), sampleId);
                if (sampleMetadata.getSampleIndexStatus(sampleIndexVersion) != TaskMetadata.Status.READY) {
                    metadataManager.updateSampleMetadata(getStudyId(), sampleId,
                            s -> s.setSampleIndexStatus(TaskMetadata.Status.READY, sampleIndexVersion));
                }
            }
        }

        return super.postLoad(input, output);
    }

    @Override
    protected void securePostLoad(List<Integer> fileIds, StudyMetadata studyMetadata) throws StorageEngineException {
        super.securePostLoad(fileIds, studyMetadata);
        TaskMetadata.Status status = dbAdaptor.getMetadataManager()
                .setStatus(studyMetadata.getId(), "load", fileIds, TaskMetadata.Status.READY);
        if (status != TaskMetadata.Status.DONE) {
            logger.warn("Unexpected status " + status);
        }
    }

    @Override
    protected void checkLoadedVariants(int fileId, StudyMetadata studyMetadata) throws StorageEngineException {

    }
}
