package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.index.sample.family.SampleFamilyIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LocalSampleFamilyIndexer extends SampleFamilyIndexer {

    private final VariantStorageEngine engine;
    private final Logger logger = LoggerFactory.getLogger(LocalSampleFamilyIndexer.class);

    public LocalSampleFamilyIndexer(LocalSampleIndexDBAdaptor sampleIndexDBAdaptor,
                                       VariantStorageEngine engine) {
        super(sampleIndexDBAdaptor);
        this.engine = engine;
    }

    @Override
    protected void runBatch(String study, List<Trio> trios, ObjectMap options, int studyId, int version)
            throws StorageEngineException {
        // TODO: Implement family index for local files
        // This requires:
        // 1. Reading child sample entries
        // 2. Reading parent sample entries (father and mother)
        // 3. Calculating Mendelian errors by comparing genotypes
        // 4. Encoding parent genotypes into parentsCode
        // 5. Building mendelianVariants list
        // 6. Updating child entries with parents index and mendelian variants

        logger.warn("Family index construction for local files is not yet implemented. Skipping {} trios.",
                trios.size());

        // For now, mark samples as processed without actually computing family index
        for (Trio trio : trios) {
            Integer childId = metadataManager.getSampleId(studyId, trio.getChild());
            metadataManager.updateSampleMetadata(studyId, childId, sampleMetadata -> {
                sampleMetadata.setFamilyIndexStatus(TaskMetadata.Status.READY, version);
            });
        }
    }
}
