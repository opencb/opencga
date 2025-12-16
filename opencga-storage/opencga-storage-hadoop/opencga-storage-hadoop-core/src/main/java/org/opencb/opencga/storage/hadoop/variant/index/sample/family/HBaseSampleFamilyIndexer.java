package org.opencb.opencga.storage.hadoop.variant.index.sample.family;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.index.sample.family.SampleFamilyIndexer;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.HBaseSampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class HBaseSampleFamilyIndexer extends SampleFamilyIndexer {

    private final MRExecutor mrExecutor;
    private final Logger logger = LoggerFactory.getLogger(HBaseSampleFamilyIndexer.class);
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final HBaseSampleIndexDBAdaptor sampleIndexDBAdaptor;

    public HBaseSampleFamilyIndexer(HBaseSampleIndexDBAdaptor sampleIndexDBAdaptor, HBaseVariantTableNameGenerator tableNameGenerator,
                                    MRExecutor mrExecutor) {
        super(sampleIndexDBAdaptor);
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.tableNameGenerator = tableNameGenerator;
        this.mrExecutor = mrExecutor;
    }

    @Override
    protected void run(String study, List<Trio> trios, ObjectMap options, int studyId, int version) throws StorageEngineException {
        sampleIndexDBAdaptor.createTableIfNeeded(studyId, version, options);

        options.put(FamilyIndexDriver.SAMPLE_INDEX_VERSION, version);
        options.put(FamilyIndexDriver.OUTPUT, sampleIndexDBAdaptor.getSampleIndexTableName(studyId, version));

        int batchSize = options.getInt(HadoopVariantStorageOptions.SAMPLE_INDEX_FAMILY_MAX_TRIOS_PER_MR.key(),
                HadoopVariantStorageOptions.SAMPLE_INDEX_FAMILY_MAX_TRIOS_PER_MR.defaultValue());
        List<List<Trio>> batches = BatchUtils.splitBatches(trios, batchSize);
        if (batches.size() == 1) {
            runBatch(study, trios, options, studyId, version);
        } else {
            logger.warn("Unable to run family index in one single MapReduce operation.");
            logger.info("Split in {} jobs of {} samples each.", batches, batches.get(0).size());
            for (int i = 0; i < batches.size(); i++) {
                List<Trio> batch = batches.get(i);
                logger.info("Running MapReduce {}/{} over {} trios", i + 1, batches, batch.size());
                runBatch(study, batch, options, studyId, version);
            }
        }
    }

    @Override
    protected void runBatch(String study, List<Trio> trios, ObjectMap options, int studyId, int version)
            throws StorageEngineException {
        if (trios.size() < 500) {
            options.put(FamilyIndexDriver.TRIOS, trios.stream()
                    .map(Trio::serialize)
                    .collect(Collectors.joining(";")));
        } else {
            CohortMetadata cohortMetadata = metadataManager.registerTemporaryCohort(study, "pendingFamilyIndexSamples",
                    trios.stream().map(Trio::getChild).collect(Collectors.toList()));

            options.put(FamilyIndexDriver.TRIOS_COHORT, cohortMetadata.getName());
            options.put(FamilyIndexDriver.TRIOS_COHORT_DELETE, true);
        }

        options.put(FamilyIndexDriver.SAMPLE_INDEX_VERSION, version);
        options.put(FamilyIndexDriver.OUTPUT,
                sampleIndexDBAdaptor.getSampleIndexTableName(studyId, version));

        mrExecutor.run(FamilyIndexDriver.class, FamilyIndexDriver.buildArgs(
                tableNameGenerator.getArchiveTableName(studyId),
                tableNameGenerator.getVariantTableName(),
                studyId, null, options),
                "Precompute mendelian errors for "
                        + (trios.size() == 1 ? "trio " + trios.get(0).serialize() : trios.size() + " trios"));
    }
}
