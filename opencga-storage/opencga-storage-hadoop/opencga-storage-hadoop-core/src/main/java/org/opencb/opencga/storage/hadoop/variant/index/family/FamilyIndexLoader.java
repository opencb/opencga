package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class FamilyIndexLoader {

    private final VariantStorageMetadataManager metadataManager;
    private final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private final MRExecutor mrExecutor;
    private final Logger logger = LoggerFactory.getLogger(FamilyIndexLoader.class);
    private final HBaseVariantTableNameGenerator tableNameGenerator;

    public FamilyIndexLoader(SampleIndexDBAdaptor sampleIndexDBAdaptor, VariantHadoopDBAdaptor dbAdaptor, MRExecutor mrExecutor) {
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.metadataManager = dbAdaptor.getMetadataManager();
        this.tableNameGenerator = dbAdaptor.getTableNameGenerator();
        this.mrExecutor = mrExecutor;
    }

    public DataResult<Trio> load(String study, List<Trio> trios, ObjectMap options) throws StorageEngineException {
        trios = new LinkedList<>(trios);
        DataResult<Trio> dr = new DataResult<>();
        dr.setResults(trios);
        dr.setEvents(new LinkedList<>());

        boolean overwrite = options.getBoolean(FamilyIndexDriver.OVERWRITE);
        if (trios.isEmpty()) {
            throw new StorageEngineException("Undefined family trios");
        }

        int studyId = metadataManager.getStudyId(study);
        int version = sampleIndexDBAdaptor.getSchemaFactory().getSampleIndexConfigurationLatest(studyId, true).getVersion();
        sampleIndexDBAdaptor.createTableIfNeeded(studyId, version, options);

        options.put(FamilyIndexDriver.SAMPLE_INDEX_VERSION, version);
        options.put(FamilyIndexDriver.OUTPUT, sampleIndexDBAdaptor.getSampleIndexTableName(studyId, version));
        Iterator<Trio> iterator = trios.iterator();
        while (iterator.hasNext()) {
            Trio trio = iterator.next();

            final Integer fatherId;
            final Integer motherId;
            final Integer childId;

            childId = metadataManager.getSampleId(studyId, trio.getChild());
            if (trio.getFather() == null) {
                fatherId = -1;
            } else {
                fatherId = metadataManager.getSampleIdOrFail(studyId, trio.getFather());
            }
            if (trio.getMother() == null) {
                motherId = -1;
            } else {
                motherId = metadataManager.getSampleIdOrFail(studyId, trio.getMother());
            }

            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, childId);
            if (!overwrite && sampleMetadata.getFamilyIndexStatus(version) == TaskMetadata.Status.READY) {
                String msg = "Skip sample " + sampleMetadata.getName() + ". Already precomputed!";
                logger.info(msg);
                dr.getEvents().add(new Event(Event.Type.INFO, msg));
                iterator.remove();
            } else {
                boolean fatherDefined = fatherId != -1;
                boolean motherDefined = motherId != -1;
                if (fatherDefined && !fatherId.equals(sampleMetadata.getFather())
                        || motherDefined && !motherId.equals(sampleMetadata.getMother())) {
                    metadataManager.updateSampleMetadata(studyId, sampleMetadata.getId(), s -> {
                        if (fatherDefined) {
                            s.setFather(fatherId);
                        }
                        if (motherDefined) {
                            s.setMother(motherId);
                        }
                        s.setFamilyIndexDefined(true);
                    });
                }
            }
        }
        if (trios.isEmpty()) {
            logger.info("Nothing to do!");
            return dr;
        }

        int batchSize = options.getInt(HadoopVariantStorageOptions.SAMPLE_INDEX_FAMILY_MAX_TRIOS_PER_MR.key(),
                HadoopVariantStorageOptions.SAMPLE_INDEX_FAMILY_MAX_TRIOS_PER_MR.defaultValue());
        List<List<Trio>> batches = BatchUtils.splitBatches(trios, batchSize);
        if (batches.size() == 1) {
            runBatch(study, trios, options, studyId);
        } else {
            logger.warn("Unable to run family index in one single MapReduce operation.");
            logger.info("Split in {} jobs of {} samples each.", batches, batches.get(0).size());
            for (int i = 0; i < batches.size(); i++) {
                List<Trio> batch = batches.get(i);
                logger.info("Running MapReduce {}/{} over {} trios", i + 1, batches, batch.size());
                runBatch(study, batch, options, studyId);
            }
        }
        postIndex(studyId, version);
        return dr;
    }

    private void runBatch(String study, List<Trio> trios, ObjectMap options, int studyId) throws StorageEngineException {
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

        mrExecutor.run(FamilyIndexDriver.class, FamilyIndexDriver.buildArgs(
                tableNameGenerator.getArchiveTableName(studyId),
                tableNameGenerator.getVariantTableName(),
                studyId, null, options),
                "Precompute mendelian errors for " + (trios.size() == 1 ? "trio " + trios.get(0).serialize() : trios.size() + " trios"));
    }

    public void postIndex(int studyId, int version)
            throws StorageEngineException {
        sampleIndexDBAdaptor.updateSampleIndexSchemaStatus(studyId, version);
    }

}
