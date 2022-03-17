package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.utils.BatchUtils;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

    public DataResult<List<String>> load(String study, List<List<String>> trios, ObjectMap options) throws StorageEngineException {
        trios = new LinkedList<>(trios);
        DataResult<List<String>> dr = new DataResult<>();
        dr.setResults(trios);
        dr.setEvents(new LinkedList<>());

        boolean overwrite = options.getBoolean(FamilyIndexDriver.OVERWRITE);
        if (trios.isEmpty()) {
            throw new StorageEngineException("Undefined family trios");
        }
        int studyId = metadataManager.getStudyId(study);
        int version = sampleIndexDBAdaptor.getSchemaFactory().getSampleIndexConfigurationLatest(studyId, true).getVersion();
        options.put(FamilyIndexDriver.SAMPLE_INDEX_VERSION, version);
        options.put(FamilyIndexDriver.OUTPUT, sampleIndexDBAdaptor.getSampleIndexTableName(studyId, version));
        Iterator<List<String>> iterator = trios.iterator();
        while (iterator.hasNext()) {
            List<Integer> trioIds = new ArrayList<>(3);
            List<String> trio = iterator.next();
            for (String sample : trio) {
                Integer sampleId;
                if (sample.equals("-")) {
                    sampleId = -1;
                } else {
                    sampleId = metadataManager.getSampleId(studyId, sample);
                    if (sampleId == null) {
                        throw new IllegalArgumentException("Sample '" + sample + "' not found.");
                    }
                }
                trioIds.add(sampleId);
            }
            if (trioIds.size() != 3) {
                throw new IllegalArgumentException("Found trio with " + trioIds.size() + " members, instead of 3: " + trioIds);
            }
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, trioIds.get(2));
            if (!overwrite && sampleMetadata.getFamilyIndexStatus(version) == TaskMetadata.Status.READY) {
                String msg = "Skip sample " + sampleMetadata.getName() + ". Already precomputed!";
                logger.info(msg);
                dr.getEvents().add(new Event(Event.Type.INFO, msg));
                iterator.remove();
            } else {
                Integer fatherId = trioIds.get(0);
                boolean fatherDefined = fatherId != -1;
                Integer motherId = trioIds.get(1);
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
        List<List<List<String>>> batches = BatchUtils.splitBatches(trios, batchSize);
        if (batches.size() == 1) {
            runBatch(study, trios, options, studyId);
        } else {
            logger.warn("Unable to run family index in one single MapReduce operation.");
            logger.info("Split in {} jobs of {} samples each.", batches, batches.get(0).size());
            for (int i = 0; i < batches.size(); i++) {
                List<List<String>> batch = batches.get(i);
                logger.info("Running MapReduce {}/{} over {} trios", i + 1, batches, batch.size());
                runBatch(study, batch, options, studyId);
            }
        }
        postIndex(studyId, version);
        return dr;
    }

    private void runBatch(String study, List<List<String>> trios, ObjectMap options, int studyId) throws StorageEngineException {
        if (trios.size() < 500) {
            options.put(FamilyIndexDriver.TRIOS, trios.stream().map(trio -> String.join(",", trio)).collect(Collectors.joining(";")));
        } else {
            CohortMetadata cohortMetadata = metadataManager.registerTemporaryCohort(study, "pendingFamilyIndexSamples",
                    trios.stream().map(t -> t.get(2)).collect(Collectors.toList()));

            options.put(FamilyIndexDriver.TRIOS_COHORT, cohortMetadata.getName());
            options.put(FamilyIndexDriver.TRIOS_COHORT_DELETE, true);
        }

        mrExecutor.run(FamilyIndexDriver.class, FamilyIndexDriver.buildArgs(
                tableNameGenerator.getArchiveTableName(studyId),
                tableNameGenerator.getVariantTableName(),
                studyId, null, options),
                "Precompute mendelian errors for " + (trios.size() == 1 ? "trio " + trios.get(0) : trios.size() + " trios"));
    }

    public void postIndex(int studyId, int version)
            throws StorageEngineException {
        sampleIndexDBAdaptor.updateSampleIndexSchemaStatus(studyId, version);
    }

}
