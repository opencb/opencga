package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.sample.family.FamilyIndexBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.family.SampleFamilyIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Local (filesystem-backed) family index builder. Iterates variants from the engine's DBAdaptor in
 * sorted order, accumulates parent-genotype encodings and Mendelian errors per chunk via
 * {@link FamilyIndexBuilder}, and merges the result into the on-disk {@link SampleIndexEntry}.
 *
 * <p>Mirrors the per-chunk semantics of {@code MongoDBSampleFamilyIndexerTask} and the HBase
 * {@code FamilyIndexDriver}. The on-disk merge preserves any genotype/annotation fields written
 * earlier by the genotype/annotation indexers.
 *
 * <p><b>Simplification vs. MongoDB/HBase:</b> the MongoDB indexer distinguishes "parent's file is
 * present in this variant but parent has the default GT (0/0)" from "parent's file is absent
 * (parent GT truly unknown)". This requires per-file {@code mgt} access which the engine-level
 * {@code VariantDBIterator} hides. Here we always default missing GTs to {@code 0/0}, which is
 * correct for single-file studies but may misclassify Mendelian errors when parent and child come
 * from different files. The dummy/local engine that consumes this class does not currently support
 * multi-file ({@code SplitData.MULTI}) layouts, so this is acceptable for the test scope.
 */
public class LocalSampleFamilyIndexer extends SampleFamilyIndexer {

    private static final int MISSING_SAMPLE = -1;

    private final Logger logger = LoggerFactory.getLogger(LocalSampleFamilyIndexer.class);
    private final VariantStorageEngine engine;
    private final LocalSampleIndexDBAdaptor localAdaptor;

    public LocalSampleFamilyIndexer(LocalSampleIndexDBAdaptor sampleIndexDBAdaptor,
                                    VariantStorageEngine engine) {
        super(sampleIndexDBAdaptor);
        this.engine = engine;
        this.localAdaptor = sampleIndexDBAdaptor;
    }

    @Override
    protected void indexBatch(String study, List<Trio> trios, ObjectMap options, int studyId, int version)
            throws StorageEngineException {

        // Resolve sample IDs/names per trio
        List<int[]> resolvedTrios = new ArrayList<>(trios.size());
        Map<Integer, FamilyIndexBuilder> builders = new HashMap<>(trios.size());
        Map<Integer, Map<String, Integer>> genotypeCount = new HashMap<>(trios.size());
        Map<Integer, String> sampleNameById = new HashMap<>();
        Set<String> includeSamples = new LinkedHashSet<>();

        for (Trio trio : trios) {
            int childId = metadataManager.getSampleId(studyId, trio.getChild());
            int fatherId = trio.getFather() == null
                    ? MISSING_SAMPLE
                    : metadataManager.getSampleId(studyId, trio.getFather());
            int motherId = trio.getMother() == null
                    ? MISSING_SAMPLE
                    : metadataManager.getSampleId(studyId, trio.getMother());

            resolvedTrios.add(new int[]{fatherId, motherId, childId});
            builders.put(childId, new FamilyIndexBuilder(childId));
            genotypeCount.put(childId, new HashMap<>());
            sampleNameById.put(childId, trio.getChild());
            includeSamples.add(trio.getChild());
            if (fatherId != MISSING_SAMPLE) {
                sampleNameById.put(fatherId, trio.getFather());
                includeSamples.add(trio.getFather());
            }
            if (motherId != MISSING_SAMPLE) {
                sampleNameById.put(motherId, trio.getMother());
                includeSamples.add(trio.getMother());
            }
        }

        VariantQuery query = new VariantQuery()
                .study(metadataManager.getStudyName(studyId))
                .includeSample(new ArrayList<>(includeSamples));
        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(VariantField.ID, VariantField.STUDIES))
                .append(QueryOptions.SORT, true);

        String currentChromosome = null;
        int currentBatchStart = -1;
        try (VariantDBIterator iterator = engine.getDBAdaptor().iterator(query, queryOptions)) {
            while (iterator.hasNext()) {
                Variant variant = iterator.next();
                int batchStart = SampleIndexSchema.getChunkStart(variant.getStart());
                String chromosome = variant.getChromosome();

                if (currentChromosome != null
                        && (!chromosome.equals(currentChromosome) || batchStart != currentBatchStart)) {
                    flush(studyId, version, currentChromosome, currentBatchStart, builders, genotypeCount);
                }
                currentChromosome = chromosome;
                currentBatchStart = batchStart;

                StudyEntry studyEntry = variant.getStudies().get(0);
                for (int[] trio : resolvedTrios) {
                    int fatherId = trio[0];
                    int motherId = trio[1];
                    int childId = trio[2];
                    String childGt = readGt(studyEntry, sampleNameById.get(childId));
                    String fatherGt = fatherId == MISSING_SAMPLE
                            ? null
                            : readGt(studyEntry, sampleNameById.get(fatherId));
                    String motherGt = motherId == MISSING_SAMPLE
                            ? null
                            : readGt(studyEntry, sampleNameById.get(motherId));

                    FamilyIndexBuilder builder = builders.get(childId);
                    builder.addParents(childGt, fatherGt, motherGt);
                    int idx = genotypeCount.get(childId).merge(childGt, 1, Integer::sum) - 1;
                    computeMendelianError(variant, fatherId, motherId, fatherGt, motherGt, childGt, builder, idx);
                }
            }
        } catch (StorageEngineException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageEngineException("Error iterating variants for family index", e);
        }
        if (currentChromosome != null) {
            flush(studyId, version, currentChromosome, currentBatchStart, builders, genotypeCount);
        }
    }

    private static String readGt(StudyEntry studyEntry, String sampleName) {
        String gt = studyEntry.getSampleData(sampleName, "GT");
        return gt == null ? "0/0" : gt;
    }

    private void flush(int studyId, int version, String chromosome, int batchStart,
                       Map<Integer, FamilyIndexBuilder> builders,
                       Map<Integer, Map<String, Integer>> genotypeCount) throws StorageEngineException {
        for (Map.Entry<Integer, FamilyIndexBuilder> e : builders.entrySet()) {
            int childId = e.getKey();
            SampleIndexEntry familyEntry = e.getValue().buildAndResetEntry(chromosome, batchStart);
            if (familyEntry == null) {
                continue;
            }
            // Merge into the existing on-disk entry so we don't clobber genotype/annotation fields
            SampleIndexEntry existing = localAdaptor.readEntry(studyId, version, childId, chromosome, batchStart);
            if (existing == null) {
                existing = new SampleIndexEntry(childId, chromosome, batchStart);
            }
            for (Map.Entry<String, SampleIndexEntry.SampleIndexGtEntry> ge : familyEntry.getGts().entrySet()) {
                byte[] parentsIdx = ge.getValue().getParentsIndex();
                if (parentsIdx != null && parentsIdx.length > 0) {
                    existing.getGtEntry(ge.getKey()).setParentsIndex(parentsIdx);
                }
            }
            byte[] mendelian = familyEntry.getMendelianVariantsValue();
            if (mendelian != null && mendelian.length > 0) {
                existing.setMendelianVariants(mendelian);
            }
            localAdaptor.writeEntry(studyId, version, existing);
        }
        genotypeCount.values().forEach(Map::clear);
    }

    private void computeMendelianError(Variant variant, int fatherId, int motherId,
                                       String fatherGtStr, String motherGtStr, String childGtStr,
                                       FamilyIndexBuilder builder, int idx) throws IOException {
        if (childGtStr == null) {
            return;
        }
        if (fatherGtStr == null && fatherId != MISSING_SAMPLE) {
            return;
        }
        if (motherGtStr == null && motherId != MISSING_SAMPLE) {
            return;
        }
        try {
            Genotype fatherGt = fatherGtStr == null ? null : new Genotype(fatherGtStr);
            Genotype motherGt = motherGtStr == null ? null : new Genotype(motherGtStr);
            Genotype childGt = new Genotype(childGtStr);
            Integer me = MendelianError.compute(fatherGt, motherGt, childGt, variant.getChromosome());
            builder.addMendelianError(variant, childGtStr, idx, me);
        } catch (IllegalArgumentException e) {
            logger.warn("Malformed genotype, skipping: father={}, mother={}, child={}",
                    fatherGtStr, motherGtStr, childGtStr);
        }
    }
}
