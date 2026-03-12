package org.opencb.opencga.storage.mongodb.variant.index.sample.family;

import org.bson.Document;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.index.sample.family.FamilyIndexBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntryChunk;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema.getChunkStart;

/**
 * MongoDB-specific {@link Task} that builds sample family index (parent genotype encoding + Mendelian errors)
 * directly from raw MongoDB Documents returned by the nativeIterator.
 *
 * <p>Mirrors the logic of {@code FamilyIndexDriver.FamilyIndexMapper} (the HBase MapReduce equivalent).
 * For each variant, it extracts per-sample genotypes from {@code files[].mgt}, then for each trio
 * computes parent GT encoding and Mendelian errors using {@link FamilyIndexBuilder}.
 */
public class MongoDBSampleFamilyIndexerTask implements Task<Document, SampleIndexEntry> {

    private static final int MISSING_SAMPLE = -1;

    private final Logger logger = LoggerFactory.getLogger(MongoDBSampleFamilyIndexerTask.class);

    private final int studyId;
    /** Per-trio [fatherId, motherId, childId]; -1 = MISSING. */
    private final List<int[]> trios;
    /** Per-trio: true if any parent is from a different file than the child (unknown parent GT possible). */
    private final List<Boolean> trioHasUnknownParentGenotypes;
    /** One FamilyIndexBuilder per childId, accumulates across all variants in a chunk. */
    private final Map<Integer, FamilyIndexBuilder> builders;
    /** Per-childId: GT string → occurrence count within the current chunk. */
    private final Map<Integer, Map<String, Integer>> genotypeCount;

    private SampleIndexEntryChunk currentChunk = null;

    public MongoDBSampleFamilyIndexerTask(MongoDBSampleIndexDBAdaptor sampleIndexDBAdaptor,
                                          int studyId, List<Trio> trioList, ObjectMap options) {
        this.studyId = studyId;
        VariantStorageMetadataManager metadataManager = sampleIndexDBAdaptor.getMetadataManager();

        trios = new ArrayList<>(trioList.size());
        trioHasUnknownParentGenotypes = new ArrayList<>(trioList.size());
        builders = new HashMap<>(trioList.size());
        genotypeCount = new HashMap<>(trioList.size());

        for (Trio trio : trioList) {
            int fatherId = trio.getFather() == null ? MISSING_SAMPLE
                    : metadataManager.getSampleId(studyId, trio.getFather());
            int motherId = trio.getMother() == null ? MISSING_SAMPLE
                    : metadataManager.getSampleId(studyId, trio.getMother());
            int childId = metadataManager.getSampleId(studyId, trio.getChild());

            trios.add(new int[]{fatherId, motherId, childId});

            SampleMetadata childMetadata = metadataManager.getSampleMetadata(studyId, childId);
            List<Integer> childFiles = childMetadata.getFiles();
            boolean parentsInSeparatedFile = false;
            if (fatherId != MISSING_SAMPLE) {
                List<Integer> fatherFiles = metadataManager.getSampleMetadata(studyId, fatherId).getFiles();
                if (fatherFiles.size() != childFiles.size() || !fatherFiles.containsAll(childFiles)) {
                    parentsInSeparatedFile = true;
                }
            }
            if (motherId != MISSING_SAMPLE) {
                List<Integer> motherFiles = metadataManager.getSampleMetadata(studyId, motherId).getFiles();
                if (motherFiles.size() != childFiles.size() || !motherFiles.containsAll(childFiles)) {
                    parentsInSeparatedFile = true;
                }
            }
            trioHasUnknownParentGenotypes.add(parentsInSeparatedFile);
            builders.put(childId, new FamilyIndexBuilder(childId));
            genotypeCount.put(childId, new HashMap<>());
        }
    }

    @Override
    public List<SampleIndexEntry> apply(List<Document> docs) throws Exception {
        List<SampleIndexEntry> result = new ArrayList<>();
        for (Document doc : docs) {
            String typeStr = doc.getString(DocumentToVariantConverter.TYPE_FIELD);
            if (VariantType.NO_VARIATION.name().equals(typeStr)) {
                continue;
            }

            Variant variant = buildVariant(doc);
            SampleIndexEntryChunk chunk = new SampleIndexEntryChunk(
                    variant.getChromosome(), getChunkStart(variant.getStart()));

            if (currentChunk != null && !currentChunk.equals(chunk)) {
                flush(result);
            }
            currentChunk = chunk;

            processVariant(variant, doc);
        }
        return result;
    }

    @Override
    public List<SampleIndexEntry> drain() throws Exception {
        List<SampleIndexEntry> result = new ArrayList<>();
        flush(result);
        return result;
    }

    private void flush(List<SampleIndexEntry> result) {
        if (currentChunk == null) {
            return;
        }
        for (FamilyIndexBuilder builder : builders.values()) {
            SampleIndexEntry entry = builder.buildAndResetEntry(currentChunk.getChromosome(), currentChunk.getBatchStart());
            if (entry != null) {
                result.add(entry);
            }
        }
        for (Map<String, Integer> counts : genotypeCount.values()) {
            counts.clear();
        }
        currentChunk = null;
    }

    private void processVariant(Variant variant, Document doc) throws IOException {
        // Build GT map from files[].mgt, detecting discrepancies for samples appearing in multiple files
        Map<Integer, String> gtMap = new HashMap<>();
        Map<Integer, Set<String>> discrepanciesGtMap = new HashMap<>();

        List<Document> fileDocs = doc.getList(DocumentToVariantConverter.FILES_FIELD, Document.class);
        if (fileDocs != null) {
            for (Document fileDoc : fileDocs) {
                Integer sid = fileDoc.getInteger(DocumentToStudyEntryConverter.STUDYID_FIELD);
                if (sid == null || sid != studyId) {
                    continue;
                }
                Document mgt = fileDoc.get(DocumentToStudyEntryConverter.FILE_GENOTYPE_FIELD, Document.class);
                if (mgt == null) {
                    continue;
                }
                for (Map.Entry<String, Object> mgtEntry : mgt.entrySet()) {
                    String gt = DocumentToSamplesConverter.genotypeToDataModelType(mgtEntry.getKey());
                    @SuppressWarnings("unchecked")
                    List<Integer> mgtSampleIds = (List<Integer>) mgtEntry.getValue();
                    for (Integer sampleId : mgtSampleIds) {
                        String oldGt = gtMap.put(sampleId, gt);
                        if (oldGt != null && !oldGt.equals(gt)) {
                            Set<String> gts = discrepanciesGtMap.computeIfAbsent(sampleId, s -> new HashSet<>());
                            gts.add(oldGt);
                            gts.add(gt);
                        }
                    }
                }
            }
        }

        // For each trio, compute parent GTs and Mendelian errors
        for (int i = 0; i < trios.size(); i++) {
            int[] trio = trios.get(i);
            int father = trio[0];
            int mother = trio[1];
            int child = trio[2];

            FamilyIndexBuilder builder = builders.get(child);
            String defaultGenotype = trioHasUnknownParentGenotypes.get(i) ? null : "0/0";

            Set<String> fatherDiscrepancies = discrepanciesGtMap.get(father);
            Set<String> motherDiscrepancies = discrepanciesGtMap.get(mother);
            Set<String> childDiscrepancies = discrepanciesGtMap.get(child);

            if (fatherDiscrepancies == null && motherDiscrepancies == null && childDiscrepancies == null) {
                // Simple path: no discrepancies
                String fatherGtStr = father == MISSING_SAMPLE ? null : gtMap.getOrDefault(father, defaultGenotype);
                String motherGtStr = mother == MISSING_SAMPLE ? null : gtMap.getOrDefault(mother, defaultGenotype);
                String childGtStr = gtMap.getOrDefault(child, "0/0");

                builder.addParents(childGtStr, fatherGtStr, motherGtStr);
                int idx = genotypeCount.get(child).merge(childGtStr, 1, Integer::sum) - 1;
                computeMendelianError(variant, father, mother, fatherGtStr, motherGtStr, childGtStr, builder, idx);
            } else {
                // Discrepancy path: iterate all GT combinations
                if (fatherDiscrepancies == null) {
                    fatherDiscrepancies = Collections.singleton(
                            father == MISSING_SAMPLE ? null : gtMap.getOrDefault(father, defaultGenotype));
                }
                if (motherDiscrepancies == null) {
                    motherDiscrepancies = Collections.singleton(
                            mother == MISSING_SAMPLE ? null : gtMap.getOrDefault(mother, defaultGenotype));
                }
                if (childDiscrepancies == null) {
                    childDiscrepancies = Collections.singleton(gtMap.getOrDefault(child, "0/0"));
                }
                for (String childGtStr : childDiscrepancies) {
                    int idx = genotypeCount.get(child).merge(childGtStr, 1, Integer::sum) - 1;
                    builder.addParents(childGtStr, fatherDiscrepancies, motherDiscrepancies);
                    for (String fatherGtStr : fatherDiscrepancies) {
                        for (String motherGtStr : motherDiscrepancies) {
                            computeMendelianError(variant, father, mother, fatherGtStr, motherGtStr, childGtStr, builder, idx);
                        }
                    }
                }
            }
        }
    }

    private void computeMendelianError(Variant variant, int father, int mother,
                                       String fatherGtStr, String motherGtStr, String childGtStr,
                                       FamilyIndexBuilder builder, int idx) throws IOException {
        if ((fatherGtStr != null || father == MISSING_SAMPLE)
                && (motherGtStr != null || mother == MISSING_SAMPLE)
                && childGtStr != null) {
            Genotype fatherGt;
            Genotype motherGt;
            Genotype childGt;
            try {
                fatherGt = fatherGtStr == null ? null : new Genotype(fatherGtStr);
                motherGt = motherGtStr == null ? null : new Genotype(motherGtStr);
                childGt = new Genotype(childGtStr);
            } catch (IllegalArgumentException e) {
                logger.warn("Malformed genotype, skipping: father={}, mother={}, child={}", fatherGtStr, motherGtStr, childGtStr);
                return;
            }
            Integer me = MendelianError.compute(fatherGt, motherGt, childGt, variant.getChromosome());
            builder.addMendelianError(variant, childGtStr, idx, me);
        }
    }

    private static Variant buildVariant(Document doc) {
        Variant v = new Variant(
                doc.getString(DocumentToVariantConverter.CHROMOSOME_FIELD),
                doc.getInteger(DocumentToVariantConverter.START_FIELD),
                doc.getInteger(DocumentToVariantConverter.END_FIELD),
                doc.getString(DocumentToVariantConverter.REFERENCE_FIELD),
                doc.getString(DocumentToVariantConverter.ALTERNATE_FIELD));
        String typeStr = doc.getString(DocumentToVariantConverter.TYPE_FIELD);
        if (typeStr != null) {
            v.setType(VariantType.valueOf(typeStr));
        }
        return v;
    }
}
