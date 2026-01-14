package org.opencb.opencga.storage.core.variant.index.sample.genotype;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.INCLUDE_GENOTYPE;
import static org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema.getChunkStart;
import static org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema.validGenotype;

/**
 * Writer for Sample Index entries.
 *
 * It buffers entries by chunks, and writes them when the buffer exceeds a certain size.
 *
 * @author Jacobo Coll &lt;
 */
public abstract class SampleIndexWriter implements DataWriter<Variant> {

    protected final int studyId;
    protected final List<Integer> sampleIds;
    // Map from IndexChunk -> List (following sampleIds order) of Map<Genotype, SortedSet<VariantFileIndex>>
    protected final Map<IndexChunk, Chunk> buffer = new LinkedHashMap<>();
    protected final HashSet<String> genotypes = new HashSet<>();
    protected final boolean rebuildIndex;
    protected final boolean multiFileIndex;
    protected final int[] fileIdxMap;
    protected final ObjectMap options;
    protected final SampleIndexDBAdaptor dbAdaptor;
    protected final SampleIndexVariantConverter sampleIndexVariantConverter;
    protected final boolean includeGenotype;
    protected final SampleIndexSchema schema;

    public SampleIndexWriter(SampleIndexDBAdaptor dbAdaptor,
                             VariantStorageMetadataManager metadataManager,
                             int studyId, int fileId, List<Integer> sampleIds,
                             VariantStorageEngine.SplitData splitData, ObjectMap options, SampleIndexSchema schema) {
        this.studyId = studyId;
        this.sampleIds = sampleIds;
        this.options = options;
        if (splitData != null) {
            switch (splitData) {
                case CHROMOSOME:
                    rebuildIndex = false;
                    multiFileIndex = false;
                    break;
                case REGION:
                    rebuildIndex = true;
                    multiFileIndex = false;
                    break;
                case MULTI:
                    rebuildIndex = true;
                    multiFileIndex = true;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown LoadSplitData " + splitData);
            }
        } else {
            rebuildIndex = false;
            multiFileIndex = false;
        }
        fileIdxMap = new int[sampleIds.size()];
        if (multiFileIndex) {
            int i = 0;
            for (Integer sampleId : sampleIds) {
                fileIdxMap[i] = metadataManager.getSampleMetadata(studyId, sampleId).getFiles().indexOf(fileId);
                i++;
            }
        }
        includeGenotype = YesNoAuto.parse(options, INCLUDE_GENOTYPE.key()).yesOrAuto();
        this.dbAdaptor = dbAdaptor;
        this.schema = schema;
        sampleIndexVariantConverter = new SampleIndexVariantConverter(this.schema);
    }

    protected class Chunk implements Iterable<SampleIndexEntryBuilder> {
        private final List<SampleIndexEntryBuilder> samples;
        private boolean merging;

        @Override
        public Iterator<SampleIndexEntryBuilder> iterator() {
            return samples.iterator();
        }

        Chunk(IndexChunk indexChunk) {
            samples = new ArrayList<>(sampleIds.size());
            merging = false;
            for (Integer sampleId : sampleIds) {
                SampleIndexEntryBuilder builder;
                if (rebuildIndex) {
                    builder = fetchIndex(indexChunk, sampleId);
                    if (!builder.getGtSet().isEmpty()) {
                        merging = true;
                    }
                } else {
                    // This loader can't ensure an ordered input data to the SampleIndexEntryBuilder.
                    builder = new SampleIndexEntryBuilder(sampleId, indexChunk.chromosome, indexChunk.position, schema,
                            false, multiFileIndex);
                }
                samples.add(builder);
            }
        }

        public boolean isMerging() {
            return merging;
        }

        public SampleIndexEntryBuilder get(int sampleIdx) {
            return samples.get(sampleIdx);
        }

        private SampleIndexEntryBuilder fetchIndex(IndexChunk indexChunk, Integer sampleId) {
            try {
                return dbAdaptor.queryByGtBuilder(studyId, sampleId, indexChunk.chromosome, indexChunk.position, schema);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void addVariant(int sampleIdx, String gt, SampleIndexVariant variantIndexEntry) {
            SampleIndexEntryBuilder sampleEntry = get(sampleIdx);
            if (isMerging() && !multiFileIndex) {
                // Look for the variant in any genotype to avoid duplications
                if (sampleEntry.containsVariant(variantIndexEntry)) {
                    throw new IllegalArgumentException("Already loaded variant " + variantIndexEntry.getVariant());
                }
            }
            if (variantIndexEntry.getFilesIndex().size() > 1
                    || schema.getFileIndex().isMultiFile(variantIndexEntry.getFilesIndex().get(0))) {
                throw new IllegalArgumentException("Unexpected multi-file at variant " + variantIndexEntry.getVariant());
            }
            sampleEntry.add(gt, variantIndexEntry);
        }
    }

    protected static class IndexChunk {
        private final String chromosome;
        private final Integer position;

        IndexChunk(String chromosome, Integer position) {
            this.chromosome = chromosome;
            this.position = position;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IndexChunk)) {
                return false;
            }
            IndexChunk that = (IndexChunk) o;
            return Objects.equals(chromosome, that.chromosome)
                    && Objects.equals(position, that.position);
        }

        @Override
        public int hashCode() {
            return Objects.hash(chromosome, position);
        }

        @Override
        public String toString() {
            return chromosome + ":" + position;
        }
    }

    @Override
    public boolean write(List<Variant> variants) {
        for (Variant variant : variants) {
            IndexChunk indexChunk = new IndexChunk(variant.getChromosome(), getChunkStart(variant.getStart()));
            int sampleIdx = 0;
            StudyEntry studyEntry = variant.getStudies().get(0);
            // The variant hasGT if has the SampleDataKey GT AND the genotypes are not being excluded
            boolean hasGT = includeGenotype && !studyEntry.getSampleDataKeys().isEmpty()
                    && studyEntry.getSampleDataKeys().get(0).equals("GT");
            for (SampleEntry sample : studyEntry.getSamples()) {
                String gt = hasGT ? sample.getData().get(0) : GenotypeClass.NA_GT_VALUE;
                if (validVariant(variant) && validGenotype(gt)) {
                    genotypes.add(gt);
                    Chunk chunk = buffer.computeIfAbsent(indexChunk, Chunk::new);
                    SampleIndexVariant indexEntry = sampleIndexVariantConverter
                            .createSampleIndexVariant(sampleIdx, fileIdxMap[sampleIdx], variant);
                    chunk.addVariant(sampleIdx, gt, indexEntry);
                }
                sampleIdx++;
            }
        }

        // Leave 3 chunks in the buffer
        write(3);
        return true;
    }

    protected abstract void write(int remain);

    public static boolean validVariant(Variant variant) {
        return !variant.getType().equals(VariantType.NO_VARIATION);
    }

    @Override
    public boolean post() {
        // Drain buffer
        write(0);
        return true;
    }

    public HashSet<String> getLoadedGenotypes() {
        return genotypes;
    }

    public int getSampleIndexVersion() {
        return schema.getVersion();
    }
}
