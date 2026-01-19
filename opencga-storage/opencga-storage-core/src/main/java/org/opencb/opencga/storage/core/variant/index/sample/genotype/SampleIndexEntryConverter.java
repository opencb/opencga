package org.opencb.opencga.storage.core.variant.index.sample.genotype;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
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
public class SampleIndexEntryConverter implements Task<Variant, SampleIndexEntry> {

    protected final int studyId;
    protected final List<Integer> sampleIds;
    // Map from IndexChunk -> List (following sampleIds order) of Map<Genotype, SortedSet<VariantFileIndex>>
    protected final Map<IndexChunk, Chunk> buffer = new LinkedHashMap<>();
    protected final HashSet<String> genotypes = new HashSet<>();
    protected final boolean rebuildIndex;
    protected final boolean[] multiFileIndex;
    protected final Map<Integer, Integer>[] fileIdxMap;
    protected final ObjectMap options;
    protected final SampleIndexDBAdaptor dbAdaptor;
    protected final SampleIndexVariantConverter sampleIndexVariantConverter;
    protected final boolean includeGenotype;
    protected final SampleIndexSchema schema;
    private final Integer fileId;

    public SampleIndexEntryConverter(SampleIndexDBAdaptor dbAdaptor,
                                     int studyId, List<Integer> sampleIds, ObjectMap options, SampleIndexSchema schema) {
        this(dbAdaptor, studyId, sampleIds, null, null, options, schema);
    }


    public SampleIndexEntryConverter(SampleIndexDBAdaptor dbAdaptor,
                                     int studyId, int fileId, List<Integer> sampleIds,
                                     VariantStorageEngine.SplitData splitData, ObjectMap options, SampleIndexSchema schema) {
        this(dbAdaptor, studyId, sampleIds, fileId, splitData, options, schema);
    }

    private SampleIndexEntryConverter(SampleIndexDBAdaptor dbAdaptor,
                                     int studyId, List<Integer> sampleIds, Integer fileId,
                                      VariantStorageEngine.SplitData splitData, ObjectMap options, SampleIndexSchema schema) {
        this.studyId = studyId;
        this.sampleIds = sampleIds;
        this.options = options;
        includeGenotype = YesNoAuto.parse(options, INCLUDE_GENOTYPE.key()).yesOrAuto();
        this.dbAdaptor = dbAdaptor;
        this.schema = schema;
        sampleIndexVariantConverter = new SampleIndexVariantConverter(this.schema);
        multiFileIndex = new boolean[sampleIds.size()];
        fileIdxMap = new Map[sampleIds.size()];
        if (fileId != null) {
            final boolean multiFileIndexFile;
            // Loading a single file
            this.fileId = fileId;
            if (splitData != null) {
                switch (splitData) {
                    case CHROMOSOME:
                        rebuildIndex = false;
                        multiFileIndexFile = false;
                        break;
                    case REGION:
                        rebuildIndex = true;
                        multiFileIndexFile = false;
                        break;
                    case MULTI:
                        rebuildIndex = true;
                        multiFileIndexFile = true;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown LoadSplitData " + splitData);
                }
            } else {
                rebuildIndex = false;
                multiFileIndexFile = false;
            }
            for (int i = 0; i < sampleIds.size(); i++) {
                if (multiFileIndexFile) {
                    int sampleId = sampleIds.get(i);
                    SampleMetadata sampleMetadata = dbAdaptor.getMetadataManager().getSampleMetadata(studyId, sampleId);
                    List<Integer> files = sampleMetadata.getFiles();
                    multiFileIndex[i] = true;
                    fileIdxMap[i] = Collections.singletonMap(fileId, files.indexOf(fileId));
                } else {
                    multiFileIndex[i] = false;
                    fileIdxMap[i] = Collections.singletonMap(fileId, 0);
                }
            }
        } else {
            // Considering all files
            if (splitData != null) {
                throw new IllegalArgumentException("Cannot use splitData when loading all files");
            }
            this.fileId = null;
            rebuildIndex = false;
            for (int i = 0; i < sampleIds.size(); i++) {
                Integer sampleId = sampleIds.get(i);
                List<Integer> files = dbAdaptor.getMetadataManager().getSampleMetadata(studyId, sampleId).getFiles();
                // Map all files
                Map<Integer, Integer> map = new HashMap<>();
                int idx = 0;
                for (Integer fId : files) {
                    map.put(fId, idx);
                    idx++;
                }
                fileIdxMap[i] = map;
            }
        }
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
            int idx = 0;
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
                            false, multiFileIndex[idx]);
                }
                samples.add(builder);
                idx++;
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
            if (isMerging() && !multiFileIndex[sampleIdx]) {
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
    public List<SampleIndexEntry> apply(List<Variant> variants) {
        for (Variant variant : variants) {
            IndexChunk indexChunk = new IndexChunk(variant.getChromosome(), getChunkStart(variant.getStart()));
            int sampleIdx = 0;
            StudyEntry studyEntry = variant.getStudies().get(0);
            // The variant hasGT if has the SampleDataKey GT AND the genotypes are not being excluded
            boolean hasGT = includeGenotype && !studyEntry.getSampleDataKeys().isEmpty()
                    && studyEntry.getSampleDataKeys().get(0).equals("GT");
            for (SampleEntry sample : studyEntry.getSamples()) {
                String gt = hasGT ? sample.getData().get(0) : GenotypeClass.NA_GT_VALUE;
                Integer fileIndex = sample.getFileIndex();
                Integer fileId = null;
                if (fileIndex == null) {
                    fileIndex = 0;
                    fileId = this.fileId;
                } else if (multiFileIndex[sampleIdx]) {
                    String fileName = studyEntry.getFiles().get(fileIndex).getFileId();
                    fileId = dbAdaptor.getMetadataManager().getFileId(studyId, fileName);
                }
                if (validVariant(variant) && validGenotype(gt)) {
                    genotypes.add(gt);
                    Chunk chunk = buffer.computeIfAbsent(indexChunk, Chunk::new);
                    SampleIndexVariant indexEntry = sampleIndexVariantConverter
                            .createSampleIndexVariant(sampleIdx, fileIndex, fileIdxMap[sampleIdx].get(fileId), variant);
                    chunk.addVariant(sampleIdx, gt, indexEntry);
                }
                sampleIdx++;
            }
        }

        // Leave 3 chunks in the buffer
        return convert(3);
    }

    private List<SampleIndexEntry> convert(int remain) {
        if (buffer.size() <= remain) {
            return Collections.emptyList();
        }
        List<SampleIndexEntry> entries = new ArrayList<>();
        Iterator<Map.Entry<IndexChunk, Chunk>> iterator = buffer.entrySet().iterator();
        while (buffer.size() > remain && iterator.hasNext()) {
            Map.Entry<IndexChunk, Chunk> entry = iterator.next();
            Chunk chunk = entry.getValue();
            for (SampleIndexEntryBuilder builder : chunk) {
                if (builder.isEmpty()) {
                    continue;
                }
                entries.add(builder.buildEntry());
            }
            iterator.remove();
        }
        return entries;
    }

    public static boolean validVariant(Variant variant) {
        return !variant.getType().equals(VariantType.NO_VARIATION);
    }

    @Override
    public List<SampleIndexEntry> drain() {
        // Drain buffer
        return convert(0);
    }

    public HashSet<String> getLoadedGenotypes() {
        return genotypes;
    }

    public int getSampleIndexVersion() {
        return schema.getVersion();
    }
}
