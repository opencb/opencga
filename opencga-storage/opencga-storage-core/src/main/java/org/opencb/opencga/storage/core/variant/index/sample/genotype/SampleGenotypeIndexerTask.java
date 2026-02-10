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
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntryChunk;
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
 * This class is not thread safe.
 * This class assumes that variants from the same chunk will be processed together.
 * Variants must be provided ordered by chromosome and position.
 *
 * @author Jacobo Coll &lt;
 */
public class SampleGenotypeIndexerTask implements Task<Variant, SampleIndexEntry> {

    protected final int studyId;
    protected final List<Integer> sampleIds;
    // Map from IndexChunk -> List (following sampleIds order) of Map<Genotype, SortedSet<VariantFileIndex>>
    protected final Map<SampleIndexEntryChunk, Chunk> buffer = new LinkedHashMap<>();
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

    public SampleGenotypeIndexerTask(SampleIndexDBAdaptor dbAdaptor,
                                     int studyId, List<Integer> sampleIds, ObjectMap options, SampleIndexSchema schema) {
        this(dbAdaptor, studyId, sampleIds, null, null, options, schema);
    }


    public SampleGenotypeIndexerTask(SampleIndexDBAdaptor dbAdaptor,
                                     int studyId, int fileId, List<Integer> sampleIds,
                                     VariantStorageEngine.SplitData splitData, ObjectMap options, SampleIndexSchema schema) {
        this(dbAdaptor, studyId, sampleIds, fileId, splitData, options, schema);
    }

    private SampleGenotypeIndexerTask(SampleIndexDBAdaptor dbAdaptor,
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

        Chunk(SampleIndexEntryChunk indexChunk) {
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
                    builder = new SampleIndexEntryBuilder(sampleId, indexChunk.getChromosome(), indexChunk.getBatchStart(), schema,
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

        private SampleIndexEntryBuilder fetchIndex(SampleIndexEntryChunk indexChunk, Integer sampleId) {
            try {
                return dbAdaptor.queryByGtBuilder(studyId, sampleId, indexChunk.getChromosome(), indexChunk.getBatchStart(), schema);
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

    @Override
    public List<SampleIndexEntry> apply(List<Variant> variants) {
        for (Variant variant : variants) {
            SampleIndexEntryChunk indexChunk = new SampleIndexEntryChunk(variant.getChromosome(), getChunkStart(variant.getStart()));
            int sampleIdx = 0;
            StudyEntry studyEntry = variant.getStudies().get(0);
            // The variant hasGT if has the SampleDataKey GT AND the genotypes are not being excluded
            boolean hasGT = includeGenotype && !studyEntry.getSampleDataKeys().isEmpty()
                    && studyEntry.getSampleDataKeys().get(0).equals("GT");
            for (SampleEntry sample : studyEntry.getSamples()) {
                String gt = hasGT ? sample.getData().get(0) : GenotypeClass.NA_GT_VALUE;
                if (validVariant(variant) && validGenotype(gt)) {
                    final int fileIndex;
                    final int filePosition;
                    if (sample.getFileIndex() == null) {
                        filePosition = fileIdxMap[sampleIdx].get(fileId);
                        fileIndex = 0;
                    } else {
                        fileIndex = sample.getFileIndex();
                        if (multiFileIndex[sampleIdx]) {
                            String fileName = studyEntry.getFiles().get(fileIndex).getFileId();
                            int fileId = dbAdaptor.getMetadataManager().getFileId(studyId, fileName);
                            filePosition = fileIdxMap[sampleIdx].get(fileId);
                        } else {
                            // Single file per sample
                            filePosition = 0;
                        }
                    }
                    genotypes.add(gt);
                    Chunk chunk = buffer.computeIfAbsent(indexChunk, Chunk::new);
                    SampleIndexVariant indexEntry = sampleIndexVariantConverter
                            .createSampleIndexVariant(sampleIdx, fileIndex, filePosition, variant);
                    chunk.addVariant(sampleIdx, gt, indexEntry);
                }
                sampleIdx++;
            }
        }

        // Leave 3 chunks in the buffer
        return convert(3);
    }

    @Override
    public List<SampleIndexEntry> drain() {
        // Drain buffer
        return convert(0);
    }

    private List<SampleIndexEntry> convert(int remain) {
        if (buffer.size() <= remain) {
            return Collections.emptyList();
        }
        List<SampleIndexEntry> entries = new ArrayList<>();
        Iterator<Map.Entry<SampleIndexEntryChunk, Chunk>> iterator = buffer.entrySet().iterator();
        while (buffer.size() > remain && iterator.hasNext()) {
            Map.Entry<SampleIndexEntryChunk, Chunk> entry = iterator.next();
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

    public HashSet<String> getLoadedGenotypes() {
        return genotypes;
    }

    public int getSampleIndexVersion() {
        return schema.getVersion();
    }
}
