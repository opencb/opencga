package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.SplitData;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.EXCLUDE_GENOTYPES;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.*;

/**
 * Created on 14/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexDBLoader extends AbstractHBaseDataWriter<Variant, Mutation> {

    private final int studyId;
    private final List<Integer> sampleIds;
    // Map from IndexChunk -> List (following sampleIds order) of Map<Genotype, SortedSet<VariantFileIndex>>
    private final Map<IndexChunk, Chunk> buffer = new LinkedHashMap<>();
    private final HashSet<String> genotypes = new HashSet<>();
    private final boolean rebuildIndex;
    private final boolean multiFileIndex;
    private final int[] fileIdxMap;
    private final byte[] family;
    private final ObjectMap options;
    private final SampleIndexDBAdaptor dbAdaptor;
    private final VariantFileIndexConverter variantFileIndexConverter;;
    private final boolean excludeGenotypes;
    private final SampleIndexSchema schema;
    private final StudyMetadata.SampleIndexConfigurationVersioned sampleIndexConfiguration;

    public SampleIndexDBLoader(SampleIndexDBAdaptor dbAdaptor, HBaseManager hBaseManager,
                               VariantStorageMetadataManager metadataManager,
                               int studyId, int fileId, List<Integer> sampleIds,
                               SplitData splitData, ObjectMap options) {
        super(hBaseManager, dbAdaptor.getSampleIndexTableName(studyId));
        this.studyId = studyId;
        this.sampleIds = sampleIds;
        family = GenomeHelper.COLUMN_FAMILY_BYTES;
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
        excludeGenotypes = options.getBoolean(
                EXCLUDE_GENOTYPES.key(),
                EXCLUDE_GENOTYPES.defaultValue());
        this.dbAdaptor = dbAdaptor;
        sampleIndexConfiguration = dbAdaptor.getSampleIndexConfiguration(studyId);
        schema = dbAdaptor.getSchema(studyId);
        variantFileIndexConverter = new VariantFileIndexConverter(schema);
    }

    private class Chunk implements Iterable<SampleIndexEntryPutBuilder> {
        private List<SampleIndexEntryPutBuilder> samples;
        private boolean merging;

        @Override
        public Iterator<SampleIndexEntryPutBuilder> iterator() {
            return samples.iterator();
        }

        Chunk(IndexChunk indexChunk) {
            samples = new ArrayList<>(sampleIds.size());
            merging = false;
            for (Integer sampleId : sampleIds) {
                SampleIndexEntryPutBuilder builder;
                if (rebuildIndex) {
                    builder = fetchIndex(indexChunk, sampleId);
                    if (!builder.getGtSet().isEmpty()) {
                        merging = true;
                    }
                } else {
                    builder = new SampleIndexEntryPutBuilder(sampleId, indexChunk.chromosome, indexChunk.position, schema);
                }
                samples.add(builder);
            }
        }

        public boolean isMerging() {
            return merging;
        }

        public SampleIndexEntryPutBuilder get(int sampleIdx) {
            return samples.get(sampleIdx);
        }

        private SampleIndexEntryPutBuilder fetchIndex(IndexChunk indexChunk, Integer sampleId) {
            try {
                return dbAdaptor.queryByGtBuilder(studyId, sampleId, indexChunk.chromosome, indexChunk.position);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void addVariant(int sampleIdx, String gt, SampleVariantIndexEntry variantIndexEntry) {
            SampleIndexEntryPutBuilder sampleEntry = get(sampleIdx);
            if (isMerging() && !multiFileIndex) {
                // Look for the variant in any genotype to avoid duplications
                if (sampleEntry.containsVariant(variantIndexEntry)) {
                    throw new IllegalArgumentException("Already loaded variant " + variantIndexEntry.getVariant());
                }
            }
            if (schema.getFileIndex().isMultiFile(variantIndexEntry.getFileIndex())) {
                throw new IllegalArgumentException("Unexpected multi-file at variant " + variantIndexEntry.getVariant());
            }
            sampleEntry.add(gt, variantIndexEntry);
        }
    }

    private static class IndexChunk {
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
    public boolean open() {
        super.open();
        SampleIndexSchema.createTableIfNeeded(tableName, hBaseManager, options);
        return true;
    }

    @Override
    protected List<Mutation> convert(List<Variant> variants) {
        for (Variant variant : variants) {
            IndexChunk indexChunk = new IndexChunk(variant.getChromosome(), getChunkStart(variant.getStart()));
            int sampleIdx = 0;
            StudyEntry studyEntry = variant.getStudies().get(0);
            // The variant hasGT if has the SampleDataKey GT AND the genotypes are not being excluded
            boolean hasGT = studyEntry.getSampleDataKeys().get(0).equals("GT") && !excludeGenotypes;
            for (SampleEntry sample : studyEntry.getSamples()) {
                String gt = hasGT ? sample.getData().get(0) : GenotypeClass.NA_GT_VALUE;
                if (validVariant(variant) && validGenotype(gt)) {
                    genotypes.add(gt);
                    Chunk chunk = buffer.computeIfAbsent(indexChunk, Chunk::new);
                    BitBuffer fileIndexValue = variantFileIndexConverter.createFileIndexValue(sampleIdx, fileIdxMap[sampleIdx], variant);
                    SampleVariantIndexEntry indexEntry = new SampleVariantIndexEntry(variant, fileIndexValue);
                    chunk.addVariant(sampleIdx, gt, indexEntry);
                }
                sampleIdx++;
            }
        }

        return getMutations();
    }


    public static boolean validVariant(Variant variant) {
        return !variant.getType().equals(VariantType.NO_VARIATION);
    }

    @Override
    public boolean post() {
        try {
            // Drain buffer
            mutate(getMutations(0));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return super.post();
    }

    protected List<Mutation> getMutations() {
        // Leave 3 chunks in the buffer
        return getMutations(3);
    }

    protected List<Mutation> getMutations(int remain) {
        List<Mutation> mutations = new LinkedList<>();

        while (buffer.size() > remain) {
            IndexChunk indexChunk = buffer.keySet().iterator().next();
            Chunk chunk = buffer.remove(indexChunk);
            for (SampleIndexEntryPutBuilder builder : chunk) {
                Put put = builder.build();
                if (!put.isEmpty()) {
                    mutations.add(put);

                    if (chunk.isMerging() && !builder.isEmpty()) {
                        Delete delete = new Delete(put.getRow());
                        for (String gt : builder.getGtSet()) {
                            delete.addColumn(family, SampleIndexSchema.toAnnotationClinicalIndexColumn(gt));
                            delete.addColumn(family, SampleIndexSchema.toAnnotationBiotypeIndexColumn(gt));
                            delete.addColumn(family, SampleIndexSchema.toAnnotationConsequenceTypeIndexColumn(gt));
                            delete.addColumn(family, SampleIndexSchema.toAnnotationCtBtIndexColumn(gt));
                            delete.addColumn(family, SampleIndexSchema.toAnnotationIndexColumn(gt));
                            delete.addColumn(family, SampleIndexSchema.toAnnotationPopFreqIndexColumn(gt));
                            delete.addColumn(family, SampleIndexSchema.toAnnotationIndexCountColumn(gt));
                            delete.addColumn(family, SampleIndexSchema.toParentsGTColumn(gt));
                        }
                        delete.addColumn(family, SampleIndexSchema.toMendelianErrorColumn());
                        mutations.add(delete);
                    }
                }
            }
        }

        return mutations;
    }

    public HashSet<String> getLoadedGenotypes() {
        return genotypes;
    }

    public int getSampleIndexVersion() {
        return sampleIndexConfiguration.getVersion();
    }
}
