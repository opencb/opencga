package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.sample.VariantFileIndexConverter.VariantFileIndex;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

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
    private final SampleIndexToHBaseConverter converter;
    private final boolean rebuildIndex;
    private final byte[] family;
    private final ObjectMap options;
    private final SampleIndexDBAdaptor dbAdaptor;

    public SampleIndexDBLoader(SampleIndexDBAdaptor dbAdaptor, HBaseManager hBaseManager,
                               String tableName, int studyId, List<Integer> sampleIds,
                               byte[] columnFamily, boolean splitData, ObjectMap options) {
        super(hBaseManager, tableName);
        this.studyId = studyId;
        this.sampleIds = sampleIds;
        converter = new SampleIndexToHBaseConverter(columnFamily);
        family = columnFamily;
        this.options = options;
        this.rebuildIndex = splitData;
        this.dbAdaptor = dbAdaptor;
    }

    private class Chunk implements Iterable<Map<String, SortedSet<VariantFileIndex>>> {
        private List<Map<String, SortedSet<VariantFileIndex>>> samples;
        private boolean merging;

        @Override
        public Iterator<Map<String, SortedSet<VariantFileIndex>>> iterator() {
            return samples.iterator();
        }

        public Chunk(IndexChunk indexChunk) {
            samples = new ArrayList<>(sampleIds.size());
            merging = false;
            for (int i = 0; i < sampleIds.size(); i++) {
                Map<String, SortedSet<VariantFileIndex>> map;
                if (rebuildIndex) {
                    map = fetchIndex(indexChunk, sampleIds.get(i));
                    if (map.isEmpty()) {
                        map = new HashMap<>();
                    } else {
                        merging = true;
                    }
                } else {
                    map = new HashMap<>();
                }
                samples.add(map);
            }
        }

        public boolean isMerging() {
            return merging;
        }

        public Map<String, SortedSet<VariantFileIndex>> get(int sampleIdx) {
            return samples.get(sampleIdx);
        }

        private Map<String, SortedSet<VariantFileIndex>> fetchIndex(IndexChunk indexChunk, Integer sampleId) {
            try {
                return dbAdaptor.queryByGtWithFile(studyId, sampleId, indexChunk.chromosome, indexChunk.position);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void addVariant(int sampleIdx, String gt, VariantFileIndex variantfileIndex) {
            Map<String, SortedSet<VariantFileIndex>> sampleMap = get(sampleIdx);
            SortedSet<VariantFileIndex> variantsList = sampleMap.computeIfAbsent(gt, k -> new TreeSet<>());
            if (isMerging()) {
                // Look for the variant in any genotype to avoid duplications
                for (Map.Entry<String, SortedSet<VariantFileIndex>> entry : sampleMap.entrySet()) {
                    if (entry.getValue().contains(variantfileIndex)) {
                        throw new IllegalArgumentException("Already loaded variant " + variantfileIndex.getVariant());
                    }
                }
            }
            variantsList.add(variantfileIndex);
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
            IndexChunk indexChunk = new IndexChunk(variant.getChromosome(), (variant.getStart() / BATCH_SIZE) * BATCH_SIZE);
            int sampleIdx = 0;
            StudyEntry studyEntry = variant.getStudies().get(0);
            boolean hasGT = studyEntry.getFormat().get(0).equals("GT");
            for (List<String> samplesData : studyEntry.getSamplesData()) {
                String gt = hasGT ? samplesData.get(0) : GenotypeClass.NA_GT_VALUE;
                if (validVariant(variant) && validGenotype(gt)) {
                    genotypes.add(gt);
                    Chunk chunk = buffer.computeIfAbsent(indexChunk, Chunk::new);
                    VariantFileIndex variantfileIndex = new VariantFileIndexConverter().toVariantFileIndex(sampleIdx, variant);
                    chunk.addVariant(sampleIdx, gt, variantfileIndex);
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
            ListIterator<Integer> sampleIterator = sampleIds.listIterator();
            for (Map<String, SortedSet<VariantFileIndex>> gtsMap : chunk) {
                Integer sampleId = sampleIterator.next();

                byte[] rk = toRowKey(sampleId, indexChunk.chromosome, indexChunk.position);
                Put put = converter.convert(rk, gtsMap);
                if (!put.isEmpty()) {
                    mutations.add(put);

                    if (chunk.isMerging() && !gtsMap.isEmpty()) {
                        Delete delete = new Delete(put.getRow());
                        for (String gt : gtsMap.keySet()) {
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
}
