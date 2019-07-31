package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.*;

/**
 * Created on 14/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexDBLoader extends AbstractHBaseDataWriter<Variant, Put> {

    private final List<Integer> sampleIds;
    private final byte[] family;
    // Map from IndexChunk -> List (following sampleIds order) of Map<Genotype, StringBuilder>
    private final Map<IndexChunk, List<Map<String, SortedSet<Variant>>>> buffer = new LinkedHashMap<>();
    private final HashSet<String> genotypes = new HashSet<>();
    private final SampleIndexToHBaseConverter converter;
    private final ObjectMap options;

    public SampleIndexDBLoader(HBaseManager hBaseManager, String tableName, List<Integer> sampleIds, byte[] family, ObjectMap options) {
        super(hBaseManager, tableName);
        this.sampleIds = sampleIds;
        this.family = family;
        converter = new SampleIndexToHBaseConverter(family);
        this.options = options;
    }

    private class IndexChunk {
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
    }

    @Override
    public boolean open() {
        super.open();
        SampleIndexSchema.createTableIfNeeded(tableName, hBaseManager, options);

        return true;
    }

    @Override
    protected List<Put> convert(List<Variant> variants) {
        for (Variant variant : variants) {
            IndexChunk indexChunk = new IndexChunk(variant.getChromosome(), (variant.getStart() / BATCH_SIZE) * BATCH_SIZE);
            int sampleIdx = 0;
            StudyEntry studyEntry = variant.getStudies().get(0);
            boolean hasGT = studyEntry.getFormat().get(0).equals("GT");
            for (List<String> samplesData : studyEntry.getSamplesData()) {
                String gt = hasGT ? samplesData.get(0) : GenotypeClass.NA_GT_VALUE;
                if (validVariant(variant) && validGenotype(gt)) {
                    genotypes.add(gt);
                    Set<Variant> variantsList = buffer
                            .computeIfAbsent(indexChunk, k -> {
                                List<Map<String, SortedSet<Variant>>> list = new ArrayList<>(sampleIds.size());
                                for (int i = 0; i < sampleIds.size(); i++) {
                                    list.add(new HashMap<>());
                                }
                                return list;
                            })
                            .get(sampleIdx)
                            .computeIfAbsent(gt, k -> new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR));
                    variantsList.add(variant);
                }
                sampleIdx++;
            }
        }

        return getPuts();
    }

    public static boolean validVariant(Variant variant) {
        return !variant.getType().equals(VariantType.NO_VARIATION);
    }

    @Override
    public boolean post() {
        try {
            // Drain buffer
            mutate(getPuts(0));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return super.post();
    }

    protected List<Put> getPuts() {
        // Leave 3 chunks in the buffer
        return getPuts(3);
    }

    protected List<Put> getPuts(int remain) {
        List<Put> puts = new LinkedList<>();

        while (buffer.size() > remain) {
            IndexChunk indexChunk = buffer.keySet().iterator().next();
            List<Map<String, SortedSet<Variant>>> sampleList = buffer.remove(indexChunk);
            ListIterator<Integer> sampleIterator = sampleIds.listIterator();
            for (Map<String, SortedSet<Variant>> gtsMap : sampleList) {
                int sampleIdx = sampleIterator.nextIndex();
                Integer sampleId = sampleIterator.next();

                byte[] rk = toRowKey(sampleId, indexChunk.chromosome, indexChunk.position);
                Put put = converter.convert(rk, gtsMap, sampleIdx);
                if (!put.isEmpty()) {
                    puts.add(put);
                }
            }
        }

        return puts;
    }

    public HashSet<String> getLoadedGenotypes() {
        return genotypes;
    }
}
