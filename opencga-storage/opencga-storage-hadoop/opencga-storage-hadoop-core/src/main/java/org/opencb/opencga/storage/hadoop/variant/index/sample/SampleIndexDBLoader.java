package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexConverter.toGenotypeColumn;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexConverter.toGenotypeCountColumn;

/**
 * Created on 14/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexDBLoader extends AbstractHBaseDataWriter<Variant, Put> {

    public static final int BATCH_SIZE = 1_000_000;
    private final List<Integer> sampleIds;
    private final byte[] family;
    // Map from IndexChunk -> List (following sampleIds order) of Map<Genotype, StringBuilder>
    private final Map<IndexChunk, List<Map<String, Set<String>>>> buffer = new LinkedHashMap<>();
    private final HashSet<String> genotypes = new HashSet<>();

    public SampleIndexDBLoader(HBaseManager hBaseManager, String tableName, List<Integer> sampleIds, byte[] family) {
        super(hBaseManager, tableName);
        this.sampleIds = sampleIds;
        this.family = family;
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

        try {
            hBaseManager.createTableIfNeeded(tableName, family, Compression.Algorithm.NONE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    protected List<Put> convert(List<Variant> variants) {
        for (Variant variant : variants) {
            IndexChunk indexChunk = new IndexChunk(variant.getChromosome(), (variant.getStart() / BATCH_SIZE) * BATCH_SIZE);
            int sampleIdx = 0;
            for (List<String> samplesData : variant.getStudies().get(0).getSamplesData()) {
                String gt = samplesData.get(0);
                if (validVariant(variant) && validGenotype(gt)) {
                    genotypes.add(gt);
                    Set<String> variantsList = buffer
                            .computeIfAbsent(indexChunk, k -> {
                                List<Map<String, Set<String>>> list = new ArrayList<>(sampleIds.size());
                                for (int i = 0; i < sampleIds.size(); i++) {
                                    list.add(new HashMap<>());
                                }
                                return list;
                            })
                            .get(sampleIdx)
                            .computeIfAbsent(gt, k -> new TreeSet<>());
                    variantsList.add(variant.toString());
                }
                sampleIdx++;
            }
        }

        return getPuts();
    }

    public static boolean validVariant(Variant variant) {
        return !variant.getType().equals(VariantType.NO_VARIATION);
    }

    /**
     * Genotypes HOM_REF and MISSING are not loaded in the SampleIndexTable.
     *
     * @param gt genotype
     * @return is valid genotype
     */
    public static boolean validGenotype(String gt) {
        return gt != null
                && !gt.isEmpty()
                && !gt.equals("0/0")
                && !gt.equals("0|0")
                && !gt.equals("./.")
                && !gt.equals(".");
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
            List<Map<String, Set<String>>> sampleList = buffer.remove(indexChunk);
            Iterator<Integer> sampleIterator = sampleIds.iterator();
            for (Map<String, Set<String>> gtsMap : sampleList) {
                Integer sampleId = sampleIterator.next();

                byte[] rk = SampleIndexConverter.toRowKey(sampleId, indexChunk.chromosome, indexChunk.position);
                Put put = new Put(rk);

                for (Map.Entry<String, Set<String>> gtsEntry : gtsMap.entrySet()) {
                    put.addColumn(family, toGenotypeColumn(gtsEntry.getKey()), Bytes.toBytes(String.join(",", gtsEntry.getValue())));
                    put.addColumn(family, toGenotypeCountColumn(gtsEntry.getKey()), Bytes.toBytes(gtsEntry.getValue().size()));
                }
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
