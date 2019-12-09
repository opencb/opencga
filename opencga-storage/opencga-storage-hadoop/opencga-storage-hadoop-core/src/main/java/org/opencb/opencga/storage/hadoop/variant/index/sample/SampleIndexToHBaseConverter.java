package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;

import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.VariantFileIndexConverter.VariantFileIndex;

/**
 * Created on 31/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexToHBaseConverter {

    private final byte[] family;
    private final SampleIndexVariantBiConverter variantConverter;
    private final VariantFileIndexConverter fileIndexConverter;

    public SampleIndexToHBaseConverter(byte[] family) {
        this.family = family;
        variantConverter = new SampleIndexVariantBiConverter();
        fileIndexConverter = new VariantFileIndexConverter();
    }

    public Put convert(byte[] rk, Map<String, SortedSet<Variant>> gtsMap, int sampleIdx) {
        Put put = new Put(rk);

        for (Map.Entry<String, SortedSet<Variant>> gtsEntry : gtsMap.entrySet()) {
            SortedSet<Variant> variants = gtsEntry.getValue();
            String gt = gtsEntry.getKey();

            byte[] variantsBytes = variantConverter.toBytes(variants);
            byte[] fileMask = new byte[variants.size()];
            int i = 0;
            for (Variant variant : variants) {
                fileMask[i] = fileIndexConverter.createFileIndexValue(sampleIdx, variant);
                i++;
            }

            put.addColumn(family, SampleIndexSchema.toGenotypeColumn(gt), variantsBytes);
            put.addColumn(family, SampleIndexSchema.toGenotypeCountColumn(gt), Bytes.toBytes(variants.size()));
            put.addColumn(family, SampleIndexSchema.toFileIndexColumn(gt), fileMask);
        }


        return put;
    }

    public Put convert(byte[] rk, Map<String, SortedSet<VariantFileIndex>> gtsMap) {
        Put put = new Put(rk);

        for (Map.Entry<String, SortedSet<VariantFileIndex>> gtsEntry : gtsMap.entrySet()) {
            SortedSet<VariantFileIndex> variants = gtsEntry.getValue();
            String gt = gtsEntry.getKey();

            byte[] variantsBytes = variantConverter.toBytes(variants.stream()
                    .map(VariantFileIndex::getVariant)
                    .collect(Collectors.toList()));
            byte[] fileMask = new byte[variants.size()];
            int i = 0;
            for (VariantFileIndex variant : variants) {
                fileMask[i] = variant.getFileIndex();
                i++;
            }

            put.addColumn(family, SampleIndexSchema.toGenotypeColumn(gt), variantsBytes);
            put.addColumn(family, SampleIndexSchema.toGenotypeCountColumn(gt), Bytes.toBytes(variants.size()));
            put.addColumn(family, SampleIndexSchema.toFileIndexColumn(gt), fileMask);
        }

        return put;
    }

}
