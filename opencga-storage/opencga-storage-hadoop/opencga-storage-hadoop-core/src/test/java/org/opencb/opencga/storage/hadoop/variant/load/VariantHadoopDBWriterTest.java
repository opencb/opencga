package org.opencb.opencga.storage.hadoop.variant.load;

import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHbaseTransformTask;

import java.util.*;

import static org.junit.Assert.*;

public class VariantHadoopDBWriterTest {

    @Test
    public void testFilterVariantsNotFromThisSlice() {
        List<Variant> variants = Arrays.asList(
                new Variant("1:2998:AAAAAAAAAA:T"),
                new Variant("1", 3000, 2999, "", "T"),
                new Variant("1", 3001, 3000, "", "T"));
        List<Variant> filteredVariants = new ArrayList<>(variants.size());
        int chunkSize = 1000;

        Map<Long, List<Variant>> slices = new TreeMap<>();
        for (Variant variant : variants) {
            for (long l : VariantHbaseTransformTask.getCoveredSlicePositions(variant.getStart(), variant.getEnd(), chunkSize)) {
                slices.computeIfAbsent(l, k -> new LinkedList<>()).add(variant);
            }
        }

        slices.forEach((sliceStart, slice) -> {
            List<Variant> variantsNotFromThisSlice = VariantHadoopDBWriter.filterVariantsNotFromThisSlice(sliceStart, slice);
//            System.out.println(sliceStart + " " + slice + "\n        -> " + variantsNotFromThisSlice);
            filteredVariants.addAll(variantsNotFromThisSlice);
        });

        assertEquals(variants, filteredVariants);
    }



}