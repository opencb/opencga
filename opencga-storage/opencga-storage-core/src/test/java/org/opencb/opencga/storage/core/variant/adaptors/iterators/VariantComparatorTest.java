package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class VariantComparatorTest {

    @Test
    public void comparator() {

        String[] alleles = {"A", "C", "G", "T", "A", "C", "G", "T", "", "CT", "GT", "AC"};
        int n = 10000;
        int repeated = ((int) (n * 0.2));

        List<Variant> variants = new ArrayList<>(n);
        for (int i = 0; i < n - repeated; i++) {
            variants.add(new Variant("chr" + RandomUtils.nextInt(1, 4), RandomUtils.nextInt(1, 1000), alleles[RandomUtils.nextInt(0, alleles.length)], alleles[RandomUtils.nextInt(0, alleles.length)]));
        }
        variants.addAll(variants.subList(0, repeated));

        Collections.shuffle(variants);
        List<Variant> variants2 = new ArrayList<>(variants);

        StopWatch s1 = StopWatch.createStarted();
        variants.sort(VariantDBIterator.VARIANT_COMPARATOR_AUTOMATIC);
        System.out.println("Auto "+ TimeUtils.durationToString(s1));

        StopWatch s2 = StopWatch.createStarted();
        variants2.sort(VariantDBIterator.VARIANT_COMPARATOR_FAST);
        System.out.println("Fast "+TimeUtils.durationToString(s2));

        assertEquals(variants, variants2);

    }
}