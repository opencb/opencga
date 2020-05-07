package org.opencb.opencga.storage.core.variant.dedup;

import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertSame;

public class MaxQualDuplicatedVariantsResolverTest {

    @Test
    public void testComparatorPass() {
        Variant expected;
        List<Variant> variants = Arrays.asList(
                Variant.newBuilder("1:100:A:C").setQuality(100.0).setFilter("PASS").build(),
                expected = Variant.newBuilder("1:100:A:C").setQuality(101.0).setFilter("PASS").build(),
                Variant.newBuilder("1:100:A:C").setQuality(99.0).setFilter("PASS").build(),
                Variant.newBuilder("1:100:A:C").setQuality(".").setFilter("PASS").build(),
                Variant.newBuilder("1:100:A:C").setQuality("").setFilter("PASS").build(),
                Variant.newBuilder("1:100:A:C").setQuality("ASDF").setFilter("PASS").build()
        );

        variants.sort(MaxQualDuplicatedVariantsResolver.COMPARATOR);
        assertSame(variants.get(0).getStudies().get(0).getFile(0).toString(), expected, variants.get(0));
    }

    @Test
    public void testComparatorNoPass() {
        Variant expected;
        List<Variant> variants = Arrays.asList(
                Variant.newBuilder("1:100:A:C").setQuality(100.0).setFilter("A").build(),
                expected = Variant.newBuilder("1:100:A:C").setQuality(101.0).setFilter("B").build(),
                Variant.newBuilder("1:100:A:C").setQuality(99.0).setFilter("C").build(),
                Variant.newBuilder("1:100:A:C").setQuality(".").setFilter("D").build(),
                Variant.newBuilder("1:100:A:C").setQuality("").setFilter("E").build(),
                Variant.newBuilder("1:100:A:C").setQuality("ASDF").setFilter("F").build()
        );


        variants.sort(MaxQualDuplicatedVariantsResolver.COMPARATOR);
        assertSame(variants.get(0).getStudies().get(0).getFile(0).toString(), expected, variants.get(0));
    }

    @Test
    public void testComparatorMix() {
        Variant expected;
        List<Variant> variants = Arrays.asList(
                Variant.newBuilder("1:100:A:C").setQuality(100.0).setFilter("A").build(),
                Variant.newBuilder("1:100:A:C").setQuality(101.0).setFilter("B").build(),
                expected = Variant.newBuilder("1:100:A:C").setQuality(99.0).setFilter("PASS").build(),
                Variant.newBuilder("1:100:A:C").setQuality(98.0).setFilter("PASS").build(),
                Variant.newBuilder("1:100:A:C").setQuality(".").setFilter("D").build(),
                Variant.newBuilder("1:100:A:C").setQuality("").setFilter("E").build(),
                Variant.newBuilder("1:100:A:C").setQuality("ASDF").setFilter("F").build()
        );

        variants.sort(MaxQualDuplicatedVariantsResolver.COMPARATOR);
        assertSame(variants.get(0).getStudies().get(0).getFile(0).toString(), expected, variants.get(0));
    }
}