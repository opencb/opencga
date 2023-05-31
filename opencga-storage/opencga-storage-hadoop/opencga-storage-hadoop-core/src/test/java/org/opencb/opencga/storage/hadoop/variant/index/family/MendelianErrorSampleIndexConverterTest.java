package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class MendelianErrorSampleIndexConverterTest {

    final int numVariants = 500;
    private final SampleIndexSchema schema = SampleIndexSchema.defaultSampleIndexSchema();

    @Test
    public void testIterator() throws IOException {
        int batchStart = 12000000;
        List<Variant> variants01 = new ArrayList<>(numVariants);
        List<Variant> variants11 = new ArrayList<>(numVariants);
        for (int i = 0; i < numVariants; i++) {
            variants01.add(new Variant("1:" + (batchStart + i * 10) + ":A:T"));
            variants11.add(new Variant("1:" + (batchStart + i * 10 + 5) + ":A:C"));
        }

        // Check mendelian errors iterator
        FamilyIndexPutBuilder builder = new FamilyIndexPutBuilder(0);
        for (int i = 0; i < variants01.size(); i++) {
            if (i % 5 == 0) {
                builder.addMendelianError(variants01.get(i), "0/1", i, 1);
            }
            if (i % 5 == 2) {
                builder.addMendelianError(variants11.get(i), "1/1", i, 1);
            }
        }

        checkIterator(variants01, variants11, () -> {
            SampleIndexEntry entry = new SampleIndexEntry(0, "1", batchStart);
            entry.setMendelianVariants(builder.getMendelianErrors());
            return entry;
        });
    }

    private void checkIterator(List<Variant> variants01, List<Variant> variants11, Supplier<SampleIndexEntry> factory) {
        checkIterator(variants01, variants11, factory.get(), false);
        checkIterator(variants01, variants11, factory.get(), true);
    }

    private void checkIterator(List<Variant> variants01, List<Variant> variants11, SampleIndexEntry entry, boolean annotated) {
        if (annotated) {
            byte[] annot = new byte[numVariants];
            for (int i = 0; i < numVariants; i++) {
                if (i % 3 != 0) {
                    annot[i] = AnnotationIndexConverter.INTERGENIC_MASK;
                }
            }
            entry.getGtEntry("0/1").setAnnotationIndex(annot);
            entry.getGtEntry("1/1").setAnnotationIndex(annot);
        }

        MendelianErrorSampleIndexEntryIterator iterator =
                new MendelianErrorSampleIndexEntryIterator(entry, schema);

        int c = 0;
        while (iterator.hasNext()) {
            int idx = iterator.nextIndex();
            String gt = iterator.nextGenotype();

            if (idx % 5 == 0) {
                assertEquals("0/1", gt);
            } else if (idx % 5 == 2) {
                assertEquals("1/1", gt);
            } else {
                fail("Unknown gt " + gt);
            }

//            System.out.println(gt + "[" + idx + "]" + (annotated && idx % 3 == 0 ? " - non_intergenic" : ""));
            if (annotated) {
                if (idx % 3 == 0) {
                    if (gt.equals("0/1")) {
                        assertEquals(idx / 3, iterator.nextNonIntergenicIndex());
                    } else if (gt.equals("1/1")) {
                        assertEquals(idx / 3, iterator.nextNonIntergenicIndex());
                    }
                } else {
                    try {
                        iterator.nextNonIntergenicIndex();
                        fail("Expect IllegalStateException!");
                    } catch (IllegalStateException e) {
                        assertEquals("Next variant is not intergenic!", e.getMessage());
                    }
                }
            } else {
                assertEquals(-1, iterator.nextNonIntergenicIndex());
            }
            if (c % 2 == 0) {
                iterator.skip();
            } else {
                if (gt.equals("0/1")) {
                    assertEquals(variants01.get(idx), iterator.next());
                } else {
                    assertEquals(variants11.get(idx), iterator.next());
                }
            }
            c++;
        }
        assertFalse(iterator.hasNext());
        assertEquals(numVariants / 5 * 2, c);
    }

}