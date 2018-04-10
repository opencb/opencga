package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * Created on 15/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsTaskUtilsTest {


    @Test
    public void testgetOverlappingVariants() {
        FillGapsTask a = new FillGapsTask(new StudyConfiguration(1, "a"), new GenomeHelper(new Configuration()), true);

        VcfSliceProtos.VcfSlice vcfSlice = buildVcfSlice("17:29113:T:C", "17:29185:A:G", "17:29190-29189::AAAAAAAA", "17:29464:G:");

        List<Variant> variants = Arrays.asList(
                new Variant("1:29180:CAGAGACAGC:AAGAAACAGCAAAAAAAAAA"),
                new Variant("1:29180:C:A"),
                new Variant("1:29180::CAGAGA"),
                new Variant("1:29181:A:G"),
                new Variant("1:29193:A:G"),
                new Variant("1:29198:A:G")
        );

        ListIterator<VcfSliceProtos.VcfRecord> iterator = vcfSlice.getRecordsList().listIterator();
        for (Variant variant : variants) {
            ArrayList<Pair<VcfSliceProtos.VcfSlice, VcfSliceProtos.VcfRecord>> list = new ArrayList<>();
            a.getOverlappingVariants(variant, 1, vcfSlice, iterator, list);
            System.out.println(variant + " iteratorIdx : " + iterator.nextIndex());
        }
    }


    @Test
    public void testgetOverlappingVariants2() {
        FillGapsTask a = new FillGapsTask(new StudyConfiguration(1, "a"), new GenomeHelper(new Configuration()), true);

        VcfSliceProtos.VcfSlice vcfSlice = buildVcfSlice(
                "2:182562574-182562573::T",
                "2:182562574-182562573::TT",
                "2:182562574-182562576:TTT:"
        );

        List<Variant> variants = Arrays.asList(
                new Variant("2:182562571:CTG:"),
                new Variant("2:182562572:TGT:"),
                new Variant("2:182562572:TG:"),
                new Variant("2:182562572::T"),
                new Variant("2:182562573:G:T"),
                new Variant("2:182562574:TTTT:"),
                new Variant("2:182562574:TTT:"),
                new Variant("2:182562574:TT:"),
                new Variant("2:182562574:T:"),
                new Variant("2:182562574::T"),
                new Variant("2:182562574::TT"),
                new Variant("2:182562575:T:C"),
                new Variant("2:182562575:T:G"),
                new Variant("2:182562576:T:A"),
                new Variant("2:182562576:T:C"),
                new Variant("2:182562594:G:A"),
                new Variant("2:182562613:G:A"),
                new Variant("2:182562674:A:G"),
                new Variant("2:182562683:A:G"),
                new Variant("2:182562890:A:G"),
                new Variant("2:182562947:C:A")
        );

        ListIterator<VcfSliceProtos.VcfRecord> iterator = vcfSlice.getRecordsList().listIterator();
        for (Variant variant : variants) {
            ArrayList<Pair<VcfSliceProtos.VcfSlice, VcfSliceProtos.VcfRecord>> list = new ArrayList<>();
            a.getOverlappingVariants(variant, 1, vcfSlice, iterator, list);
            System.out.println(variant + " iteratorIdx : " + iterator.nextIndex());
        }
    }

    private VcfSliceProtos.VcfSlice buildVcfSlice(String... variants) {
        Variant variant = new Variant(variants[0]);
        int position = (variant.getStart() / 1000) * 1000;
        VcfSliceProtos.VcfSlice.Builder vcfSlice = VcfSliceProtos.VcfSlice.newBuilder()
                .setChromosome(variant.getChromosome())
                .setPosition(position);

        for (String s : variants) {
            variant = new Variant(s);
            vcfSlice.addRecords(VcfSliceProtos.VcfRecord.newBuilder()
                    .setRelativeStart(variant.getStart() - position)
                    .setRelativeEnd(variant.getEnd() - position)
                    .setReference(variant.getReference())
                    .setAlternate(variant.getAlternate())
                    .build());

        }

        return vcfSlice.build();
    }

    @Test
    public void testOverlapsWith() {
        assertTrue(FillGapsTask.overlapsWith(new Variant("1:100:T:-"), "1", 100, 100));

        Variant variant = new Variant("1:100:-:T");
        assertTrue(FillGapsTask.overlapsWith(variant, "1", variant.getStart(), variant.getEnd()));

        variant = new Variant("1:100:-:TTTTT");
        assertFalse(FillGapsTask.overlapsWith(variant, "1", 102, 102));
    }

    @Test
    public void testIsRegionAfterVariantStart() {
        assertTrue(FillGapsTask.isRegionAfterVariantStart(100, 100, new Variant("1:100:-:T")));
        assertFalse(FillGapsTask.isRegionAfterVariantStart(100, 100, new Variant("1:100:A:T")));
        assertTrue(FillGapsTask.isRegionAfterVariantStart(101, 101, new Variant("1:100:A:T")));

        assertFalse(FillGapsTask.isRegionAfterVariantStart(99, 99, new Variant("1:100:AAA:GGG")));
        assertFalse(FillGapsTask.isRegionAfterVariantStart(100, 100, new Variant("1:100:AAA:GGG")));
        assertFalse(FillGapsTask.isRegionAfterVariantStart(101, 100, new Variant("1:100:AAA:GGG")));
        assertTrue(FillGapsTask.isRegionAfterVariantStart(101, 101, new Variant("1:100:AAA:GGG")));
        assertTrue(FillGapsTask.isRegionAfterVariantStart(102, 102, new Variant("1:100:AAA:GGG")));
        assertTrue(FillGapsTask.isRegionAfterVariantStart(103, 103, new Variant("1:100:AAA:GGG")));
    }
}
