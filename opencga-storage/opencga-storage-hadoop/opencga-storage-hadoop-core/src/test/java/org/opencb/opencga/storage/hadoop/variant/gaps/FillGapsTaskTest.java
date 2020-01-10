package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VariantToVcfSliceConverter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;

import java.util.*;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * Created on 15/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsTaskTest {

    private VariantStorageMetadataManager metadataManager;
    private StudyMetadata studyMetadata;
    private VariantToVcfSliceConverter toSliceConverter;

    @Before
    public void setUp() throws Exception {
        DummyVariantStorageMetadataDBAdaptorFactory.clear();
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        studyMetadata = metadataManager.createStudy("S");
        metadataManager.updateStudyMetadata("S", sm -> {
            sm.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "DP");
            sm.getVariantHeader().getComplexLines().add(new VariantFileHeaderComplexLine("INFO", "OTHER", "asdf", "1", "String", Collections.emptyMap()));
            return sm;
        });
        int studyId = studyMetadata.getId();
        int file1 = metadataManager.registerFile(studyId, "file1.vcf", Arrays.asList("S1", "S2"));
        int file2 = metadataManager.registerFile(studyId, "file2.vcf", Arrays.asList("S3", "S4"));
        metadataManager.addIndexedFiles(studyId, Arrays.asList(file1, file2));
        toSliceConverter = new VariantToVcfSliceConverter(
                new HashSet<>(Arrays.asList("FILTER", "QUAL", "OTHER")),
                new HashSet<>(Arrays.asList("GT", "DP")));
    }

    @Test
    public void testGetOverlappingVariants() {
        FillGapsTask a = new FillGapsTask(this.studyMetadata, new GenomeHelper(new Configuration()), true, false, metadataManager);

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
//            System.out.println(variant + " iteratorIdx : " + iterator.nextIndex());
        }
    }


    @Test
    public void testGetOverlappingVariants2() {
        FillGapsTask a = new FillGapsTask(this.studyMetadata, new GenomeHelper(new Configuration()), true, false, metadataManager);

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
//            System.out.println(variant + " iteratorIdx : " + iterator.nextIndex());
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

    @Test
    public void fillGapsAlreadyPresent() {
        FillGapsTask task = new FillGapsTask(this.studyMetadata, new GenomeHelper(new Configuration()), true, false, metadataManager);

        Put put = new Put(VariantPhoenixKeyFactory.generateVariantRowKey(new Variant("1:100:A:T")));
        VariantOverlappingStatus overlappingStatus = task.fillGaps(new Variant("1:100:A:T"),
                new HashSet<>(Arrays.asList(1, 2)), put, new ArrayList<>(), 1,
                toSliceConverter.convert(Arrays.asList(variantFile1("1:100:A:C"), variantFile1("1:100:A:T"))),
                toSliceConverter.convert(Collections.emptyList()));
        assertEquals(VariantOverlappingStatus.NONE, overlappingStatus);

        assertTrue(put.getFamilyCellMap().isEmpty());
    }

    @Test
    public void fillGapsReferenceOverlapping() {
        FillGapsTask task = new FillGapsTask(this.studyMetadata, new GenomeHelper(new Configuration()), false, false, metadataManager);

        Variant variant = fillGaps(task, VariantOverlappingStatus.REFERENCE, "1:100:A:T",
                toSliceConverter.convert(Collections.emptyList()), toSliceConverter.convert(Arrays.asList(variantFile1("1:100:A:<*>"))));

        StudyEntry studyEntry = variant.getStudies().get(0);
        assertEquals(1, studyEntry.getSecondaryAlternates().size());
        assertEquals("<*>", studyEntry.getSecondaryAlternates().get(0).getAlternate());
        assertEquals("GT:DP", studyEntry.getFormatAsString());
        assertEquals("0/0", studyEntry.getSampleData("S1", "GT"));
        assertEquals("./.", studyEntry.getSampleData("S2", "GT"));
        assertEquals("1234", studyEntry.getSampleData("S1", "DP"));
        assertEquals("1", studyEntry.getSampleData("S2", "DP"));
        assertEquals("PASS", studyEntry.getFiles().get(0).getAttributes().get("FILTER"));
        assertEquals("50", studyEntry.getFiles().get(0).getAttributes().get("QUAL"));
        assertEquals("VALUE", studyEntry.getFiles().get(0).getAttributes().get("OTHER"));
        assertEquals("100:A:<*>:0", studyEntry.getFiles().get(0).getCall());


        variant = fillGaps(task, VariantOverlappingStatus.REFERENCE, "1:100:A:T",
                toSliceConverter.convert(Collections.emptyList()), toSliceConverter.convert(Arrays.asList(variantFile1("1:100:A:."))));

        studyEntry = variant.getStudies().get(0);
        assertEquals(0, studyEntry.getSecondaryAlternates().size());
        assertEquals("GT:DP", studyEntry.getFormatAsString());
        assertEquals("0/0", studyEntry.getSampleData("S1", "GT"));
        assertEquals("./.", studyEntry.getSampleData("S2", "GT"));
        assertEquals("1234", studyEntry.getSampleData("S1", "DP"));
        assertEquals("1", studyEntry.getSampleData("S2", "DP"));
        assertEquals("PASS", studyEntry.getFiles().get(0).getAttributes().get("FILTER"));
        assertEquals("50", studyEntry.getFiles().get(0).getAttributes().get("QUAL"));
        assertEquals("VALUE", studyEntry.getFiles().get(0).getAttributes().get("OTHER"));
        assertEquals("100:A:.:0", studyEntry.getFiles().get(0).getCall());
    }

    @Test
    public void fillGapsMultipleOverlapping() {
        FillGapsTask task = new FillGapsTask(this.studyMetadata, new GenomeHelper(new Configuration()), true, false, metadataManager);

        Variant variant = fillGaps(task, VariantOverlappingStatus.MULTI, "1:100:A:T",
                toSliceConverter.convert(Arrays.asList(variantFile1("1:100:A:C"), variantFile1("1:100:A:G"))));

        StudyEntry studyEntry = variant.getStudies().get(0);
        assertEquals(1, studyEntry.getSecondaryAlternates().size());
        assertEquals("<*>", studyEntry.getSecondaryAlternates().get(0).getAlternate());
        assertEquals("GT:DP", studyEntry.getFormatAsString());
        assertEquals("2/2", studyEntry.getSampleData("S1", "GT"));
        assertEquals("2/2", studyEntry.getSampleData("S2", "GT"));
        assertEquals(".", studyEntry.getSampleData("S1", "DP"));
        assertEquals(".", studyEntry.getSampleData("S2", "DP"));
        assertEquals(null, studyEntry.getFiles().get(0).getAttributes().get("FILTER"));
        assertEquals(null, studyEntry.getFiles().get(0).getAttributes().get("QUAL"));
        assertEquals(null, studyEntry.getFiles().get(0).getAttributes().get("OTHER"));
        assertEquals(null, studyEntry.getFiles().get(0).getCall());
    }

    @Test
    public void testFillGapsMultiAllelic() {
        FillGapsTask task = new FillGapsTask(this.studyMetadata, new GenomeHelper(new Configuration()), true, false, metadataManager);

        Variant variant = fillGaps(task, VariantOverlappingStatus.VARIANT, "1:100:A:T",
                toSliceConverter.convert(Arrays.asList(variantFile1("1:100:A:C"))));

        StudyEntry studyEntry = variant.getStudies().get(0);
        assertEquals(1, studyEntry.getSecondaryAlternates().size());
        assertEquals("C", studyEntry.getSecondaryAlternates().get(0).getAlternate());
        assertEquals("GT:DP", studyEntry.getFormatAsString());
        assertEquals("0/2", studyEntry.getSampleData("S1", "GT"));
        assertEquals("2/2", studyEntry.getSampleData("S2", "GT"));
        assertEquals("1234", studyEntry.getSampleData("S1", "DP"));
        assertEquals("1234", studyEntry.getSampleData("S2", "DP"));
        assertEquals("PASS", studyEntry.getFiles().get(0).getAttributes().get("FILTER"));
        assertEquals("50", studyEntry.getFiles().get(0).getAttributes().get("QUAL"));
        assertEquals("VALUE", studyEntry.getFiles().get(0).getAttributes().get("OTHER"));
        assertEquals("100:A:C:0", studyEntry.getFiles().get(0).getCall());
    }

    @Test
    public void testFillGapsSkipMultiNewAllelic() {
        FillGapsTask task = new FillGapsTask(this.studyMetadata, new GenomeHelper(new Configuration()), true, true, metadataManager);

        Variant variant = fillGaps(task, VariantOverlappingStatus.VARIANT, "1:100:A:T",
                toSliceConverter.convert(Arrays.asList(variantFile1("1:100:A:C"))));

        StudyEntry studyEntry = variant.getStudies().get(0);
        assertEquals(1, studyEntry.getSecondaryAlternates().size());
        assertEquals("<*>", studyEntry.getSecondaryAlternates().get(0).getAlternate());
        assertEquals("GT:DP", studyEntry.getFormatAsString());
        assertEquals("0/2", studyEntry.getSampleData("S1", "GT"));
        assertEquals("2/2", studyEntry.getSampleData("S2", "GT"));
        assertEquals(".", studyEntry.getSampleData("S1", "DP"));
        assertEquals(".", studyEntry.getSampleData("S2", "DP"));
        assertEquals("PASS", studyEntry.getFiles().get(0).getAttributes().get("FILTER"));
        assertEquals("50", studyEntry.getFiles().get(0).getAttributes().get("QUAL"));
        assertEquals(null, studyEntry.getFiles().get(0).getAttributes().get("OTHER"));
        assertEquals("100:A:C:0", studyEntry.getFiles().get(0).getCall());


        Variant archiveVariant = variantFile1("1:100:A:C,G");
        assertEquals("1/1", archiveVariant.getStudies().get(0).getSampleData(1).set(0, "1/2"));
        variant = fillGaps(task, VariantOverlappingStatus.VARIANT, "1:100:A:T",
                toSliceConverter.convert(Arrays.asList(archiveVariant)));

        studyEntry = variant.getStudies().get(0);
        assertEquals(1, studyEntry.getSecondaryAlternates().size());
        assertEquals("<*>", studyEntry.getSecondaryAlternates().get(0).getAlternate());
        assertEquals("0/2", studyEntry.getSampleData("S1", "GT"));
        assertEquals("2/2", studyEntry.getSampleData("S2", "GT"));
        assertEquals("100:A:C,G:0", studyEntry.getFiles().get(0).getCall());
    }

    protected Variant fillGaps(FillGapsTask task,
                               VariantOverlappingStatus expected, String variant, VcfSliceProtos.VcfSlice nonRefVcfSlice) {
        return fillGaps(task, expected, variant, nonRefVcfSlice, VcfSliceProtos.VcfSlice.newBuilder().build());
    }

    protected Variant fillGaps(FillGapsTask task,
                               VariantOverlappingStatus expected, String variant, VcfSliceProtos.VcfSlice nonRefVcfSlice, VcfSliceProtos.VcfSlice refVcfSlice) {
        Put put = new Put(VariantPhoenixKeyFactory.generateVariantRowKey(new Variant(variant)));
        VariantOverlappingStatus overlappingStatus = task.fillGaps(new Variant(variant), new HashSet<>(Arrays.asList(1, 2)), put, new ArrayList<>(), 1, nonRefVcfSlice, refVcfSlice);
        assertEquals(expected, overlappingStatus);

        return putToVariant(put);
    }

    private Variant putToVariant(Put put) {
        Result result = Result.create(put.getFamilyCellMap().values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
        return HBaseToVariantConverter.fromResult(metadataManager)
                .setFormats(Arrays.asList("GT", "DP"))
                .convert(result);
    }

    protected Variant variantFile1(String variant) {
        if (new Variant(variant).getType().equals(VariantType.NO_VARIATION)) {
            return new VariantBuilder(variant).setStudyId("S")
                    .setFormat("GT", "DP")
                    .addSample("S1", "0/0", "1234")
                    .addSample("S2", "./.", "1")
                    .setFileId("file1.vcf")
                    .setFilter("PASS")
                    .setQuality(50.0)
                    .addAttribute("OTHER", "VALUE")
                    .build();
        } else {
            return new VariantBuilder(variant).setStudyId("S")
                    .setFormat("GT", "DP")
                    .addSample("S1", "0/1", "1234")
                    .addSample("S2", "1/1", "1234")
                    .setFileId("file1.vcf")
                    .setFilter("PASS")
                    .setQuality(50.0)
                    .addAttribute("OTHER", "VALUE")
                    .build();
        }
    }
}
