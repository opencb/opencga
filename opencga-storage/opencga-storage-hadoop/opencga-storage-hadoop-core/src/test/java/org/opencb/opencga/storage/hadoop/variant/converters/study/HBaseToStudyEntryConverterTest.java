package org.opencb.opencga.storage.hadoop.variant.converters.study;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseVariantConverterConfiguration;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;

import java.util.*;

/**
 * Created on 06/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToStudyEntryConverterTest {

    private HBaseToStudyEntryConverter converter;
    private StudyMetadata sm;
    private VariantStorageMetadataManager mm;

    @Before
    public void setUp() throws Exception {
        DummyVariantStorageMetadataDBAdaptorFactory.clear();
        mm = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        sm = mm.createStudy("S1");

        mm.registerFile(sm.getId(), "f1", Arrays.asList("S1", "S2", "S3"));
        mm.registerFile(sm.getId(), "f2", Arrays.asList("S4", "S5", "S6"));
        mm.registerFile(sm.getId(), "f3", Arrays.asList("S7", "S8", "S9"));
        mm.addIndexedFiles(sm.getId(), Arrays.asList(1, 2));

        mm.updateStudyMetadata(sm.getId(), s -> {
            s.getAttributes().put(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
            return s;
        });

        converter = new HBaseToStudyEntryConverter(mm, null)
                .configure(HBaseVariantConverterConfiguration.builder()
                        .build());
    }

    @Test
    public void testConvertBasic() throws Exception {
        List<VariantRow.SampleColumn> fixedValues = new ArrayList<>();
        fixedValues.add(getSampleColumn(1, listOf("0/0", "PASS")));
        fixedValues.add(getSampleColumn(3, listOf("0/1", "PASS")));

        StudyEntry s = converter.convert(fixedValues, Collections.emptyList(), new Variant("1:1000:A:C"), 1);
        StudyEntry expected = new StudyEntry("1", Collections.emptyList(), listOf("GT", "FT"))
                .addSampleData("S1", listOf("0/0", "PASS"))
                .addSampleData("S2", listOf("?/?", "."))
                .addSampleData("S3", listOf("0/1", "PASS"))
                .addSampleData("S4", listOf("?/?", "."))
                .addSampleData("S5", listOf("?/?", "."))
                .addSampleData("S6", listOf("?/?", "."));
        Assert.assertEquals(s.toString(), expected, s);
    }

    @Test
    public void testConvertExtendedFormat() throws Exception {
        mm.updateStudyMetadata(sm.getId(), s -> {
            s.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "AD,DP");
            return s;
        });

        List<VariantRow.SampleColumn> fixedValues = new ArrayList<>();
        fixedValues.add(getSampleColumn(1, listOf("0/0", "1,2", "10")));
        fixedValues.add(getSampleColumn(3, listOf("0/1", "3,4", "20")));

        StudyEntry s = converter.convert(fixedValues, Collections.emptyList(), new Variant("1:1000:A:C"), 1);

        StudyEntry expected = new StudyEntry("1", Collections.emptyList(), listOf("GT", "AD", "DP"))
                .addSampleData("S1", listOf("0/0", "1,2", "10"))
                .addSampleData("S2", listOf("?/?", ".", "."))
                .addSampleData("S3", listOf("0/1", "3,4", "20"))
                .addSampleData("S4", listOf("?/?", ".", "."))
                .addSampleData("S5", listOf("?/?", ".", "."))
                .addSampleData("S6", listOf("?/?", ".", "."));
        Assert.assertEquals(s.toString(), expected, s);
    }

//    @Test
//    public void testConvertExtendedFormatFileEntryData() throws Exception {
//        sc.getAttributes().put(VariantStorageEngine.Options.EXTRA_FORMAT_FIELDS.key(), "AD,DP");
//        scm.updateStudyConfiguration(sc, null);
//
//        List<Pair<Integer, List<String>>> fixedValues = new ArrayList<>();
//        List<Pair<Integer, byte[]>> otherValues = new ArrayList<>();
//        fixedValues.add(Pair.of(1, listOf("0/0", "1,2", "10")));
//        otherValues.add(Pair.of(1, OtherSampleData.newBuilder()
//                .putSampleData("KEY_1", "VALUE_1")
//                .putSampleData("KEY_2", "VALUE_2")
//                .build().toByteArray()));
//        fixedValues.add(Pair.of(2, listOf("1/1", "8,9", "70")));
//        fixedValues.add(Pair.of(3, listOf("0/1", "3,4", "20")));
//        otherValues.add(Pair.of(3, OtherSampleData.newBuilder()
//                .putSampleData("KEY_1", "VALUE_1")
//                .putSampleData("KEY_3", "VALUE_3")
//                .build().toByteArray()));
//
//        StudyEntry s = converter.convert(fixedValues, otherValues, new Variant("1:1000:A:C"), 1, null);
//        StudyEntry expected = new StudyEntry("1", Collections.emptyList(), listOf("GT", "AD", "DP", "KEY_1", "KEY_2", "KEY_3"))
//                .addSampleData("S1", listOf("0/0", "1,2", "10", "VALUE_1", "VALUE_2", "."))
//                .addSampleData("S2", listOf("1/1", "8,9", "70", ".", ".", "."))
//                .addSampleData("S3", listOf("0/1", "3,4", "20", "VALUE_1", ".", "VALUE_3"))
//                .addSampleData("S4", listOf("?/?", ".", ".", ".", ".", "."))
//                .addSampleData("S5", listOf("?/?", ".", ".", ".", ".", "."))
//                .addSampleData("S6", listOf("?/?", ".", ".", ".", ".", "."));
//        Assert.assertEquals(s.toString(), expected, s);
//    }

    @Test
    public void testConvertFileEntryData() throws Exception {
        mm.updateStudyMetadata(sm.getId(), s -> {
            s.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "AD,DP");
            s.getAttributes().put(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.ADVANCED);
            return s;
        });

        List<VariantRow.SampleColumn> fixedValues = new ArrayList<>();
        List<Pair<String, PhoenixArray>> otherValues = new ArrayList<>();
        List<String> right = listOf("0/0", "1,2", "10");
        fixedValues.add(getSampleColumn(1, right));
//        otherValues.add(Pair.of(1, OtherSampleData.newBuilder()
//                .putSampleData("KEY_1", "VALUE_1")
//                .putSampleData("KEY_2", "VALUE_2")
//                .build().toByteArray()));
        fixedValues.add(getSampleColumn(2, listOf("1/1", "8,9", "70")));
        fixedValues.add(getSampleColumn(3, listOf("0/1", "3,4", "20")));
        fixedValues.add(getSampleColumn(5, listOf("0/1", ".", ".")));
        fixedValues.add(getSampleColumn(6, listOf(".", ".", ".")));
//        otherValues.add(Pair.of(3, OtherSampleData.newBuilder()
//                .putSampleData("KEY_1", "VALUE_1")
//                .putSampleData("KEY_3", "VALUE_3")
//                .build().toByteArray()));

        StudyEntry s = converter.convert(fixedValues, otherValues, new Variant("1:1000:A:C"), 1);
        StudyEntry expected = new StudyEntry("1", Collections.emptyList(), listOf("GT", "AD", "DP"))
                .addSampleData("S1", listOf("0/0", "1,2", "10"))
                .addSampleData("S2", listOf("1/1", "8,9", "70"))
                .addSampleData("S3", listOf("0/1", "3,4", "20"))
                .addSampleData("S4", listOf("0/0", ".", "."))
                .addSampleData("S5", listOf("0/1", ".", "."))
                .addSampleData("S6", listOf(".", ".", "."));
        Assert.assertEquals(s.toString(), expected, s);
    }

    private VariantRow.SampleColumn getSampleColumn(int sampleId, List<String> sampleData) {
        return new VariantRow.SampleColumn() {
            @Override
            public int getStudyId() {
                return 0;
            }

            @Override
            public int getSampleId() {
                return sampleId;
            }

            @Override
            public Integer getFileId() {
                return null;
            }

            @Override
            public List<String> getSampleData() {
                return sampleData;
            }

            @Override
            public List<String> getMutableSampleData() {
                return new ArrayList<>(sampleData);
            }

            @Override
            public String getSampleData(int idx) {
                return sampleData.get(idx);
            }
        };
    }

    @Test
    public void testGetAlternateCoordinate() {
        Assert.assertEquals(new AlternateCoordinate("1", 10035, 10035, "A", "<NON_REF>", VariantType.NO_VARIATION),
                HBaseToStudyEntryConverter.getAlternateCoordinate("1:10035:10035:A:<NON_REF>:NO_VARIATION"));

        Assert.assertEquals(new AlternateCoordinate("1", 10035, 10035, "A", "<DUP:TANDEM>", VariantType.DUPLICATION),
                HBaseToStudyEntryConverter.getAlternateCoordinate("1:10035:10035:A:<DUP:TANDEM>:DUPLICATION"));

        Assert.assertEquals(new AlternateCoordinate("1", 10035, 10035, "A", "A]chr1:1234]", VariantType.BREAKEND),
                HBaseToStudyEntryConverter.getAlternateCoordinate("1:10035:10035:A:A]chr1:1234]:BREAKEND"));

    }

    private PhoenixArray arrayOf(String... values) {
        return new PhoenixArray(PVarchar.INSTANCE, values);
    }

    private <T> List<T> listOf(T... values) {
        ArrayList<T> list = new ArrayList<>(values.length);
        for (T value : values) {
            list.add(value);
        }
        return list;
    }
}