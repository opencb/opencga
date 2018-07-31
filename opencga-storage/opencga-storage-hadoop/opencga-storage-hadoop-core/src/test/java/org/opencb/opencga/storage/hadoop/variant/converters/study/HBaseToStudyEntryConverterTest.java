package org.opencb.opencga.storage.hadoop.variant.converters.study;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyProjectMetadataAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyStudyConfigurationAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Created on 06/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToStudyEntryConverterTest {

    private HBaseToStudyEntryConverter converter;
    private StudyConfiguration sc;
    private StudyConfigurationManager scm;

    @Before
    public void setUp() throws Exception {
        scm = new StudyConfigurationManager(new DummyProjectMetadataAdaptor(), new DummyStudyConfigurationAdaptor(), new DummyVariantFileMetadataDBAdaptor());
        sc = new StudyConfiguration(1, "S1");
        sc.getIndexedFiles().add(1);
        sc.getIndexedFiles().add(2);
        sc.getSamplesInFiles().put(1, new LinkedHashSet<>(listOf(1, 2, 3)));
        sc.getSamplesInFiles().put(2, new LinkedHashSet<>(listOf(4, 5, 6)));
        sc.getSampleIds().put("S1", 1);
        sc.getSampleIds().put("S2", 2);
        sc.getSampleIds().put("S3", 3);
        sc.getSampleIds().put("S4", 4);
        sc.getSampleIds().put("S5", 5);
        sc.getSampleIds().put("S6", 6);
        sc.getSampleIds().put("S7", 7);
        sc.getAttributes().put(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);

        scm.updateStudyConfiguration(sc, null);

        final GenomeHelper genomeHelper = new GenomeHelper(new Configuration());
        converter = new HBaseToStudyEntryConverter(genomeHelper.getColumnFamily(), scm, null);

    }

    @Test
    public void testConvertBasic() throws Exception {
        List<Pair<Integer, List<String>>> fixedValues = new ArrayList<>();
        fixedValues.add(Pair.of(1, listOf("0/0", "PASS")));
        fixedValues.add(Pair.of(3, listOf("0/1", "PASS")));

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
        sc.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "AD,DP");
        scm.updateStudyConfiguration(sc, null);

        List<Pair<Integer, List<String>>> fixedValues = new ArrayList<>();
        fixedValues.add(Pair.of(1, listOf("0/0", "1,2", "10")));
        fixedValues.add(Pair.of(3, listOf("0/1", "3,4", "20")));

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
//        sc.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "AD,DP");
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
        sc.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "AD,DP");
        sc.getAttributes().put(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.ADVANCED);
        scm.updateStudyConfiguration(sc, null);

        List<Pair<Integer, List<String>>> fixedValues = new ArrayList<>();
        List<Pair<String, PhoenixArray>> otherValues = new ArrayList<>();
        fixedValues.add(Pair.of(1, listOf("0/0", "1,2", "10")));
//        otherValues.add(Pair.of(1, OtherSampleData.newBuilder()
//                .putSampleData("KEY_1", "VALUE_1")
//                .putSampleData("KEY_2", "VALUE_2")
//                .build().toByteArray()));
        fixedValues.add(Pair.of(2, listOf("1/1", "8,9", "70")));
        fixedValues.add(Pair.of(3, listOf("0/1", "3,4", "20")));
        fixedValues.add(Pair.of(5, listOf("0/1", ".", ".")));
        fixedValues.add(Pair.of(6, listOf(".", ".", ".")));
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