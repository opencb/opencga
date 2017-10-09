package org.opencb.opencga.storage.hadoop.variant.converters.samples;

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
import org.opencb.opencga.storage.core.variant.dummy.DummyStudyConfigurationAdaptor;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.OtherSampleData;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowProto;

import java.util.*;

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
        scm = new StudyConfigurationManager(new DummyStudyConfigurationAdaptor());
        sc = new StudyConfiguration(1, "S1");
        sc.getIndexedFiles().add(1);
        sc.getIndexedFiles().add(2);
        sc.getSamplesInFiles().put(1, new LinkedHashSet<>(Arrays.asList(1, 2, 3)));
        sc.getSamplesInFiles().put(2, new LinkedHashSet<>(Arrays.asList(4, 5, 6)));
        sc.getSampleIds().put("S1", 1);
        sc.getSampleIds().put("S2", 2);
        sc.getSampleIds().put("S3", 3);
        sc.getSampleIds().put("S4", 4);
        sc.getSampleIds().put("S5", 5);
        sc.getSampleIds().put("S6", 6);
        sc.getSampleIds().put("S7", 7);
        sc.getAttributes().put(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);

        scm.updateStudyConfiguration(sc, null);

        converter = new HBaseToStudyEntryConverter(new GenomeHelper(new Configuration()), scm);

    }

    @Test
    public void testConvertBasic() throws Exception {
        List<Pair<String, Object>> values = new ArrayList<>();
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 1)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"0/0", "PASS"})));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 3)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"0/1", "PASS"})));

        StudyEntry s = converter.convert(values.iterator(), new Variant("1:1000:A:C"), 1, null);
        StudyEntry expected = new StudyEntry("1", Collections.emptyList(), Arrays.asList("GT", "FT"))
                .addSampleData("S1", Arrays.asList("0/0", "PASS"))
                .addSampleData("S2", Arrays.asList("?/?", "."))
                .addSampleData("S3", Arrays.asList("0/1", "PASS"))
                .addSampleData("S4", Arrays.asList("?/?", "."))
                .addSampleData("S5", Arrays.asList("?/?", "."))
                .addSampleData("S6", Arrays.asList("?/?", "."));
        Assert.assertEquals(s.toString(), expected, s);
    }

    @Test
    public void testConvertExtendedFormat() throws Exception {
        sc.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "AD,DP");
        scm.updateStudyConfiguration(sc, null);

        List<Pair<String, Object>> values = new ArrayList<>();
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 1)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"0/0", "1,2", "10"})));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 3)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"0/1", "3,4", "20"})));

        StudyEntry s = converter.convert(values.iterator(), new Variant("1:1000:A:C"), 1, null);

        StudyEntry expected = new StudyEntry("1", Collections.emptyList(), Arrays.asList("GT", "AD", "DP"))
                .addSampleData("S1", Arrays.asList("0/0", "1,2", "10"))
                .addSampleData("S2", Arrays.asList("?/?", ".", "."))
                .addSampleData("S3", Arrays.asList("0/1", "3,4", "20"))
                .addSampleData("S4", Arrays.asList("?/?", ".", "."))
                .addSampleData("S5", Arrays.asList("?/?", ".", "."))
                .addSampleData("S6", Arrays.asList("?/?", ".", "."));
        Assert.assertEquals(s.toString(), expected, s);
    }

    @Test
    public void testConvertExtendedFormatFileEntryData() throws Exception {
        sc.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "AD,DP");
        scm.updateStudyConfiguration(sc, null);

        List<Pair<String, Object>> values = new ArrayList<>();
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 1)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"0/0", "1,2", "10"})));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildOtherSampleDataColumnKey(1, 1)), OtherSampleData.newBuilder()
                .putSampleData("KEY_1", "VALUE_1")
                .putSampleData("KEY_2", "VALUE_2")
                .build().toByteArray()));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 2)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"1/1", "8,9", "70"})));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 3)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"0/1", "3,4", "20"})));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildOtherSampleDataColumnKey(1, 3)), OtherSampleData.newBuilder()
                .putSampleData("KEY_1", "VALUE_1")
                .putSampleData("KEY_3", "VALUE_3")
                .build().toByteArray()));

        StudyEntry s = converter.convert(values.iterator(), new Variant("1:1000:A:C"), 1, null);
        StudyEntry expected = new StudyEntry("1", Collections.emptyList(), Arrays.asList("GT", "AD", "DP", "KEY_1", "KEY_2", "KEY_3"))
                .addSampleData("S1", Arrays.asList("0/0", "1,2", "10", "VALUE_1", "VALUE_2", "."))
                .addSampleData("S2", Arrays.asList("1/1", "8,9", "70", ".", ".", "."))
                .addSampleData("S3", Arrays.asList("0/1", "3,4", "20", "VALUE_1", ".", "VALUE_3"))
                .addSampleData("S4", Arrays.asList("?/?", ".", ".", ".", ".", "."))
                .addSampleData("S5", Arrays.asList("?/?", ".", ".", ".", ".", "."))
                .addSampleData("S6", Arrays.asList("?/?", ".", ".", ".", ".", "."));
        Assert.assertEquals(s.toString(), expected, s);
    }

    @Test
    public void testConvertExtendedFormatFileEntryDataAndRows() throws Exception {
        sc.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "AD,DP");
        sc.getAttributes().put(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.ADVANCED);
        scm.updateStudyConfiguration(sc, null);

        List<Pair<String, Object>> values = new ArrayList<>();
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 1)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"0/0", "1,2", "10"})));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildOtherSampleDataColumnKey(1, 1)), OtherSampleData.newBuilder()
                .putSampleData("KEY_1", "VALUE_1")
                .putSampleData("KEY_2", "VALUE_2")
                .build().toByteArray()));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 2)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"1/1", "8,9", "70"})));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 3)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"0/1", "3,4", "20"})));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildOtherSampleDataColumnKey(1, 3)), OtherSampleData.newBuilder()
                .putSampleData("KEY_1", "VALUE_1")
                .putSampleData("KEY_3", "VALUE_3")
                .build().toByteArray()));

        VariantTableStudyRowProto.Builder rowBuilder = VariantTableStudyRowProto.newBuilder()
                .setStart(1000)
                .setReference("A")
                .setAlternate("C")
                .setHomRefCount(2)
                .addHet(3)
                .addHet(5)
                .addHomVar(2)
                .addNocall(6);
        VariantTableStudyRow row = new VariantTableStudyRow(rowBuilder.build(), "1", 1);
        StudyEntry s = converter.convert(values.iterator(), new Variant("1:1000:A:C"), 1, row);
        StudyEntry expected = new StudyEntry("1", Collections.emptyList(), Arrays.asList("GT", "AD", "DP", "KEY_1", "KEY_2", "KEY_3"))
                .addSampleData("S1", Arrays.asList("0/0", "1,2", "10", "VALUE_1", "VALUE_2", "."))
                .addSampleData("S2", Arrays.asList("1/1", "8,9", "70", ".", ".", "."))
                .addSampleData("S3", Arrays.asList("0/1", "3,4", "20", "VALUE_1", ".", "VALUE_3"))
                .addSampleData("S4", Arrays.asList("0/0", ".", ".", ".", ".", "."))
                .addSampleData("S5", Arrays.asList("0/1", ".", ".", ".", ".", "."))
                .addSampleData("S6", Arrays.asList(VariantTableStudyRow.NOCALL, ".", ".", ".", ".", "."));
        Assert.assertEquals(s.toString(), expected, s);
    }

    @Test
    public void testConvertExtendedFormatFileEntryDataAndRowsExpectedFormat() throws Exception {
        sc.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "AD,DP");
        sc.getAttributes().put(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.ADVANCED);
        scm.updateStudyConfiguration(sc, null);
        converter.setFormats(Arrays.asList("GT", "AD", "KEY_3"));

        List<Pair<String, Object>> values = new ArrayList<>();
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 1)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"0/0", "1,2", "10"})));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildOtherSampleDataColumnKey(1, 1)), OtherSampleData.newBuilder()
                .putSampleData("KEY_1", "VALUE_1")
                .putSampleData("KEY_2", "VALUE_2")
                .build().toByteArray()));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 2)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"1/1", "8,9", "70"})));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildSampleColumnKey(1, 3)), new PhoenixArray(PVarchar.INSTANCE, new String[]{"0/1", "3,4", "20"})));
        values.add(Pair.of(new String(VariantPhoenixHelper.buildOtherSampleDataColumnKey(1, 3)), OtherSampleData.newBuilder()
                .putSampleData("KEY_1", "VALUE_1")
                .putSampleData("KEY_3", "VALUE_3")
                .build().toByteArray()));

        VariantTableStudyRowProto.Builder rowBuilder = VariantTableStudyRowProto.newBuilder()
                .setStart(1000)
                .setReference("A")
                .setAlternate("C")
                .setHomRefCount(2)
                .addHet(3)
                .addHet(5)
                .addHomVar(2)
                .addNocall(6);
        VariantTableStudyRow row = new VariantTableStudyRow(rowBuilder.build(), "1", 1);
        StudyEntry s = converter.convert(values.iterator(), new Variant("1:1000:A:C"), 1, row);
        StudyEntry expected = new StudyEntry("1", Collections.emptyList(), Arrays.asList("GT", "AD", "KEY_3"))
                .addSampleData("S1", Arrays.asList("0/0", "1,2", "."))
                .addSampleData("S2", Arrays.asList("1/1", "8,9", "."))
                .addSampleData("S3", Arrays.asList("0/1", "3,4", "VALUE_3"))
                .addSampleData("S4", Arrays.asList("0/0", ".", "."))
                .addSampleData("S5", Arrays.asList("0/1", ".", "."))
                .addSampleData("S6", Arrays.asList(VariantTableStudyRow.NOCALL, ".", "."));
        Assert.assertEquals(s.toString(), expected, s);
    }
}