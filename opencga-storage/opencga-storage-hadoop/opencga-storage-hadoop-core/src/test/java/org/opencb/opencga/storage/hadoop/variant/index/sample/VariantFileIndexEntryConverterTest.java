package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.VariantFileIndexConverter.*;

public class VariantFileIndexEntryConverterTest {

    private VariantFileIndexConverter fileIndexConverter;

    @Before
    public void setUp() throws Exception {
        fileIndexConverter = new VariantFileIndexConverter();
    }

    @Test
    public void testConvert() {

        int snv = VariantFileIndexConverter.getTypeCode(VariantType.SNV) << TYPE_SHIFT;
        assertEquals(snv,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", "0").build()));
        assertEquals(VariantFileIndexConverter.getTypeCode(VariantType.INDEL) << TYPE_SHIFT,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:-").addSample("s1", "0/1", "0").build()));
        assertEquals(VariantFileIndexConverter.getTypeCode(VariantType.DELETION) << TYPE_SHIFT,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100-200:A:<DEL>").addSample("s1", "0/1", "0").build()));
        assertEquals(VariantFileIndexConverter.getTypeCode(VariantType.INSERTION) << TYPE_SHIFT,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100-200:A:<INS>").addSample("s1", "0/1", "0").build()));

        assertEquals(snv | FILTER_PASS_MASK,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", "0").setFilter("PASS").build()));

        assertEquals(snv | IndexUtils.getRangeCode(2000, SampleIndexConfiguration.QUAL_THRESHOLDS) << QUAL_SHIFT,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", "0").setQuality(2000.0).build()));
        assertEquals(snv | IndexUtils.getRangeCode(10, SampleIndexConfiguration.QUAL_THRESHOLDS) << QUAL_SHIFT,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", "0").setQuality(10.0).build()));

        for (Integer dp : Arrays.asList(1, 5, 10, 20, 50)) {
            assertEquals((byte) (snv | IndexUtils.getRangeCode(dp, SampleIndexConfiguration.DP_THRESHOLDS) << DP_SHIFT),
                    fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", dp.toString()).build()));
            assertEquals((byte) (snv | IndexUtils.getRangeCode(dp, SampleIndexConfiguration.DP_THRESHOLDS) << DP_SHIFT),
                    fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", dp.toString()).addFileData("DP", 10000).build()));
            assertEquals((byte) (snv | IndexUtils.getRangeCode(dp, SampleIndexConfiguration.DP_THRESHOLDS) << DP_SHIFT),
                    fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", dp.toString()).addFileData("DP", 0).build()));
            assertEquals((byte) (snv | IndexUtils.getRangeCode(dp, SampleIndexConfiguration.DP_THRESHOLDS) << DP_SHIFT),
                    fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").setSampleDataKeys("GT").addSample("s1", "0/1").addFileData("DP", dp).build()));
        }


    }

    private VariantBuilder v(String s) {
        return Variant.newBuilder(s).setStudyId("s").setSampleDataKeys("GT", "DP").setFileId("f1");
    }

}