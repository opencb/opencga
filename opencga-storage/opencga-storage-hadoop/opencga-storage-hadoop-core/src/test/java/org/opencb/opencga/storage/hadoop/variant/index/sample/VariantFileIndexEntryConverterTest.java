package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.config.IndexFieldConfiguration;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class VariantFileIndexEntryConverterTest {

    private VariantFileIndexConverter fileIndexConverter;
    private FileIndexSchema fileIndex;

    @Before
    public void setUp() throws Exception {
        fileIndexConverter = new VariantFileIndexConverter(SampleIndexSchema.defaultSampleIndexSchema());
        fileIndex = (SampleIndexSchema.defaultSampleIndexSchema()).getFileIndex();
    }

    @Test
    public void testConvert() {
        BitBuffer bitBuffer = new BitBuffer(fileIndex.getBitsLength());

        fileIndex.getTypeIndex().write(VariantType.SNV, bitBuffer);
        assertEquals(bitBuffer,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", "0").build()));

        fileIndex.getTypeIndex().write(VariantType.INDEL, bitBuffer);
        assertEquals(bitBuffer,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:-").addSample("s1", "0/1", "0").build()));

        fileIndex.getTypeIndex().write(VariantType.DELETION, bitBuffer);
        assertEquals(bitBuffer,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100-200:A:<DEL>").addSample("s1", "0/1", "0").build()));

        fileIndex.getTypeIndex().write(VariantType.INSERTION, bitBuffer);
        assertEquals(bitBuffer,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100-200:A:<INS>").addSample("s1", "0/1", "0").build()));

        fileIndex.getTypeIndex().write(VariantType.SNV, bitBuffer);
        fileIndex.getCustomField(IndexFieldConfiguration.Source.FILE, StudyEntry.FILTER).write("PASS", bitBuffer);
        assertEquals(bitBuffer,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", "0").setFilter("PASS").build()));

        fileIndex.getCustomField(IndexFieldConfiguration.Source.FILE, StudyEntry.FILTER).write(null, bitBuffer);
        fileIndex.getCustomField(IndexFieldConfiguration.Source.FILE, StudyEntry.QUAL).write("2000.0", bitBuffer);
        assertEquals(bitBuffer,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", "0").setQuality(2000.0).build()));

        fileIndex.getCustomField(IndexFieldConfiguration.Source.FILE, StudyEntry.QUAL).write("10.0", bitBuffer);
        assertEquals(bitBuffer,
                fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", "0").setQuality(10.0).build()));

        bitBuffer.clear();
        fileIndex.getTypeIndex().write(VariantType.SNV, bitBuffer);
        fileIndex.getFilePositionIndex().write(3, bitBuffer);
        fileIndex.getCustomField(IndexFieldConfiguration.Source.FILE, StudyEntry.QUAL).write("10.0", bitBuffer);
        assertEquals(bitBuffer,
                fileIndexConverter.createFileIndexValue(0, 3, v("1:100:A:C").addSample("s1", "0/1", "0").setQuality(10.0).build()));

        bitBuffer.clear();
        fileIndex.getTypeIndex().write(VariantType.SNV, bitBuffer);
        for (Integer dp : Arrays.asList(1, 5, 10, 20, 50)) {
            fileIndex.getCustomField(IndexFieldConfiguration.Source.SAMPLE, "DP").write(String.valueOf(dp), bitBuffer);
            assertEquals(bitBuffer,
                    fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", dp.toString()).build()));
            assertEquals(bitBuffer,
                    fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", dp.toString()).addFileData("DP", 10000).build()));
            assertEquals(bitBuffer,
                    fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", dp.toString()).addFileData("DP", 0).build()));
//            assertEquals(bitBuffer,
//                    fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").setSampleDataKeys("GT").addSample("s1", "0/1").addFileData("DP", dp).build()));
        }
    }

    private VariantBuilder v(String s) {
        return Variant.newBuilder(s).setStudyId("s").setSampleDataKeys("GT", "DP").setFileId("f1");
    }

}