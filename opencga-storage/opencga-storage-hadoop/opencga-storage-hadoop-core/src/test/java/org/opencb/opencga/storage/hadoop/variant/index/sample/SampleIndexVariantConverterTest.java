package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.core.config.storage.FieldConfiguration;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;

import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@Category(ShortTests.class)
public class SampleIndexVariantConverterTest {

    private SampleIndexVariantConverter converter;
    private FileIndexSchema fileIndex;

    @Before
    public void setUp() throws Exception {
        converter = new SampleIndexVariantConverter(SampleIndexSchema.defaultSampleIndexSchema());
        fileIndex = (SampleIndexSchema.defaultSampleIndexSchema()).getFileIndex();
    }

    @Test
    public void testConvert() {
        BitBuffer bitBuffer = new BitBuffer(fileIndex.getBitsLength());

        fileIndex.getTypeIndex().write(VariantType.SNV, bitBuffer);
        assertEquals(bitBuffer,
                converter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", ".").build()));

        fileIndex.getTypeIndex().write(VariantType.INDEL, bitBuffer);
        assertEquals(bitBuffer,
                converter.createFileIndexValue(0, 0, v("1:100:A:-").addSample("s1", "0/1", ".").build()));

        fileIndex.getTypeIndex().write(VariantType.DELETION, bitBuffer);
        assertEquals(bitBuffer,
                converter.createFileIndexValue(0, 0, v("1:100-200:A:<DEL>").addSample("s1", "0/1", ".").build()));

        fileIndex.getTypeIndex().write(VariantType.INSERTION, bitBuffer);
        assertEquals(bitBuffer,
                converter.createFileIndexValue(0, 0, v("1:100-200:A:<INS>").addSample("s1", "0/1", ".").build()));

        fileIndex.getTypeIndex().write(VariantType.SNV, bitBuffer);
        fileIndex.getCustomField(FieldConfiguration.Source.FILE, StudyEntry.FILTER).write("PASS", bitBuffer);
        assertEquals(bitBuffer,
                converter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", ".").setFilter("PASS").build()));

        fileIndex.getCustomField(FieldConfiguration.Source.FILE, StudyEntry.FILTER).write(null, bitBuffer);
        fileIndex.getCustomField(FieldConfiguration.Source.FILE, StudyEntry.QUAL).write("2000.0", bitBuffer);
        assertEquals(bitBuffer,
                converter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", ".").setQuality(2000.0).build()));

        fileIndex.getCustomField(FieldConfiguration.Source.FILE, StudyEntry.QUAL).write("10.0", bitBuffer);
        assertEquals(bitBuffer,
                converter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", ".").setQuality(10.0).build()));

        bitBuffer.clear();
        fileIndex.getTypeIndex().write(VariantType.SNV, bitBuffer);
        fileIndex.getFilePositionIndex().write(3, bitBuffer);
        fileIndex.getCustomField(FieldConfiguration.Source.FILE, StudyEntry.QUAL).write("10.0", bitBuffer);
        assertEquals(bitBuffer,
                converter.createFileIndexValue(0, 3, v("1:100:A:C").addSample("s1", "0/1", ".").setQuality(10.0).build()));

        bitBuffer.clear();
        fileIndex.getTypeIndex().write(VariantType.SNV, bitBuffer);
        for (Integer dp : IntStream.range(0, 60).toArray()) {
            fileIndex.getCustomField(FieldConfiguration.Source.SAMPLE, "DP").write(String.valueOf(dp), bitBuffer);
            assertEquals(bitBuffer,
                    converter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", dp.toString()).build()));
            assertEquals(bitBuffer,
                    converter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", dp.toString()).addFileData("DP", 10000).build()));
            assertEquals(bitBuffer,
                    converter.createFileIndexValue(0, 0, v("1:100:A:C").addSample("s1", "0/1", dp.toString()).addFileData("DP", 0).build()));
//            assertEquals(bitBuffer,
//                    fileIndexConverter.createFileIndexValue(0, 0, v("1:100:A:C").setSampleDataKeys("GT").addSample("s1", "0/1").addFileData("DP", dp).build()));
        }
    }

    private VariantBuilder v(String s) {
        return Variant.newBuilder(s).setStudyId("s").setSampleDataKeys("GT", "DP").setFileId("f1");
    }

}