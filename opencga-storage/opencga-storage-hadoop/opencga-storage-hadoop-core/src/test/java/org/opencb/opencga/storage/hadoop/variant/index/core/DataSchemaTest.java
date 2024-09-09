package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.core.io.bit.ExposedByteArrayOutputStream;

import java.nio.ByteBuffer;

public class DataSchemaTest {


    private DataSchema dataSchema;
    private VarCharDataField key1;
    private VarCharDataField key2;
    private IntegerDataField key3;
    private VarCharDataField key4;

    @Before
    public void setUp() throws Exception {
        key1 = new VarCharDataField(new IndexFieldConfiguration(IndexFieldConfiguration.Source.META, "key1", null));
        key2 = new VarCharDataField(new IndexFieldConfiguration(IndexFieldConfiguration.Source.META, "key2", null));
        key3 = new IntegerDataField(new IndexFieldConfiguration(IndexFieldConfiguration.Source.META, "key3", null));
        key4 = new VarCharDataField(new IndexFieldConfiguration(IndexFieldConfiguration.Source.META, "key4", null));
        dataSchema = new DataSchema() {
            {
                addField(key1);
                addField(key2);
                addField(key3);
                addField(key4);
            }
        };
    }

    @Test
    public void readWriteDefault() {
        ExposedByteArrayOutputStream stream = new ExposedByteArrayOutputStream();
        for (DataFieldBase field : dataSchema.getFields()) {
            field.write(field.getDefault(), stream);
        }
        ByteBuffer byteByffer = stream.toByteByffer();

        ExposedByteArrayOutputStream stream2 = new ExposedByteArrayOutputStream();
        dataSchema.writeEntry(stream2, byteByffer);
        Assert.assertEquals(1, stream2.toByteByffer().limit());
        Assert.assertEquals(0, stream2.toByteByffer().get());
    }

    @Test
    public void readWrite() {

        ByteBuffer bbEntry1 = ByteBuffer.allocate(100);
        ByteBuffer bbEntry2 = ByteBuffer.allocate(100);
        ByteBuffer bb = ByteBuffer.allocate(100);

        key1.write("key1_value", bbEntry1);
        key2.write("key2_value", bbEntry1);
        key3.write(1234255, bbEntry1);
        key4.write("key4_value", bbEntry1);
        bbEntry1.limit(bbEntry1.position());

        key1.write("key1_value", bbEntry2);
        key2.write("key2_value", bbEntry2);
        key3.write(32, bbEntry2);
        key4.write("key4_value", bbEntry2);
        bbEntry2.limit(bbEntry2.position());

        dataSchema.writeEntry(bb, bbEntry1);
        dataSchema.writeEntry(bb, bbEntry2);

        bbEntry1.rewind();
        bbEntry2.rewind();
        bb.rewind();

//        System.out.println("Bytes.toStringBinary(bbEntry) = " + Bytes.toStringBinary(bbEntry));
//        System.out.println("Bytes.toStringBinary(bbEntry) = " + Bytes.toStringBinary(bb));

        // Read entries sequentially
        ByteBuffer readEntry = dataSchema.readNextEntry(bb);
        checkEntry(bbEntry1, readEntry, 1234255);

        ByteBuffer readEntry2 = dataSchema.readNextEntry(bb);
        checkEntry(bbEntry2, readEntry2, 32);

        // Read entries random
        readEntry2 = dataSchema.readEntry(bb, 1);
        checkEntry(bbEntry2, readEntry2, 32);

        readEntry = dataSchema.readEntry(bb, 0);
        checkEntry(bbEntry1, readEntry, 1234255);
    }

    private void checkEntry(ByteBuffer expected, ByteBuffer readEntry, int key3NumberValue) {
        Assert.assertEquals(expected, readEntry);
//        System.out.println("Bytes.toStringBinary(readEntry) = " + Bytes.toStringBinary(readEntry));

        // Sequential field read order
        Assert.assertEquals("key1_value", key1.readAndDecode(readEntry));
        Assert.assertEquals("key2_value", key2.readAndDecode(readEntry));
        Assert.assertEquals(key3NumberValue, key3.readAndDecode(readEntry).intValue());
        Assert.assertEquals("key4_value", key4.readAndDecode(readEntry));

        readEntry.rewind();

        // Wrong order.
        Assert.assertEquals("key1_value", key4.readAndDecode(readEntry));
        Assert.assertEquals("key2_value", key1.readAndDecode(readEntry));
        Assert.assertEquals(key3NumberValue, key3.readAndDecode(readEntry).intValue());
        Assert.assertEquals("key4_value", key2.readAndDecode(readEntry));

        readEntry.rewind();

        // Random field access order
        Assert.assertEquals("key4_value", dataSchema.readFieldAndDecode(readEntry, key4));
        Assert.assertEquals("key1_value", dataSchema.readFieldAndDecode(readEntry, key1));
        Assert.assertEquals(key3NumberValue, dataSchema.readFieldAndDecode(readEntry, key3).intValue());
        Assert.assertEquals("key2_value", dataSchema.readFieldAndDecode(readEntry, key2));
    }
}