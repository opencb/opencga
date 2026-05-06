package org.opencb.opencga.storage.core.variant.index.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.core.config.storage.FieldConfiguration;
import org.opencb.opencga.storage.core.io.bit.ExposedByteArrayOutputStream;
import org.opencb.opencga.storage.core.variant.index.core.DataFieldBase;
import org.opencb.opencga.storage.core.variant.index.core.DataSchema;
import org.opencb.opencga.storage.core.variant.index.core.IntegerDataField;
import org.opencb.opencga.storage.core.variant.index.core.VarCharDataField;

import java.nio.ByteBuffer;

public class DataSchemaTest {


    private DataSchema dataSchema;
    private VarCharDataField key1;
    private VarCharDataField key2;
    private IntegerDataField key3;
    private VarCharDataField key4;

    @Before
    public void setUp() throws Exception {
        key1 = new VarCharDataField(new FieldConfiguration(FieldConfiguration.Source.META, "key1", null));
        key2 = new VarCharDataField(new FieldConfiguration(FieldConfiguration.Source.META, "key2", null));
        key3 = new IntegerDataField(new FieldConfiguration(FieldConfiguration.Source.META, "key3", null));
        key4 = new VarCharDataField(new FieldConfiguration(FieldConfiguration.Source.META, "key4", null));
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
        dataSchema.writeDocument(stream2, byteByffer);
        Assert.assertEquals(1, stream2.toByteByffer().limit());
        Assert.assertEquals(0, stream2.toByteByffer().get());
    }

    @Test
    public void readWrite() {

        ByteBuffer bbDoc1 = ByteBuffer.allocate(100);
        ByteBuffer bbDoc2 = ByteBuffer.allocate(100);
        ByteBuffer bb = ByteBuffer.allocate(100);

        key1.write("key1_value", bbDoc1);
        key2.write("key2_value", bbDoc1);
        key3.write(1234255, bbDoc1);
        key4.write("key4_value", bbDoc1);
        bbDoc1.limit(bbDoc1.position());

        key1.write("key1_value", bbDoc2);
        key2.write("key2_value", bbDoc2);
        key3.write(32, bbDoc2);
        key4.write("key4_value", bbDoc2);
        bbDoc2.limit(bbDoc2.position());

        dataSchema.writeDocument(bb, bbDoc1);
        dataSchema.writeDocument(bb, bbDoc2);

        bbDoc1.rewind();
        bbDoc2.rewind();
        bb.rewind();

        // Read entries sequentially
        ByteBuffer readDoc = dataSchema.readNextDocument(bb);
        checkDocument(bbDoc1, readDoc, 1234255);

        ByteBuffer readDoc2 = dataSchema.readNextDocument(bb);
        checkDocument(bbDoc2, readDoc2, 32);

        // Read entries random
        readDoc2 = dataSchema.readDocument(bb, 1);
        checkDocument(bbDoc2, readDoc2, 32);

        readDoc = dataSchema.readDocument(bb, 0);
        checkDocument(bbDoc1, readDoc, 1234255);
    }

    private void checkDocument(ByteBuffer expected, ByteBuffer readDoc, int key3NumberValue) {
        Assert.assertEquals(expected, readDoc);
//        System.out.println("Bytes.toStringBinary(readDoc) = " + Bytes.toStringBinary(readDoc));

        // Sequential field read order
        Assert.assertEquals("key1_value", key1.readAndDecode(readDoc));
        Assert.assertEquals("key2_value", key2.readAndDecode(readDoc));
        Assert.assertEquals(key3NumberValue, key3.readAndDecode(readDoc).intValue());
        Assert.assertEquals("key4_value", key4.readAndDecode(readDoc));

        readDoc.rewind();

        // Wrong order.
        Assert.assertEquals("key1_value", key4.readAndDecode(readDoc));
        Assert.assertEquals("key2_value", key1.readAndDecode(readDoc));
        Assert.assertEquals(key3NumberValue, key3.readAndDecode(readDoc).intValue());
        Assert.assertEquals("key4_value", key2.readAndDecode(readDoc));

        readDoc.rewind();

        // Random field access order
        Assert.assertEquals("key4_value", dataSchema.readFieldAndDecode(readDoc, key4));
        Assert.assertEquals("key1_value", dataSchema.readFieldAndDecode(readDoc, key1));
        Assert.assertEquals(key3NumberValue, dataSchema.readFieldAndDecode(readDoc, key3).intValue());
        Assert.assertEquals("key2_value", dataSchema.readFieldAndDecode(readDoc, key2));
    }
}