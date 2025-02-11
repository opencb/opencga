package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.nio.ByteBuffer;

@Category(ShortTests.class)
public class VarBinaryDataFieldTest {

    private VarBinaryDataField field;

    @Before
    public void setUp() {
        field = new VarBinaryDataField(null);
    }

    @Test
    public void testEncode() {
        testEncode(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}));
        testEncode(ByteBuffer.wrap(new byte[]{}));
        testEncode(ByteBuffer.wrap(new byte[]{0}));
        testEncode(ByteBuffer.wrap(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0}));
    }

    private void testEncode(ByteBuffer value) {
        ByteBuffer encode = field.encode(value);
        ByteBuffer decode = field.decode(encode);

        Assert.assertEquals(value, decode);

        int byteLength = field.getByteLength(value);
        Assert.assertEquals(byteLength, encode.limit());

        ByteBuffer buffer = ByteBuffer.allocate(byteLength);
        field.write(value, buffer);
        buffer.rewind();

        ByteBuffer actualValue = field.readAndDecode(buffer);
        buffer.rewind();
        Assert.assertEquals(value, actualValue);

        ByteBuffer readUndecoded = field.read(buffer);
        buffer.rewind();
        actualValue = field.decode(readUndecoded);
        Assert.assertEquals(value, actualValue);
    }


}