package org.opencb.opencga.storage.hadoop.variant.io;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.*;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

@Category(ShortTests.class)
public class MaxWriteBlockOutputStreamTest {

    @Test
    public void shouldWriteAndReadDataCorrectly() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        MaxWriteBlockOutputStream outputStream = new MaxWriteBlockOutputStream(byteArrayOutputStream);

        byte[] data = "test data".getBytes();
        outputStream.write(data, 0, data.length);
        outputStream.flush();

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
        byte[] readData = new byte[data.length];
        dataInputStream.readFully(readData);

        assertArrayEquals(data, readData);
    }

    @Test
    public void shouldHandleEmptyData() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        MaxWriteBlockOutputStream outputStream = new MaxWriteBlockOutputStream(byteArrayOutputStream);

        byte[] data = new byte[0];
        outputStream.write(data, 0, data.length);
        outputStream.flush();

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
        byte[] readData = new byte[data.length];
        dataInputStream.readFully(readData);

        assertArrayEquals(data, readData);
    }

    @Test
    public void shouldHandleLargeData() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream = Mockito.spy(byteArrayOutputStream);
        Mockito.verify(byteArrayOutputStream, Mockito.never()).write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
        MaxWriteBlockOutputStream outputStream = new MaxWriteBlockOutputStream(byteArrayOutputStream, 1024);

        byte[] data = new byte[1024 * 1024]; // 1 MB of data
        new Random().nextBytes(data);
        outputStream.write(data, 0, data.length);
        outputStream.flush();

        // Check that the write method was called multiple times
        Mockito.verify(byteArrayOutputStream, Mockito.times(1024)).write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
        Mockito.verify(byteArrayOutputStream, Mockito.never()).write(Mockito.any(byte[].class));

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
        byte[] readData = new byte[data.length];
        dataInputStream.readFully(readData);

        assertArrayEquals(data, readData);
    }

    @Test
    public void shouldThrowExceptionForNullData() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        MaxWriteBlockOutputStream outputStream = new MaxWriteBlockOutputStream(byteArrayOutputStream);

        assertThrows(NullPointerException.class, () -> {
            outputStream.write(null, 0, 0);
        });
    }

}