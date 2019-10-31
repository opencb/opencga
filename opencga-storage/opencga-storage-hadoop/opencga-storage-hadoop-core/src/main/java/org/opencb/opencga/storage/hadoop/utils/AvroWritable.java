package org.opencb.opencga.storage.hadoop.utils;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.Writable;

import java.io.*;

public abstract class AvroWritable<T extends GenericRecord> implements Writable {

    protected T value;

    private final SpecificDatumWriter<T> writer;
    private final SpecificDatumReader<T> reader;
    private final BinaryEncoder binaryEncoder;
    private final BinaryDecoder binaryDecoder;
    private final DataInputAsInputStream is;
    private final DataOutputAsOutputStream os;

    protected AvroWritable(Class<T> sampleVariantStatsClass) {
        writer = new SpecificDatumWriter<>(sampleVariantStatsClass);
        reader = new SpecificDatumReader<>(sampleVariantStatsClass);
        os = new DataOutputAsOutputStream(null);
        is = new DataInputAsInputStream(null);
        binaryEncoder = EncoderFactory.get().directBinaryEncoder(os, null);
        binaryDecoder = DecoderFactory.get().directBinaryDecoder(is, null);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        writeAvro(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        readAvro(dataInput);
    }

    protected final void writeAvro(DataOutput out) throws IOException {
        os.setOut(out); // Replace the DataOutput used by the encoder
        writer.write(value, binaryEncoder);
        binaryEncoder.flush();
    }

    protected final void readAvro(DataInput in) throws IOException {
        is.setIn(in); // Replace the DataInput used by the decoder
        value = reader.read(value, binaryDecoder);
    }

    public T getValue() {
        return value;
    }

    public AvroWritable<T> setValue(T value) {
        this.value = value;
        return this;
    }

    private static class DataOutputAsOutputStream extends OutputStream {
        private DataOutput out;

        DataOutputAsOutputStream(DataOutput out) {
            this.out = out;
        }

        public DataOutputAsOutputStream setOut(DataOutput out) {
            this.out = out;
            return this;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }
    }

    private static class DataInputAsInputStream extends InputStream {
        private DataInput in;

        DataInputAsInputStream(DataInput in) {
            this.in = in;
        }

        public DataInputAsInputStream setIn(DataInput in) {
            this.in = in;
            return this;
        }

        @Override
        public int read() throws IOException {
            return Byte.toUnsignedInt(in.readByte());
        }

        @Override
        public int read(byte[] b) throws IOException {
            in.readFully(b);
            return b.length;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            in.readFully(b, off, len);
            return len;
        }
    }

}
