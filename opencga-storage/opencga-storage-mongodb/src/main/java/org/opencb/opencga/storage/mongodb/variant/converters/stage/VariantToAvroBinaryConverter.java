package org.opencb.opencga.storage.mongodb.variant.converters.stage;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.utils.CompressionUtils;

import java.io.*;
import java.util.zip.DataFormatException;

/**
 * Created on 27/06/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantToAvroBinaryConverter implements ComplexTypeConverter<Variant, Binary> {

    private SpecificDatumWriter<VariantAvro> writer = new SpecificDatumWriter<>(VariantAvro.getClassSchema());
    private SpecificDatumReader<VariantAvro> reader = new SpecificDatumReader<>(VariantAvro.getClassSchema());

    @Override
    public Variant convertToDataModelType(Binary object) {
        BinaryDecoder decoder = null;
        try {
            byte[] data = object.getData();
            data = CompressionUtils.decompress(data);
            InputStream is = new ByteArrayInputStream(data);
//                is = new GZIPInputStream(is);
            decoder = DecoderFactory.get().directBinaryDecoder(is, decoder);

            return new Variant(reader.read(null, decoder));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataFormatException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public Binary convertToStorageType(Variant variant) {
        BinaryEncoder encoder = null;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//            byteArrayOutputStream.reset();
        OutputStream outputStream = byteArrayOutputStream;
        try {
//                outputStream = new GZIPOutputStream(outputStream);
            encoder = EncoderFactory.get().directBinaryEncoder(outputStream, encoder);
            writer.write(variant.getImpl(), encoder);
            encoder.flush();
            outputStream.flush();
            outputStream.close();
            byte[] data = byteArrayOutputStream.toByteArray();
            data = CompressionUtils.compress(data);
            return new Binary(data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
