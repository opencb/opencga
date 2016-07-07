package org.opencb.opencga.storage.mongodb.variant.converters.stage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.utils.CompressionUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.DataFormatException;

/**
 * Created on 27/06/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantToJsonBinaryConverter implements ComplexTypeConverter<Variant, Binary> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Variant convertToDataModelType(Binary object) {
        try {
            byte[] data = object.getData();
            try {
                data = CompressionUtils.decompress(data);
            } catch (DataFormatException e) {
                throw new RuntimeException(e);
            }
            return new Variant(mapper.readValue(data, VariantAvro.class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Binary convertToStorageType(Variant variant) {
        byte[] data = variant.toJson().getBytes();
        try {
            data = CompressionUtils.compress(data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new Binary(data);
    }
}
