package org.opencb.opencga.storage.mongodb.variant.converters.stage;

import com.google.protobuf.InvalidProtocolBufferException;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converter.VariantToVcfSliceConverter;
import org.opencb.biodata.tools.variant.converter.VcfSliceToVariantListConverter;
import org.opencb.commons.datastore.core.ComplexTypeConverter;

import java.io.UncheckedIOException;

/**
 * Created on 27/06/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantToProtoBinaryConverter implements ComplexTypeConverter<Variant, Binary> {
    //        private final VariantToProtoVcfRecord converter = new VariantToProtoVcfRecord();
    private final VariantToVcfSliceConverter converter = new VariantToVcfSliceConverter();
    private final VcfSliceToVariantListConverter converterBack
            = new VcfSliceToVariantListConverter(new VariantSource("", "4", "4", ""));


    @Override
    public Variant convertToDataModelType(Binary object) {
        try {
            return converterBack.convert(VcfSliceProtos.VcfSlice.parseFrom(object.getData())).get(0);
        } catch (InvalidProtocolBufferException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Binary convertToStorageType(Variant object) {
        return new Binary(converter.convert(object).toByteArray());
    }
}
