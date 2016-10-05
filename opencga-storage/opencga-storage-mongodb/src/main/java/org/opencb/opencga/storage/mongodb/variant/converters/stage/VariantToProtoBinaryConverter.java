/*
 * Copyright 2015-2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
