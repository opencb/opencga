/*
 * Copyright 2015-2017 OpenCB
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

import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.VariantStringIdConverter;

/**
 * Created by jacobo on 18/05/17.
 */
public class StageDocumentToVariantConverter implements ComplexTypeConverter<Variant, Document> {

    public static final String ID_FIELD = "_id";
    public static final String END_FIELD = "end";
    public static final String REF_FIELD = "ref";
    public static final String ALT_FIELD = "alt";
    public static final String STUDY_FILE_FIELD = "_i";
    public static final String SECONDARY_ALTERNATES_FIELD = "alts";
    private VariantStringIdConverter idConverter = new VariantStringIdConverter();

    @Override
    public Variant convertToDataModelType(Document object) {
        return idConverter.buildVariant(object.getString(ID_FIELD),
                object.getInteger(END_FIELD),
                object.getString(REF_FIELD),
                object.getString(ALT_FIELD));
    }

    @Override
    public Document convertToStorageType(Variant variant) {
        return new Document(ID_FIELD, idConverter.buildId(variant))
                .append(REF_FIELD, variant.getReference())
                .append(ALT_FIELD, variant.getAlternate())
                .append(END_FIELD, variant.getEnd());
    }



}
