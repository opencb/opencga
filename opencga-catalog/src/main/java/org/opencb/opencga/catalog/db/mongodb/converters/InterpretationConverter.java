/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.Interpretation;

import java.io.UncheckedIOException;

public class InterpretationConverter extends OpenCgaMongoConverter<Interpretation> {

    private final ObjectMapper objectMapper;

    public InterpretationConverter() {
        super(Interpretation.class);
        this.objectMapper = JacksonUtils.getDefaultObjectMapper();
    }

    @Override
    public Document convertToStorageType(Interpretation object) {
        try {
            String json = this.objectMapper.writeValueAsString(object);
            Document document = Document.parse(json);
            replaceDots(document);
            return document;
        } catch (JsonProcessingException var4) {
            throw new UncheckedIOException(var4);
        }
    }

}
