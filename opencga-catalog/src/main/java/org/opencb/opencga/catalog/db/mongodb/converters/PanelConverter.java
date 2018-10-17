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

package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.Panel;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Created by pfurio on 01/06/16.
 */
public class PanelConverter extends GenericDocumentComplexConverter<Panel> {

    public PanelConverter() {
        super(Panel.class);
    }

    @Override
    public Document convertToStorageType(Panel object) {
        try {
            String json = JacksonUtils.getDefaultObjectMapper().writeValueAsString(object);
            Document document = Document.parse(json);
            replaceDots(document);
            document.put("uid", object.getUid());
            document.put("studyUid", object.getStudyUid());
            return document;
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Panel convertToDataModelType(Document document) {
        try {
            restoreDots(document);
            String json = JacksonUtils.getDefaultObjectMapper().writeValueAsString(document);
            return JacksonUtils.getDefaultObjectMapper().readValue(json, Panel.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
