/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.mongodb.variant.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;

import java.io.IOException;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 * TODO: Extend {@link org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter}
 */
public class DocumentToStudyConfigurationConverter implements ComplexTypeConverter<StudyConfiguration, Document> {

    static final char CHARACTER_TO_REPLACE_DOTS = (char) 163; // <-- £
    static final String TO_REPLACE_DOTS = "&#46;";
    public static final String FIELD_FILE_IDS = "_fileIds";

    private final ObjectMapper objectMapper;

    public DocumentToStudyConfigurationConverter() {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }


    @Override
    public StudyConfiguration convertToDataModelType(Document document) {
        try {
            String json = objectMapper.writeValueAsString(document).replace(TO_REPLACE_DOTS, ".");
            return objectMapper.readValue(json, StudyConfiguration.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Document convertToStorageType(StudyConfiguration studyConfiguration) {
        try {
            Document studyMongo = Document.parse(objectMapper.writeValueAsString(studyConfiguration).replace(".", TO_REPLACE_DOTS));
            studyMongo.put(FIELD_FILE_IDS, studyConfiguration.getFileIds().values());
            studyMongo.put("_id", studyConfiguration.getStudyId());
            return studyMongo;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
