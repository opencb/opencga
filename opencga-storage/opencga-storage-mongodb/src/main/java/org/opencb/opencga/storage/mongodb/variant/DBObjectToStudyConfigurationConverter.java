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

package org.opencb.opencga.storage.mongodb.variant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.core.StudyConfiguration;

import java.io.IOException;

/**
 * Created by hpccoll1 on 17/03/15.
 */
public class DBObjectToStudyConfigurationConverter implements ComplexTypeConverter<StudyConfiguration, DBObject> {

    static final char CHARACTER_TO_REPLACE_DOTS = (char) 163; // <-- £
    static final String TO_REPLACE_DOTS = "&#46;";
    public static final String FIELD_FILE_IDS = "_fileIds";

    private final ObjectMapper objectMapper;

    public DBObjectToStudyConfigurationConverter() {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }


    @Override
    public StudyConfiguration convertToDataModelType(DBObject dbObject) {
        try {
            return objectMapper.readValue(dbObject.toString().replace(TO_REPLACE_DOTS, "."), StudyConfiguration.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public DBObject convertToStorageType(StudyConfiguration studyConfiguration) {
        try {
            DBObject studyMongo = (DBObject) JSON.parse(objectMapper.writeValueAsString(studyConfiguration).replace(".", TO_REPLACE_DOTS));
            studyMongo.put(FIELD_FILE_IDS, studyConfiguration.getFileIds().values());
            return studyMongo;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
