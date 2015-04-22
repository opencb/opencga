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
