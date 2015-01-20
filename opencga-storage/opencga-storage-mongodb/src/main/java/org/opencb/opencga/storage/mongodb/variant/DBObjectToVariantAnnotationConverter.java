package org.opencb.opencga.storage.mongodb.variant;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 * Created by jacobo on 13/01/15.
 */
public class DBObjectToVariantAnnotationConverter implements ComplexTypeConverter<VariantAnnotation, DBObject> {

    public final static String ID_FIELD = "id";
    public final static String CONSEQUENCE_TYPE_FIELD = "ct";

    private final ObjectMapper jsonObjectMapper;

    public DBObjectToVariantAnnotationConverter() {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @Override
    public VariantAnnotation convertToDataModelType(DBObject object) {
        return null;
    }

    @Override
    public DBObject convertToStorageType(VariantAnnotation object) {
        DBObject dbObject = new BasicDBObject();

//        dbObject.put(ID_FIELD, object.getId());
//        BasicDBList ct = new BasicDBList();
//        for (ConsequenceType consequenceType : object.getConsequenceTypes()) {
//            try {
//                ct.add(JSON.parse(jsonObjectMapper.writeValueAsString(consequenceType)));
//            } catch (JsonProcessingException e) {
//                e.printStackTrace();
//            }
//        }
//        dbObject.put(CONSEQUENCE_TYPE_FIELD, ct);

        try {
            dbObject = (DBObject) JSON.parse(jsonObjectMapper.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return dbObject;
    }
}
