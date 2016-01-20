package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.Document;
import org.opencb.opencga.catalog.models.User;

import java.io.IOException;

/**
 * Created by pfurio on 19/01/16.
 */
public class UserConverter extends GenericConverter<User, Document> {

    private ObjectWriter userWriter;

    public UserConverter() {
        objectReader = objectMapper.reader(User.class);
        userWriter = objectMapper.writerFor(User.class);
    }

    @Override
    public User convertToDataModelType(Document object) {
        User user = null;
        try {
            user = objectReader.readValue(objectWriter.writeValueAsString(object));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return user;
    }

    @Override
    public Document convertToStorageType(User object) {
        Document document = null;
        try {
            document = Document.parse(userWriter.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
