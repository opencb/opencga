package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.bson.Document;
import org.opencb.opencga.catalog.models.Project;

import java.io.IOException;

/**
 * Created by pfurio on 18/01/16.
 */
public class ProjectConverter extends GenericConverter<Project, Document> {


    public ProjectConverter() {
        objectReader = objectMapper.reader(Project.class);
        objectWriter = objectMapper.writerFor(Document.class);
    }

    @Override
    public Project convertToDataModelType(Document object) {
        Project project = null;
        try {
            Document projects = (Document) object.get("projects");
            project = objectReader.readValue(objectWriter.writeValueAsString(projects));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return project;
    }

    @Override
    public Document convertToStorageType(Project object) {
        Document document = null;
        try {
            document = Document.parse(objectWriter.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
