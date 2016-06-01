package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.Document;
import org.opencb.opencga.catalog.models.DiseasePanel;

import java.io.IOException;

/**
 * Created by pfurio on 01/06/16.
 */
public class PanelConverter extends GenericConverter<DiseasePanel, Document> {

    private ObjectWriter panelWriter;

    public PanelConverter() {
        objectReader = objectMapper.reader(DiseasePanel.class);
        panelWriter = objectMapper.writerFor(DiseasePanel.class);
    }

    @Override
    public DiseasePanel convertToDataModelType(Document object) {
        DiseasePanel diseasePanel = null;
        try {
            diseasePanel = objectReader.readValue(objectWriter.writeValueAsString(object));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return diseasePanel;
    }

    @Override
    public Document convertToStorageType(DiseasePanel object) {
        Document document = null;
        try {
            document = Document.parse(panelWriter.writeValueAsString(object));
            document.put("id", document.getInteger("id").longValue());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }

}
