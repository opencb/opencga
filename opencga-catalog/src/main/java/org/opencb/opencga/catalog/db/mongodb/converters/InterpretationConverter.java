package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.Interpretation;

import java.io.UncheckedIOException;

public class InterpretationConverter extends GenericDocumentComplexConverter<Interpretation> {

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
