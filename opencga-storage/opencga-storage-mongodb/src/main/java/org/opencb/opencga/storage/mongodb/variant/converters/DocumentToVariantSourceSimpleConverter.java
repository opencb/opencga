package org.opencb.opencga.storage.mongodb.variant.converters;

import com.fasterxml.jackson.databind.MapperFeature;
import org.bson.Document;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;

/**
 * Created on 26/04/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DocumentToVariantSourceSimpleConverter extends GenericDocumentComplexConverter<VariantSource> {

    public DocumentToVariantSourceSimpleConverter() {
        super(VariantSource.class);
        getObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
    }

    @Override
    public Document convertToStorageType(VariantSource object) {
        Document document = super.convertToStorageType(object);
        document.append("_id", object.getStudyId() + "_" + object.getFileId());
        return document;
    }

}
