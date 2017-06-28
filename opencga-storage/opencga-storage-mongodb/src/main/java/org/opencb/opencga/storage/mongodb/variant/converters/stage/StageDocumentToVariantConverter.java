package org.opencb.opencga.storage.mongodb.variant.converters.stage;

import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.VariantStringIdConverter;

/**
 * Created by jacobo on 18/05/17.
 */
public class StageDocumentToVariantConverter implements ComplexTypeConverter<Variant, Document> {

    public static final String ID_FIELD = "_id";
    public static final String END_FIELD = "end";
    public static final String REF_FIELD = "ref";
    public static final String ALT_FIELD = "alt";
    public static final String STUDY_FILE_FIELD = "_i";
    public static final String SECONDARY_ALTERNATES_FIELD = "alts";
    private VariantStringIdConverter idConverter = new VariantStringIdConverter();

    @Override
    public Variant convertToDataModelType(Document object) {
        return idConverter.buildVariant(object.getString(ID_FIELD),
                object.getInteger(END_FIELD),
                object.getString(REF_FIELD),
                object.getString(ALT_FIELD));
    }

    @Override
    public Document convertToStorageType(Variant variant) {
        return new Document(ID_FIELD, idConverter.buildId(variant))
                .append(REF_FIELD, variant.getReference())
                .append(ALT_FIELD, variant.getAlternate())
                .append(END_FIELD, variant.getEnd());
    }



}
