package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;

public class AnnotableConverter<T> extends GenericDocumentComplexConverter<T> {

    private AnnotationConverter annotationConverter;

    public AnnotableConverter(Class<T> clazz) {
        super(clazz);

        annotationConverter = new AnnotationConverter();
    }

    public T convertToDataModelType(Document document, QueryOptions queryOptions) {
//        annotationConverter.fromDBToAnnotation(document.get("annotationSets"), queryOptions);
        return super.convertToDataModelType(document);
    }
}
