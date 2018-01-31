package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.db.mongodb.AnnotationMongoDBAdaptor;
import org.opencb.opencga.core.models.Annotable;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.VariableSet;

import java.util.List;

public class AnnotableConverter<T extends Annotable> extends GenericDocumentComplexConverter<T> {

    private static final String ANNOTATION_SETS = AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SETS.key();
    private AnnotationConverter annotationConverter;

    public AnnotableConverter(Class<T> clazz) {
        super(clazz);

        annotationConverter = new AnnotationConverter();
    }

    public Document convertToStorageType(T object, VariableSet variableSet) {
        // TODO: This method cannot exist because the object will have an array of annotationSets that could be from different variablesets
        return super.convertToStorageType(object);
    }

    public T convertToDataModelType(Document document, QueryOptions queryOptions) {
        List<AnnotationSet> annotationSets = annotationConverter.fromDBToAnnotation((List<Document>) document.get(ANNOTATION_SETS),
                queryOptions);
        T t = super.convertToDataModelType(document);
        t.setAnnotationSets(annotationSets);
        return t;
    }
}
