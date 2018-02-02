package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.db.mongodb.AnnotationMongoDBAdaptor;
import org.opencb.opencga.core.models.Annotable;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.VariableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnnotableConverter<T extends Annotable> extends GenericDocumentComplexConverter<T> {

    private static final String ANNOTATION_SETS = AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SETS.key();
    private AnnotationConverter annotationConverter;

    public AnnotableConverter(Class<T> clazz) {
        super(clazz);

        annotationConverter = new AnnotationConverter();
    }

    public Document convertToStorageType(T object, List<VariableSet> variableSetList) {
        List<Document> documentList = new ArrayList<>();

        if (variableSetList != null && !variableSetList.isEmpty() && object.getAnnotationSets() != null
                && !object.getAnnotationSets().isEmpty()) {

            Map<Long, VariableSet> variableSetMap = new HashMap<>();
            for (VariableSet variableSet : variableSetList) {
                variableSetMap.put(variableSet.getId(), variableSet);
            }

            for (AnnotationSet annotationSet : object.getAnnotationSets()) {
                VariableSet variableSet = variableSetMap.get(annotationSet.getVariableSetId());
                if (variableSet != null) {
                    documentList.addAll(annotationConverter.annotationToDB(variableSet, annotationSet));
                }
            }
        }

        object.setAnnotationSets(null);
        Document document = super.convertToStorageType(object);

        document.put(ANNOTATION_SETS, documentList);

        return document;
    }

    public T convertToDataModelType(Document document, QueryOptions queryOptions) {
        List<AnnotationSet> annotationSets = annotationConverter.fromDBToAnnotation((List<Document>) document.get(ANNOTATION_SETS),
                queryOptions);
        T t = super.convertToDataModelType(document);
        t.setAnnotationSets(annotationSets);
        return t;
    }
}
