package org.opencb.opencga.catalog.utils;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.AnnotationSetDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Annotation;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.VariableSet;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 06/07/16.
 */
public class AnnotationManager {

    /**
     * Creates an annotation set for the selected entity.
     *
     * @param id id of the entity being annotated.
     * @param variableSet variable set under which the annotation will be made.
     * @param annotationSetName annotation set name that will be used for the annotation.
     * @param annotations map of annotations to create the annotation set.
     * @param attributes map with further attributes that the user might be interested in storing.
     * @param dbAdaptor DB Adaptor to make the correspondent call to create the annotation set.
     * @return a queryResult object with the annotation set created.
     * @throws CatalogException if the annotation is not valid.
     */
    public static QueryResult<AnnotationSet> createAnnotationSet(long id, VariableSet variableSet, String annotationSetName,
                                                           Map<String, Object> annotations, Map<String, Object> attributes,
                                                           AnnotationSetDBAdaptor dbAdaptor)
            throws CatalogException {

        ParamUtils.checkAlias(annotationSetName, "annotationSetName");

        // Create empty annotation set
        AnnotationSet annotationSet = new AnnotationSet(annotationSetName, variableSet.getId(), new HashSet<>(), TimeUtils.getTime(),
                attributes);

        // Fill the annotation set object with the annotations
        for (Map.Entry<String, Object> entry : annotations.entrySet()) {
            annotationSet.getAnnotations().add(new Annotation(entry.getKey(), entry.getValue()));
        }

        // Obtain all the annotationSets the object had in order to check for duplicities
        QueryResult<AnnotationSet> annotationSetQueryResult = dbAdaptor.getAnnotationSet(id, null);
        List<AnnotationSet> annotationSets;
        if (annotationSetQueryResult == null || annotationSetQueryResult.getNumResults() == 0) {
            annotationSets = Collections.emptyList();
        } else {
            annotationSets = annotationSetQueryResult.getResult();
        }

        // Check validity of annotations and duplicities
        CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);

        // Register the annotation set in the database
        return dbAdaptor.createAnnotationSet(id, annotationSet);
    }

    /**
     * Update the annotation set.
     *
     * @param id id of the entity where the annotation set will be updated.
     * @param annotationSetName annotation set name of the annotation to be updated.
     * @param newAnnotations map with the annotations that will have to be changed with the new values.
     * @param dbAdaptor DBAdaptor of the entity corresponding to the id.
     * @param studyDBAdaptor studyDBAdaptor to obtain the variableSet to check for the validity of the updated annotation.
     * @return a queryResult containing the annotation set after the update.
     * @throws CatalogException when the annotation set name could not be found or the new annotation is not valid.
     */
    public static QueryResult<AnnotationSet> updateAnnotationSet(long id, String annotationSetName, Map<String, Object> newAnnotations,
                                                                 AnnotationSetDBAdaptor dbAdaptor,
                                                                 StudyDBAdaptor studyDBAdaptor) throws CatalogException {
        // Obtain the annotation set to be updated
        QueryResult<AnnotationSet> queryResult = dbAdaptor.getAnnotationSet(id, annotationSetName);
        if (queryResult == null || queryResult.getNumResults() == 0) {
            throw new CatalogException("No annotation could be found under the name " + annotationSetName);
        }
        AnnotationSet annotationSet = queryResult.first();

        // Get the variableSet
        VariableSet variableSet = studyDBAdaptor.getVariableSet(annotationSet.getVariableSetId(), null).first();

        // Update and validate annotations
        CatalogAnnotationsValidator.mergeNewAnnotations(annotationSet, newAnnotations);
        CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, null);

        // Update the annotation set in the database
        return dbAdaptor.updateAnnotationSet(id, annotationSet);
    }


}
