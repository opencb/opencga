package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.AnnotationSet;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Created by pfurio on 06/07/16.
 */
public interface IAnnotationSetManager {

    /**
     * General method to create an annotation set that will have to be implemented. The managers implementing it will have to check the
     * validity of the sessionId and permissions and call the general createAnnotationSet implemented above.
     *
     * @param id id of the entity being annotated.
     * @param variableSetId variable set id under which the annotation will be made.
     * @param annotationSetName annotation set name that will be used for the annotation.
     * @param annotations map of annotations to create the annotation set.
     * @param attributes map with further attributes that the user might be interested in storing.
     * @param sessionId session id of the user asking for the operation.
     * @return a queryResult object with the annotation set created.
     * @throws CatalogException when the session id is not valid, the user does not have permissions or any of the annotation
     * parameters are not valid.
     */
    QueryResult<AnnotationSet> createAnnotationSet(String id, long variableSetId, String annotationSetName,
                                                   Map<String, Object> annotations, Map<String, Object> attributes, String sessionId)
            throws CatalogException;

    /**
     * Retrieve all the annotation sets corresponding to entity.
     *
     * @param id id of the entity storing the annotation.
     * @param sessionId session id of the user asking for the annotation.
     * @return a queryResult containing all the annotation sets for that entity.
     * @throws CatalogException when the session id is not valid or the user does not have proper permissions to see the annotations.
     */
    QueryResult<AnnotationSet> getAllAnnotationSets(String id, String sessionId) throws CatalogException;

    /**
     * Retrieve all the annotation sets corresponding to entity.
     *
     * @param id id of the entity storing the annotation.
     * @param sessionId session id of the user asking for the annotation.
     * @return a queryResult containing all the annotation sets for that entity as key:value pairs.
     * @throws CatalogException when the session id is not valid or the user does not have proper permissions to see the annotations.
     */
    QueryResult<ObjectMap> getAllAnnotationSetsAsMap(String id, String sessionId) throws CatalogException;

    /**
     * Retrieve the annotation set of the corresponding entity.
     *
     * @param id id of the entity storing the annotation.
     * @param annotationSetName annotation set name of the annotation that will be returned.
     * @param sessionId session id of the user asking for the annotation.
     * @return a queryResult containing the annotation set for that entity.
     * @throws CatalogException when the session id is not valid, the user does not have proper permissions to see the annotations or the
     * annotationSetName is not valid.
     */
    QueryResult<AnnotationSet> getAnnotationSet(String id, String annotationSetName, String sessionId) throws CatalogException;

    /**
     * Retrieve the annotation set of the corresponding entity.
     *
     * @param id id of the entity storing the annotation.
     * @param annotationSetName annotation set name of the annotation that will be returned.
     * @param sessionId session id of the user asking for the annotation.
     * @return a queryResult containing the annotation set for that entity as key:value pairs.
     * @throws CatalogException when the session id is not valid, the user does not have proper permissions to see the annotations or the
     * annotationSetName is not valid.
     */
    QueryResult<ObjectMap> getAnnotationSetAsMap(String id, String annotationSetName, String sessionId) throws CatalogException;


    /**
     * Update the values of the annotation set.
     *
     * @param id id of the entity storing the annotation.
     * @param annotationSetName annotation set name of the annotation that will be returned.
     * @param newAnnotations map with the annotations that will have to be changed with the new values.
     * @param sessionId session id of the user asking for the annotation.
     * @return a queryResult object containing the annotation set after the update.
     * @throws CatalogException when the session id is not valid, the user does not have permissions to update the annotationSet,
     * the newAnnotations are not correct or the annotationSetName is not valid.
     */
    QueryResult<AnnotationSet> updateAnnotationSet(String id, String annotationSetName, Map<String, Object> newAnnotations,
                                                   String sessionId) throws CatalogException;

    /**
     * Deletes the annotation set.
     *
     * @param id id of the entity storing the annotation.
     * @param annotationSetName annotation set name of the annotation to be deleted.
     * @param sessionId session id of the user asking for the annotation.
     * @return a queryResult object with the annotationSet that has been deleted.
     * @throws CatalogException when the session id is not valid, the user does not have permissions to delete the annotationSet or
     * the annotation set name is not valid.
     */
    QueryResult<AnnotationSet> deleteAnnotationSet(String id, String annotationSetName, String sessionId) throws CatalogException;

    /**
     * Deletes (or puts to the default value if mandatory) a list of annotations from the annotation set.
     *
     * @param id id of the entity storing the annotation.
     * @param annotationSetName annotation set name of the annotation where the update will be made.
     * @param annotations comma separated list of annotation names that will be deleted or updated to the default values.
     * @param sessionId session id of the user asking for the annotation.
     * @return a queryResult object with the annotation set after applying the changes.
     * @throws CatalogException when the session id is not valid, the user does not have permissions to delete the annotationSet,
     * the annotation set name is not valid or any of the annotation names are not valid.
     */
    default QueryResult<AnnotationSet> deleteAnnotations(String id, String annotationSetName, String annotations, String sessionId)
            throws CatalogException {
        throw new CatalogException("Operation still not implemented");
    }

    /**
     * Searches for annotation sets matching the parameters.
     *
     * @param id id of the entity storing the annotation.
     * @param variableSetId variable set id.
     * @param annotation comma separated list of annotations by which to look for the annotationSets.
     * @param sessionId session id of the user asking for the annotationSets
     * @return a queryResult object containing the list of annotation sets that matches the query as key:value pairs.
     * @throws CatalogException when the session id is not valid, the user does not have permissions to look for annotationSets.
     */
    QueryResult<ObjectMap> searchAnnotationSetAsMap(String id, long variableSetId, @Nullable String annotation, String sessionId)
            throws CatalogException;

    /**
     * Searches for annotation sets matching the parameters.
     *
     * @param id id of the entity storing the annotation.
     * @param variableSetId variable set id.
     * @param annotation comma separated list of annotations by which to look for the annotationSets.
     * @param sessionId session id of the user asking for the annotationSets
     * @return a queryResult object containing the list of annotation sets that matches the query.
     * @throws CatalogException when the session id is not valid, the user does not have permissions to look for annotationSets.
     */
    QueryResult<AnnotationSet> searchAnnotationSet(String id, long variableSetId, @Nullable String annotation, String sessionId)
        throws CatalogException;
}
