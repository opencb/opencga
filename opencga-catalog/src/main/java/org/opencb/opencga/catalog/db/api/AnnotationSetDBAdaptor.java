package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.catalog.models.summaries.VariableSummary;

import javax.annotation.Nullable;

/**
 * Created by pfurio on 06/07/16.
 */
public interface AnnotationSetDBAdaptor<T, U> extends AclDBAdaptor<T, U> {

    /**
     * Insert an annotation set object in the database to annotate the entity with id "id".
     *
     * @param id id of the entity that will be annotated.
     * @param annotationSet annotation set to be inserted.
     * @return a queryResult object containing the annotation set inserted.
     * @throws CatalogDBException when the annotation set could not be inserted.
     */
    QueryResult<AnnotationSet> createAnnotationSet(long id, AnnotationSet annotationSet) throws CatalogDBException;

    /**
     * Obtains all the annotation sets from id or just the one matching with the annotationSetName if provided.
     *
     * @param id id of the entity where the annotations are stored.
     * @param annotationSetName annotation set name of the annotation to be returned when provided.
     * @return a queryResult containing either all the annotation sets or just the one corresponding to the annotation set name if provided.
     * @throws CatalogDBException when the annotation set could not be retrieved due to a database error.
     */
    QueryResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName) throws CatalogDBException;

    /**
     * Obtains all the annotation sets from id or just the one matching with the annotationSetName if provided.
     *
     * @param id id of the entity where the annotations are stored.
     * @param annotationSetName annotation set name of the annotation to be returned when provided.
     * @return a queryResult containing either all the annotation sets or just the one corresponding to the annotation set name if provided
     * as key:value pairs.
     * @throws CatalogDBException when the annotation set could not be retrieved due to a database error.
     */
    QueryResult<ObjectMap> getAnnotationSetAsMap(long id, @Nullable String annotationSetName) throws CatalogDBException;

    /**
     * Updates the annotationSet with the new annotationSet provided.
     *
     * @param id id of the entity where the annotations are stored.
     * @param annotationSet new annotation set object that will replace the former annotationSet.
     * @return the annotation set after applying the changes.
     * @throws CatalogDBException when the update could not be done.
     */
    QueryResult<AnnotationSet> updateAnnotationSet(long id, AnnotationSet annotationSet) throws CatalogDBException;

    /**
     * Deletes the annotation set from the entity.
     *
     * @param id id of the entity where the annotation set is stored.
     * @param annotationSetName annotation set name of the annotation to be deleted.
     * @throws CatalogDBException when the deletion could not be performed.
     */
    void deleteAnnotationSet(long id, String annotationSetName) throws CatalogDBException;

    /**
     * Add the variable to all the possible annotations from the variableSetId using the default value.
     *
     * @param variableSetId variable set id to identify the annotations that will add a new annotation.
     * @param variable new variable that will be added.
     * @return the number of annotations that add the new annotation.
     * @throws CatalogDBException if the variable could not be added to an existing annotationSet.
     */
    QueryResult<Long> addVariableToAnnotations(long variableSetId, Variable variable) throws CatalogDBException;

    /**
     * This method will rename the id of all the annotations corresponding to the variableSetId changing oldName per newName.
     * This method cannot be called by any of the managers and will be only called when the user wants to rename the field of a variable
     * from a variableSet.
     * @param variableSetId Id of the variable to be renamed.
     * @param oldName Name of the field to be renamed.
     * @param newName New name that will be set.
     * @return the number of annotations that renamed the name.
     * @throws CatalogDBException when there is an error with database transactions.
     */
    QueryResult<Long> renameAnnotationField(long variableSetId, String oldName, String newName) throws CatalogDBException;

    /**
     * Remove the annotation with annotationName from the annotation set.
     *
     * @param variableSetId variable set id for which the annotationSets have to delete the annotation.
     * @param annotationName Annotation name.
     * @return the number of annotations that deleted the annotation.
     * @throws CatalogDBException when there is an error in the database.
     */
    QueryResult<Long> removeAnnotationField(long variableSetId, String annotationName) throws CatalogDBException;

    /**
     * Makes a groupBy to obtain the different values that every annotation has and the total number of each.
     *
     * @param variableSetId variable set id for which the group by will be done.
     * @return a list of Feature count with every different value.
     * @throws CatalogDBException when there is an error in the database.
     */
    QueryResult<VariableSummary> getAnnotationSummary(long variableSetId) throws CatalogDBException;
}
