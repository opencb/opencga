package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AnnotationSet;

import javax.annotation.Nullable;

/**
 * Created by pfurio on 06/07/16.
 */
public interface CatalogAnnotationSetDBAdaptor {

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

}
