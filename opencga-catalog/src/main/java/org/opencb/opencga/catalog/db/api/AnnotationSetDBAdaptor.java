/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.Variable;
import org.opencb.opencga.core.models.VariableSet;
import org.opencb.opencga.core.models.summaries.VariableSummary;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Created by pfurio on 06/07/16.
 */
public interface AnnotationSetDBAdaptor<T> extends DBAdaptor<T> {

    /**
     * Insert an annotation set object in the database to annotate the entity with id "id".
     *
     * @param id id of the entity that will be annotated.
     * @param variableSet variable set annotated by the annotationset.
     * @param annotationSet annotation set to be inserted.
     * @return a queryResult object containing the annotation set inserted.
     * @throws CatalogDBException when the annotation set could not be inserted.
     */
    QueryResult<AnnotationSet> createAnnotationSet(long id, VariableSet variableSet, AnnotationSet annotationSet) throws CatalogDBException;

    /**
     * Obtains all the annotation sets from id or just the one matching with the annotationSetName if provided.
     *
     * @param id id of the entity where the annotations are stored.
     * @param annotationSetName annotation set name of the annotation to be returned when provided.
     * @param options query options object.
     * @return a queryResult containing either all the annotation sets or just the one corresponding to the annotation set name if provided.
     * @throws CatalogDBException when the annotation set could not be retrieved due to a database error.
     */
    QueryResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName, QueryOptions options)
            throws CatalogDBException;

    /**
     * Obtains all the annotation sets from id or just the one matching with the annotationSetName if provided.
     *
     * @param resource id of the entity where the annotations are stored.
     * @param annotationSetName annotation set name of the annotation to be returned when provided.
     * @param studyPermission study permission.
     * @return a queryResult containing either all the annotation sets or just the one corresponding to the annotation set name if provided.
     * @throws CatalogDBException when the annotation set could not be retrieved due to a database error.
     * @throws CatalogAuthorizationException if the user does not have proper permissions.
     */
    QueryResult<AnnotationSet> getAnnotationSet(AbstractManager.MyResourceId resource, @Nullable String annotationSetName,
                                                String studyPermission) throws CatalogDBException, CatalogAuthorizationException;

    /**
     * Obtains all the annotation sets matching the parameters provided.
     *
     * @param resource resource of the entity where the annotations are stored.
     * @param variableSetId Variable set id.
     * @param annotation Annotations that will be queried.
     * @param studyPermission study permission.
     * @return a queryResult containing the mathching annotation sets.
     * @throws CatalogDBException when the annotation set could not be retrieved due to a database error.
     * @throws CatalogAuthorizationException if the user does not have proper permissions.
     */
    @Deprecated
    QueryResult<ObjectMap> searchAnnotationSetAsMap(AbstractManager.MyResourceId resource, long variableSetId, @Nullable String annotation,
                                                    String studyPermission) throws CatalogDBException, CatalogAuthorizationException;

    /**
     * Updates the annotationSet with the new annotationSet provided.
     *
     * @param id id of the entity where the annotations are stored.
     * @param variableSet Variable set.
     * @param annotationSet new annotation set object that will replace the former annotationSet.
     * @return the annotation set after applying the changes.
     * @throws CatalogDBException when the update could not be done.
     */
    QueryResult<AnnotationSet> updateAnnotationSet(long id, VariableSet variableSet, AnnotationSet annotationSet) throws CatalogDBException;

    QueryResult<T> update(long id, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException;

    QueryResult<Long> update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException;

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
     *
     * @param studyId study id.
     * @param variableSetId variable set id for which the group by will be done.
     * @return a list of Feature count with every different value.
     * @throws CatalogDBException when there is an error in the database.
     */
    QueryResult<VariableSummary> getAnnotationSummary(long studyId, long variableSetId) throws CatalogDBException;
}
