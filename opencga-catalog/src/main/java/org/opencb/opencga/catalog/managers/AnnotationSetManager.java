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

package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.AnnotationSetDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created by pfurio on 06/07/16.
 */
public abstract class AnnotationSetManager<R extends PrivateStudyUid> extends ResourceManager<R> {

    public static final String ANNOTATION_SETS = "annotationSets";
    public static final String ANNOTATIONS = "annotations";
    public static final String ID = "id";
    public static final String VARIABLE_SET_ID = "variableSetId";

    public static final String ANNOTATION_SETS_ANNOTATIONS = "annotationSets.annotations";
    public static final String ANNOTATION_SETS_ID = "annotationSets.id";
    public static final String ANNOTATION_SETS_VARIABLE_SET_ID = "annotationSets.variableSetId";

    // Variables used to store additional information in the ObjectMap parameters variable containing the actual action to be performed
    // over the different AnnotationSets
    public static final String ANNOTATION_SET_ACTION = "_annotationSetAction";
    public enum Action {
        CREATE,
        UPDATE,
        DELETE_ANNOTATION,
        DELETE_ANNOTATION_SET
    }

    AnnotationSetManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                         DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) {
        return null;
    }

//    /**
//     * General method to create an annotation set that will have to be implemented. The managers implementing it will have to check the
//     * validity of the sessionId and permissions and call the general createAnnotationSet implemented above.
//     *
//     * @param id                id of the entity being annotated.
//     * @param studyStr          study string.
//     * @param variableSetId     variable set id or name under which the annotation will be made.
//     * @param annotationSetName annotation set name that will be used for the annotation.
//     * @param annotations       map of annotations to create the annotation set.
//     * @param sessionId         session id of the user asking for the operation.
//     * @return a queryResult object with the annotation set created.
//     * @throws CatalogException when the session id is not valid, the user does not have permissions or any of the annotation
//     *                          parameters are not valid.
//     */
//    @Deprecated
//    public QueryResult<AnnotationSet> createAnnotationSet(String id, @Nullable String studyStr, String variableSetId,
//                                                          String annotationSetName, Map<String, Object> annotations, String sessionId)
//            throws CatalogException {
//        MyResource resource = getUid(id, studyStr, sessionId);
////        MyResourceId variableSetResource = catalogManager.getStudyManager().getVariableSetId(variableSetId, studyStr, sessionId);
//        AnnotationSet annotationSet = new AnnotationSet(annotationSetName, variableSetId, annotations, Collections.emptyMap());
//        ObjectMap parameters;
//        ObjectMapper jsonObjectMapper = new ObjectMapper();
//
//        try {
//            parameters = new ObjectMap(jsonObjectMapper.writeValueAsString(annotationSet));
//            parameters = new ObjectMap(ANNOTATION_SETS, Arrays.asList(parameters));
//        } catch (JsonProcessingException e) {
//            logger.error("Error parsing AnnotationSet {} to ObjectMap: {}", annotationSet, e.getMessage(), e);
//            throw new CatalogException("Error parsing AnnotationSet to ObjectMap");
//        }
//
//        QueryResult<R> update = update(studyStr, id, parameters, QueryOptions.empty(), sessionId);
//        if (update.getNumResults() == 0) {
//            return new QueryResult<>("Create annotation set", update.getDbTime(), 0, 0, update.getWarningMsg(), update.getErrorMsg(),
//                    Collections.emptyList());
//        } else {
//            Query query = new Query("uid", resource.getResource().getUid());
//            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + "." + annotationSetName);
//
//            QueryResult<Annotable> queryResult = (QueryResult<Annotable>) get(studyStr, query, options, sessionId);
//            return new QueryResult<>("Create annotation set", update.getDbTime(), queryResult.first().getAnnotationSets().size(),
//                    queryResult.first().getAnnotationSets().size(), queryResult.getWarningMsg(), queryResult.getErrorMsg(),
//                    queryResult.first().getAnnotationSets());
//        }
//
//    }

    /**
     * Update the values of the annotation set.
     *
     * @param id                id of the entity storing the annotation.
     * @param studyStr          study string.
     * @param annotationSetName annotation set name of the annotation that will be returned.
     * @param newAnnotations    map with the annotations that will have to be changed with the new values.
     * @param sessionId         session id of the user asking for the annotation.
     * @return a queryResult object containing the annotation set after the update.
     * @throws CatalogException when the session id is not valid, the user does not have permissions to update the annotationSet,
     *                          the newAnnotations are not correct or the annotationSetName is not valid.
     */
    public QueryResult<AnnotationSet> updateAnnotationSet(String id, @Nullable String studyStr, String annotationSetName,
                                                          Map<String, Object> newAnnotations, String sessionId) throws CatalogException {
        AnnotationSet annotationSet = new AnnotationSet(annotationSetName, null, newAnnotations, Collections.emptyMap());
        ObjectMap parameters;
        ObjectMapper jsonObjectMapper = getDefaultObjectMapper();

        try {
            parameters = new ObjectMap(jsonObjectMapper.writeValueAsString(annotationSet));
            parameters = new ObjectMap(ANNOTATION_SETS, Arrays.asList(parameters));
        } catch (JsonProcessingException e) {
            logger.error("Error parsing AnnotationSet {} to ObjectMap: {}", annotationSet, e.getMessage(), e);
            throw new CatalogException("Error parsing AnnotationSet to ObjectMap");
        }

        QueryResult<R> update = update(studyStr, id, parameters, QueryOptions.empty(), sessionId);
        if (update.getNumResults() == 0) {
            return new QueryResult<>("Update annotation set", update.getDbTime(), 0, 0, update.getWarningMsg(), update.getErrorMsg(),
                    Collections.emptyList());
        } else {
            String userId = catalogManager.getUserManager().getUserId(sessionId);
            Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + "." + annotationSetName);
            QueryResult<Annotable> queryResult = (QueryResult<Annotable>) internalGet(study.getUid(), id, options, userId);
            return new QueryResult<>("Update annotation set", update.getDbTime(), queryResult.first().getAnnotationSets().size(),
                    queryResult.first().getAnnotationSets().size(), queryResult.getWarningMsg(), queryResult.getErrorMsg(),
                    queryResult.first().getAnnotationSets());
        }
    }

    /**
     * Deletes the annotation set.
     *
     * @param id                id of the entity storing the annotation.
     * @param studyStr          study string.
     * @param annotationSetName annotation set name of the annotation to be deleted.
     * @param sessionId         session id of the user asking for the annotation.
     * @return a queryResult object with the annotationSet that has been deleted.
     * @throws CatalogException when the session id is not valid, the user does not have permissions to delete the annotationSet or
     *                          the annotation set name is not valid.
     */
    @Deprecated
    public QueryResult<AnnotationSet> deleteAnnotationSet(String id, @Nullable String studyStr, String annotationSetName,
                                                          String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(annotationSetName)) {
            throw new CatalogException("Missing annotationSetName field");
        }

        ObjectMap params = new ObjectMap(AnnotationSetManager.ANNOTATION_SETS,
                Collections.singleton(new AnnotationSet().setId(annotationSetName)));
        QueryOptions options = new QueryOptions(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS,
                ParamUtils.UpdateAction.REMOVE));

        QueryResult<R> update = update(studyStr, id, params, options, sessionId);
        return new QueryResult<>("Delete annotationSet", update.getDbTime(), 0, 0, update.getWarningMsg(), update.getErrorMsg(),
                Collections.emptyList());
    }

    /**
     * Deletes (or puts to the default value if mandatory) a list of annotations from the annotation set.
     *
     * @param id                id of the entity storing the annotation.
     * @param studyStr          study string.
     * @param annotationSetName annotation set name of the annotation where the update will be made.
     * @param annotations       comma separated list of annotation names that will be deleted or updated to the default values.
     * @param sessionId         session id of the user asking for the annotation.
     * @return a queryResult object with the annotation set after applying the changes.
     * @throws CatalogException when the session id is not valid, the user does not have permissions to delete the annotationSet,
     *                          the annotation set name is not valid or any of the annotation names are not valid.
     */
    @Deprecated
    public QueryResult<AnnotationSet> deleteAnnotations(String id, @Nullable String studyStr, String annotationSetName, String annotations,
                                                        String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(annotations)) {
            throw new CatalogException("Missing annotations field");
        }
        if (StringUtils.isEmpty(annotationSetName)) {
            throw new CatalogException("Missing annotationSetName field");
        }

        ObjectMap params = new ObjectMap(AnnotationSetManager.ANNOTATIONS, new AnnotationSet(annotationSetName, "",
                new ObjectMap("remove", annotations)));
        QueryOptions options = new QueryOptions();
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, ParamUtils.CompleteUpdateAction.REMOVE));

        QueryResult<R> update = update(studyStr, id, params, new QueryOptions(), sessionId);
        return new QueryResult<>("Delete annotation", update.getDbTime(), 0, 0, update.getWarningMsg(), update.getErrorMsg(),
                Collections.emptyList());
    }

    //    private Map<String, Map<String, QueryParam.Type>> getVariableTypeMap(long studyId) throws CatalogDBException {
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key());
//        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(studyId, options);
//
//        if (studyQueryResult.getNumResults() == 0) {
//            throw new CatalogDBException("Unexpected error: Study id " + studyId + " not found");
//        }
//
//        Map<String, Map<String, QueryParam.Type>> variableTypeMap = new HashMap<>();
//        List<VariableSet> variableSets = studyQueryResult.first().getVariableSets();
//        if (variableSets != null) {
//            for (VariableSet variableSet : variableSets) {
//                variableTypeMap.put(String.valueOf(variableSet.getId()), getVariableMap(variableSet));
//            }
//        }
//
//        return variableTypeMap;
//    }

    @Deprecated
    protected List<VariableSet> validateNewAnnotationSetsAndExtractVariableSets(long studyId, List<AnnotationSet> annotationSetList)
            throws CatalogException {
        if (annotationSetList == null || annotationSetList.isEmpty()) {
            return Collections.emptyList();
        }

        // Get all variableSets
        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(studyId,
                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));
        if (studyQueryResult.getNumResults() == 0) {
            throw new CatalogException("Unexpected error: Study " + studyId + " not found");
        }

        List<VariableSet> variableSetList = studyQueryResult.first().getVariableSets();
        if (variableSetList == null || variableSetList.isEmpty()) {
            throw new CatalogException("Impossible annotating variables from a study without VariableSets defined");
        }

        validateNewAnnotationSets(variableSetList, annotationSetList);

        return variableSetList;
    }

    protected void validateNewAnnotationSets(List<VariableSet> variableSetList, List<AnnotationSet> annotationSetList)
            throws CatalogException {
        if (annotationSetList == null || annotationSetList.isEmpty()) {
            return;
        }

        if (variableSetList == null || variableSetList.isEmpty()) {
            throw new CatalogException("Impossible annotating variables from a study without VariableSets defined");
        }

        Map<String, VariableSet> variableSetMap = new HashMap<>();
        for (VariableSet variableSet : variableSetList) {
            variableSetMap.put(variableSet.getId(), variableSet);
        }

        List<AnnotationSet> consideredAnnotationSetsList = new ArrayList<>(annotationSetList.size());

        Iterator<AnnotationSet> iterator = annotationSetList.iterator();
        while (iterator.hasNext()) {
            AnnotationSet annotationSet = iterator.next();
            String annotationSetName = annotationSet.getId();
            ParamUtils.checkAlias(annotationSetName, "annotationSetName");

            // Get the variable set
            if (!variableSetMap.containsKey(annotationSet.getVariableSetId())) {
                throw new CatalogException("VariableSetId " + annotationSet.getVariableSetId() + " not found in variable set list");
            }
            VariableSet variableSet = variableSetMap.get(annotationSet.getVariableSetId());

            // Check validity of annotations and duplicities
            AnnotationUtils.checkAnnotationSet(variableSet, annotationSet, consideredAnnotationSetsList, true);

            // Add the annotation to the list of annotations
            consideredAnnotationSetsList.add(annotationSet);
        }
    }

    public <T extends Annotable> List<VariableSet> checkUpdateAnnotationsAndExtractVariableSets(
            Study study, T entry, ObjectMap parameters, QueryOptions options, VariableSet.AnnotableDataModels annotableEntity,
            AnnotationSetDBAdaptor dbAdaptor, String user) throws CatalogException {

        List<VariableSet> variableSetList = null;
        boolean confidentialPermissionsChecked = false;

        Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());

        // Create or remove annotation sets
        if (parameters.containsKey(ANNOTATION_SETS)) {
            Object annotationSetsObject = parameters.get(ANNOTATION_SETS);
            if (annotationSetsObject != null) {
                if (annotationSetsObject instanceof List) {
                    ObjectMapper jsonObjectMapper = getDefaultObjectMapper();

                    ParamUtils.UpdateAction action = (ParamUtils.UpdateAction) actionMap.getOrDefault(ANNOTATION_SETS,
                            ParamUtils.UpdateAction.ADD);
//                    ParamUtils.UpdateAction action = ParamUtils.UpdateAction.valueOf(
//                            (String) actionMap.getOrDefault(ANNOTATION_SETS, ParamUtils.UpdateAction.ADD.name()));

                    if (action == ParamUtils.UpdateAction.ADD || action == ParamUtils.UpdateAction.SET) {
                        /* We need to validate that the new annotationSets are fine to be stored */

                        // Obtain all the variable sets from the study
                        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(study.getUid(),
                                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));
                        if (studyQueryResult.getNumResults() == 0) {
                            throw new CatalogException("Internal error: Study " + study.getFqn()
                                    + " not found. Update could not be performed.");
                        }
                        variableSetList = studyQueryResult.first().getVariableSets();
                        if (variableSetList == null || variableSetList.isEmpty()) {
                            throw new CatalogException("Cannot annotate anything until at least a VariableSet exists in "
                                    + "the study");
                        }
                        // Create a map variableSetId - VariableSet
                        Map<String, VariableSet> variableSetMap = new HashMap<>();
                        for (VariableSet variableSet : variableSetList) {
                            variableSetMap.put(variableSet.getId(), variableSet);
                        }

                        // Create a map annotationSetName - AnnotationSet
                        Map<String, AnnotationSet> annotationSetMap = new HashMap<>();
                        List<AnnotationSet> annotationSetList = new ArrayList<>();
                        if (action == ParamUtils.UpdateAction.ADD) {
                            // Get all the annotation sets from the entry
                            QueryResult<AnnotationSet> annotationSetQueryResult = dbAdaptor.getAnnotationSet(entry.getUid(), null);

                            if (annotationSetQueryResult != null && annotationSetQueryResult.getNumResults() > 0) {
                                for (AnnotationSet annotationSet : annotationSetQueryResult.getResult()) {
                                    annotationSetMap.put(annotationSet.getId(), annotationSet);
                                }
                                // We add all the existing annotation sets to the list
                                annotationSetList.addAll(annotationSetQueryResult.getResult());
                            }
                        }

                        // This variable will contain the annotationSet list to be added after applying minor fixes to the data (if
                        // necessary)
                        List<AnnotationSet> finalAnnotationList = new ArrayList<>();

                        for (Object annotationSetObject : ((List) annotationSetsObject)) {
                            AnnotationSet annotationSet;
                            try {
                                annotationSet = jsonObjectMapper.readValue(jsonObjectMapper.writeValueAsString(annotationSetObject),
                                        AnnotationSet.class);
                            } catch (IOException e) {
                                logger.error("Could not parse annotation set object {} to annotation set class", annotationSetObject);
                                throw new CatalogException("Internal error: Could not parse AnnotationSet object to AnnotationSet "
                                        + "class. Update could not be performed.");
                            }

                            /* Validate that the annotation changes will still keep the annotation sets persistent (if already existed) */
                            if (annotationSetMap.containsKey(annotationSet.getId())) {
                                // This should only happen if the action is ADD. When the action is SET, the annotationSetMap will be
                                // empty for starters
                                throw new CatalogException("Cannot add AnnotationSet " + annotationSet.getId() + ". An AnnotationSet with "
                                        + "the same id was already found.");
                            } else if (variableSetMap.containsKey(annotationSet.getVariableSetId())) {

                                // Validate annotable data model
                                VariableSet variableSet = variableSetMap.get(annotationSet.getVariableSetId());
                                if (ListUtils.isNotEmpty(variableSet.getEntities())
                                        && !variableSet.getEntities().contains(annotableEntity)) {
                                    throw new CatalogException("Cannot annotate " + annotableEntity + " using VariableSet '"
                                            + variableSet.getId() + "'. VariableSet is intended only for '"
                                            + StringUtils.join(variableSet.getEntities(), ",") + "' entities.");
                                }

                                // Create new annotationSet

                                if (variableSet.isConfidential()) {
                                    if (!confidentialPermissionsChecked) {
                                        authorizationManager.checkStudyPermission(study.getUid(), user,
                                                StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS, "");
                                        confidentialPermissionsChecked = true;
                                    }
                                }

                                // Validate the new annotationSet
                                AnnotationUtils.checkAnnotationSet(variableSet, annotationSet, annotationSetList, true);

                                // Add the new annotationSet to the annotationSetList for validation of other annotationSets (if any)
                                annotationSetList.add(annotationSet);

                                // Add the new annotationSet to the list of annotations to be updated
                                finalAnnotationList.add(annotationSet);
                            } else {
                                throw new CatalogException("Neither the annotationSetName nor the variableSetId matches an existing "
                                        + "AnnotationSet to perform an update or a VariableSet to create a new annotation.");
                            }
                        }

                        // Override the list of annotationSets
                        parameters.put(ANNOTATION_SETS, finalAnnotationList);

                    } else if (action == ParamUtils.UpdateAction.REMOVE) {
                        for (Object annotationSetObject : ((List) annotationSetsObject)) {
                            AnnotationSet annotationSet;
                            try {
                                annotationSet = jsonObjectMapper.readValue(jsonObjectMapper.writeValueAsString(annotationSetObject),
                                        AnnotationSet.class);
                            } catch (IOException e) {
                                logger.error("Could not parse annotation set object {} to annotation set class", annotationSetObject);
                                throw new CatalogException("Internal error: Could not parse AnnotationSet object to AnnotationSet class. "
                                        + "Update could not be performed.");
                            }

                            // Check the annotationSet ids are present
                            if (StringUtils.isEmpty(annotationSet.getId())) {
                                throw new CatalogException("Cannot remove annotationSet. Mandatory annotationSet id field is empty");
                            }
                        }
                    } else {
                        throw new CatalogException("Unrecognised annotationSet action " + action);
                    }

                } else {
                    throw new CatalogException(ANNOTATION_SETS + " must be a list of AnnotationSets");
                }
            } else {
                // Remove AnnotationSets from the parameters
                parameters.remove(ANNOTATION_SETS);
            }
        } else if (parameters.containsKey(ANNOTATIONS)) {
            // Update annotations (ADD, SET, REMOVE or RESET)
            Object annotationsObject = parameters.get(ANNOTATIONS);
            if (annotationsObject != null) {
                ParamUtils.CompleteUpdateAction action = (ParamUtils.CompleteUpdateAction) actionMap.getOrDefault(ANNOTATIONS,
                        ParamUtils.CompleteUpdateAction.ADD);

                ObjectMapper jsonObjectMapper = getDefaultObjectMapper();
                AnnotationSet annotationSet;
                try {
                    annotationSet = jsonObjectMapper.readValue(jsonObjectMapper.writeValueAsString(annotationsObject),
                            AnnotationSet.class);
                } catch (IOException e) {
                    logger.error("Could not parse annotation set object {} to annotation set class", annotationsObject);
                    throw new CatalogException("Internal error: Could not parse AnnotationSet object to AnnotationSet "
                            + "class. Update could not be performed.");
                }

                // Check the annotation set id is provided at least
                ParamUtils.checkParameter(annotationSet.getId(), "annotationSet id");

                if (action == ParamUtils.CompleteUpdateAction.RESET && (annotationSet.getAnnotations() == null
                        || annotationSet.getAnnotations().size() != 1 || !annotationSet.getAnnotations().containsKey("reset"))) {
                    throw new CatalogException("Expected annotation key 'reset' not found");
                }
                if (action == ParamUtils.CompleteUpdateAction.REMOVE && (annotationSet.getAnnotations() == null
                        || annotationSet.getAnnotations().size() != 1 || !annotationSet.getAnnotations().containsKey("remove"))) {
                    throw new CatalogException("Expected annotation key 'remove' not found");
                }
                if (action == ParamUtils.CompleteUpdateAction.ADD && (annotationSet.getAnnotations() == null
                        || annotationSet.getAnnotations().isEmpty())) {
                    throw new CatalogException("Missing annotations to add to the annotationSet");
                }
                if (action == ParamUtils.CompleteUpdateAction.REPLACE && (annotationSet.getAnnotations() == null
                        || annotationSet.getAnnotations().isEmpty())) {
                    throw new CatalogException("Missing annotations to replace");
                }

                // Obtain all the variable sets from the study
                QueryResult<Study> studyQueryResult = studyDBAdaptor.get(study.getUid(),
                        new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

                if (studyQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Internal error: Study " + study.getFqn() + " not found. Update could not be performed.");
                }
                variableSetList = studyQueryResult.first().getVariableSets();
                if (variableSetList == null || variableSetList.isEmpty()) {
                    throw new CatalogException("Cannot annotate anything until at least a VariableSet has been defined in the study");
                }
                // Create a map variableSetId - VariableSet
                Map<String, VariableSet> variableSetMap = new HashMap<>();
                for (VariableSet variableSet : variableSetList) {
                    variableSetMap.put(variableSet.getId(), variableSet);
                }

                // Get the annotation set from the entry
                QueryResult<AnnotationSet> annotationSetQueryResult = dbAdaptor.getAnnotationSet(entry.getUid(), annotationSet.getId());
                if (annotationSetQueryResult.getNumResults() == 0) {
                    throw new CatalogException("AnnotationSet " + annotationSet.getId() + " not found. Annotations could not be updated.");
                }
                AnnotationSet storedAnnotationSet = annotationSetQueryResult.first();

                // We apply the annotation changes to the storedAnotationSet object
                applyAnnotationChanges(storedAnnotationSet, annotationSet, variableSetMap.get(storedAnnotationSet.getVariableSetId()),
                        action);

                // Validate the annotationSet with the changes
                AnnotationUtils.checkAnnotationSet(variableSetMap.get(storedAnnotationSet.getVariableSetId()),
                        storedAnnotationSet, null, false);

                parameters.put(ANNOTATIONS, storedAnnotationSet);
            } else {
                // Remove Annotations from the parameters
                parameters.remove(ANNOTATIONS);
            }
        }

        return variableSetList;
    }

    private void applyAnnotationChanges(AnnotationSet targetAnnotationSet, AnnotationSet sourceAnnotationSet, VariableSet variableSet,
                                        ParamUtils.CompleteUpdateAction action) throws CatalogException {
        if (action == ParamUtils.CompleteUpdateAction.ADD || action == ParamUtils.CompleteUpdateAction.SET
                || action == ParamUtils.CompleteUpdateAction.REPLACE) {
            if (action == ParamUtils.CompleteUpdateAction.SET) {
                // We empty the current annotations map
                targetAnnotationSet.setAnnotations(new HashMap<>());
            }

            if (action == ParamUtils.CompleteUpdateAction.REPLACE) {
                // We need to check there already existed annotations to actually replace the values
                replaceAnnotations(targetAnnotationSet.getAnnotations(), sourceAnnotationSet.getAnnotations());
            } else {
                // We fill in with the annotations provided by the user
                targetAnnotationSet.getAnnotations().putAll(sourceAnnotationSet.getAnnotations());
            }
        } else if (action == ParamUtils.CompleteUpdateAction.RESET) {
            String resetFields = (String) sourceAnnotationSet.getAnnotations().get("reset");

            String[] split = resetFields.split(",");
            for (String variable : split) {
                try {
                    resetAnnotation(targetAnnotationSet.getAnnotations(), variable, variableSet.getVariables());
                } catch (CatalogException e) {
                    throw new CatalogException(variable + ": " + e.getMessage(), e);
                }
            }
        } else if (action == ParamUtils.CompleteUpdateAction.REMOVE) {
            String removeFields = (String) sourceAnnotationSet.getAnnotations().get("remove");

            String[] split = removeFields.split(",");
            for (String variable : split) {
                try {
                    removeAnnotation(targetAnnotationSet.getAnnotations(), variable, variableSet.getVariables());
                } catch (CatalogException e) {
                    throw new CatalogException(variable + ": " + e.getMessage(), e);
                }
            }
        }


    }

    private void replaceAnnotations(Map<String, Object> target, Map<String, Object> source) {
        for (String annKey : source.keySet()) {
            if (target.containsKey(annKey)) {
                // Check the value
                if (source.get(annKey) instanceof Map) {
                    if (target.get(annKey) instanceof Map) {
                        replaceAnnotations((Map<String, Object>) target.get(annKey), (Map<String, Object>) source.get(annKey));
                    }
                } else {
                    target.put(annKey, source.get(annKey));
                }
            }
        }
    }

    private void resetAnnotation(Map<String, Object> annotations, String annotation, Set<Variable> variables) throws CatalogException {
        String[] split = StringUtils.split(annotation, ".", 2);
        if (split.length > 1) {
            if (annotations.containsKey(split[0])) {
                Set<Variable> nestedVariables = null;
                for (Variable tmpVariable : variables) {
                    if (tmpVariable.getId().equals(split[0])) {
                        nestedVariables = tmpVariable.getVariableSet();
                        break;
                    }
                }
                if (nestedVariables == null) {
                    throw new CatalogException("Internal error. Could not validate reset of variable.");
                }

                removeAnnotation((Map<String, Object>) annotations.get(split[0]), split[1], nestedVariables);
            }
        } else {
            boolean reset = false;
            for (Variable variable : variables) {
                if (variable.getId().equals(annotation)) {
                    // We check if there is a default value
                    if (variable.getDefaultValue() == null) {
                        throw new CatalogException("No default value found for variable.");
                    } else {
                        annotations.put(annotation, variable.getDefaultValue());
                        reset = true;
                    }
                    break;
                }
            }
            if (!reset) {
                // We should never enter into this piece of code
                throw new CatalogException("Internal error. Could not validate reset of variable.");
            }
        }
    }

    private void removeAnnotation(Map<String, Object> annotations, String annotation, Set<Variable> variables) throws CatalogException {
        String[] split = StringUtils.split(annotation, ".", 2);
        if (split.length > 1) {
            if (annotations.containsKey(split[0])) {
                Set<Variable> nestedVariables = null;
                for (Variable tmpVariable : variables) {
                    if (tmpVariable.getId().equals(split[0])) {
                        nestedVariables = tmpVariable.getVariableSet();
                        break;
                    }
                }
                if (nestedVariables == null) {
                    throw new CatalogException("Internal error. Could not validate removal of variable.");
                }

                removeAnnotation((Map<String, Object>) annotations.get(split[0]), split[1], nestedVariables);
            }
        } else {
            for (Variable variable : variables) {
                if (variable.getId().equals(annotation)) {
                    // We check if the annotation to be remove is mandatory
                    if (variable.isRequired()) {
                        throw new CatalogException("Cannot remove required variable.");
                    }
                    break;
                }
            }

            annotations.remove(annotation);
        }
    }

}
