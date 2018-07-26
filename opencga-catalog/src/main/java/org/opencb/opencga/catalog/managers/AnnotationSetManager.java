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
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.AnnotationSetDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidator;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by pfurio on 06/07/16.
 */
public abstract class AnnotationSetManager<R extends PrivateStudyUid> extends ResourceManager<R> {

    public static final Pattern ANNOTATION_PATTERN = Pattern.compile("^([^:=^<>~!$]+:)?([^=^<>~!:$]+)([=^<>~!$]+.+)$");
    public static final Pattern OPERATION_PATTERN = Pattern.compile("^()(<=?|>=?|!==?|!?=?~|==?=?)([^=<>~!]+.*)$");

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
        MyResource resource = getUid(id, studyStr, sessionId);
        AnnotationSet annotationSet = new AnnotationSet(annotationSetName, null, newAnnotations, Collections.emptyMap());
        ObjectMap parameters;
        ObjectMapper jsonObjectMapper = new ObjectMapper();

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
            Query query = new Query("uid", resource.getResource().getUid());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + "." + annotationSetName);

            QueryResult<Annotable> queryResult = (QueryResult<Annotable>) get(studyStr, query, options, sessionId);
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

    /**
     * OpenCGA supports several ways of defining include/exclude of annotation fields:
     *  - annotationSets.annotations.a.b.c where a.b.c would be variables that will be included/excluded.
     *    annotation.a.b.c would be a shortcut used for the same purpose defined above.
     *  - annotationSets.variableSetId.cancer where cancer would correspond with the variable set id to be included/excluded.
     *    variableSet.cancer would be the shortcut used for the same purpose defined above.
     *  - annotationSets.name.cancer1 where cancer1 would correspond with the annotationSetName to be included/excluded.
     *    annotationSet.cancer1 would be the shortcut used for the same purpose defined above.
     *
     * This method will modify all the possible full way annotation fields to the corresponding shortcuts.
     *
     * @param options QueryOptions object containing
     */
    public void fixQueryOptionAnnotation(QueryOptions options) {
        List<String> includeList = options.getAsStringList(QueryOptions.INCLUDE, ",");
        List<String> excludeList = options.getAsStringList(QueryOptions.EXCLUDE, ",");

        options.putIfNotEmpty(QueryOptions.INCLUDE, fixQueryOptionAnnotation(includeList));
        options.putIfNotEmpty(QueryOptions.EXCLUDE, fixQueryOptionAnnotation(excludeList));
    }

    private String fixQueryOptionAnnotation(List<String> projectionList) {
        if (projectionList == null || projectionList.isEmpty()) {
            return null;
        }

        List<String> returnedProjection = new ArrayList<>(projectionList.size());
        for (String projection : projectionList) {
            if (projection.startsWith(ANNOTATION_SETS_ANNOTATIONS + ".")) {
                returnedProjection.add(projection.replace(ANNOTATION_SETS_ANNOTATIONS + ".", Constants.ANNOTATION + "."));
            } else if (projection.startsWith(ANNOTATION_SETS_VARIABLE_SET_ID + ".")) {
                returnedProjection.add(projection.replace(ANNOTATION_SETS_VARIABLE_SET_ID + ".", Constants.VARIABLE_SET + "."));
            } else if (projection.startsWith(ANNOTATION_SETS_ID + ".")) {
                returnedProjection.add(projection.replace(ANNOTATION_SETS_ID + ".", Constants.ANNOTATION_SET_NAME + "."));
            } else {
                returnedProjection.add(projection);
            }
        }

        return StringUtils.join(returnedProjection, ",");
    }

    /**
     * Fixes any field that might be missing from the annotation built by the user so it is perfectly ready for the dbAdaptors to be parsed.
     *
     * @param studyId study id corresponding to the entry that is being queried.
     * @param query query object containing the annotation.
     * @throws CatalogException if there are unknown variables being queried, non-existing variable sets...
     */
    public void fixQueryAnnotationSearch(long studyId, Query query) throws CatalogException {
        fixQueryAnnotationSearch(studyId, null, query, false);
    }

    /**
     * Fixes any field that might be missing from the annotation built by the user so it is perfectly ready for the dbAdaptors to be parsed.
     *
     * @param studyId study id corresponding to the entry that is being queried.
     * @param user for which the confidential permission should be checked.
     * @param query query object containing the annotation.
     * @param checkConfidentialPermission check confidential permission if querying by a confidential annotation.
     * @throws CatalogException if there are unknown variables being queried, non-existing variable sets...
     */
    public void fixQueryAnnotationSearch(long studyId, String user, Query query, boolean checkConfidentialPermission)
            throws CatalogException {
        if (query == null || query.isEmpty() || !query.containsKey(Constants.ANNOTATION)) {
            return;
        }

        List<String> originalAnnotationList = query.getAsStringList(Constants.ANNOTATION, ";");
        Map<String, VariableSet> variableSetMap = null;
        Map<String, Map<String, QueryParam.Type>> variableTypeMap = new HashMap<>();

        List<String> annotationList = new ArrayList<>(originalAnnotationList.size());
        ObjectMap queriedVariableTypeMap = new ObjectMap();

        boolean confidentialPermissionChecked = false;

        for (String annotation : originalAnnotationList) {
            if (variableSetMap == null) {
                variableSetMap = getVariableSetMap(studyId);
                for (VariableSet variableSet : variableSetMap.values()) {
                    variableTypeMap.put(String.valueOf(variableSet.getUid()), getVariableMap(variableSet));
                }
            }

            if (annotation.startsWith(Constants.ANNOTATION_SET_NAME)) {
                annotationList.add(annotation);
                continue;
            }

            // Split the annotation by key - value
            Matcher matcher = ANNOTATION_PATTERN.matcher(annotation);
            if (matcher.find()) {

                if (annotation.startsWith(Constants.VARIABLE_SET)) {
                    String variableSetString = matcher.group(3);
                    // Obtain the operator to take it out and get only the actual value
                    String operator = getOperator(variableSetString);
                    variableSetString = variableSetString.replace(operator, "");

                    VariableSet variableSet = variableSetMap.get(variableSetString);
                    if (variableSet == null) {
                        throw new CatalogException("Variable set " + variableSetString + " not found");
                    }

                    if (checkConfidentialPermission && !confidentialPermissionChecked && variableSet.isConfidential()) {
                        // We only check the confidential permission if needed once
                        authorizationManager.checkStudyPermission(studyId, user,
                                StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS);
                        confidentialPermissionChecked = true;
                    }
                    annotationList.add(Constants.VARIABLE_SET + operator + variableSet.getUid());
                    continue;
                }

                String variableSetString = matcher.group(1);
                String key = matcher.group(2);
                String valueString = matcher.group(3);

                if (StringUtils.isEmpty(variableSetString)) {
                    // Obtain the variable set for the annotations
                    variableSetString = searchVariableSetForVariable(variableTypeMap, key);
                } else {
                    VariableSet variableSet = variableSetMap.get(variableSetString.replace(":", ""));
                    if (variableSet == null) {
                        throw new CatalogException("Variable set " + variableSetString + " not found");
                    }

                    // Remove the : at the end of the variableSet and convert the id into the uid
                    variableSetString = String.valueOf(variableSet.getUid());

                    // Check if the variable set and the variable exist
                    if (!variableTypeMap.containsKey(variableSetString)) {
                        throw new CatalogException("The variable " + variableSetString + " does not exist in the study " + studyId);
                    }
                    if (!variableTypeMap.get(variableSetString).containsKey(key)) {
                        throw new CatalogException("Variable " + key + " from variableSet " + variableSetString + " does not exist. Cannot "
                                + "perform query " + annotation);
                    }
                }

                if (checkConfidentialPermission && !confidentialPermissionChecked && variableSetMap.get(variableSetString)
                        .isConfidential()) {
                    // We only check the confidential permission if needed once
                    authorizationManager.checkStudyPermission(studyId, user,
                            StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS);
                    confidentialPermissionChecked = true;
                }

                annotationList.add(variableSetString + ":" + key + valueString);
                queriedVariableTypeMap.put(variableSetString + ":" + key, variableTypeMap.get(variableSetString).get(key));
            } else {
                throw new CatalogException("Annotation format from " + annotation + " not accepted. Supported format contains "
                        + "[variableSet:]variable=value");
            }
        }

        if (!annotationList.isEmpty()) {
            query.put(Constants.ANNOTATION, StringUtils.join(annotationList, ";"));
        }
        if (!queriedVariableTypeMap.isEmpty()) {
            query.put(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, queriedVariableTypeMap);
        }
    }

    private String getOperator(String queryValue) {
        Matcher matcher = OPERATION_PATTERN.matcher(queryValue);
        if (matcher.find()) {
            return matcher.group(2);
        }
        return null;
    }

    private String searchVariableSetForVariable(Map<String, Map<String, QueryParam.Type>> variableTypeMap, String variableKey)
            throws CatalogException {
        String variableId = null;
        for (Map.Entry<String, Map<String, QueryParam.Type>> variableTypeMapEntry : variableTypeMap.entrySet()) {
            Map<String, QueryParam.Type> variableMap = variableTypeMapEntry.getValue();
            if (variableMap.containsKey(variableKey)) {
                if (variableId == null) {
                    variableId = variableTypeMapEntry.getKey();
                } else {
                    throw new CatalogException("Found more than one Variable Set for the variable " + variableKey);
                }
            }
        }

        if (variableId == null) {
            throw new CatalogException("Cannot find a Variable Set to match the variable " + variableKey);
        }

        return variableId;
    }

    private Map<String, VariableSet> getVariableSetMap(long studyId) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key());
        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(studyId, options);

        if (studyQueryResult.getNumResults() == 0) {
            throw new CatalogDBException("Unexpected error: Study id " + studyId + " not found");
        }

        Map<String, VariableSet> variableSetMap = new HashMap<>();
        List<VariableSet> variableSets = studyQueryResult.first().getVariableSets();
        if (variableSets != null) {
            for (VariableSet variableSet : variableSets) {
                variableSetMap.put(variableSet.getId(), variableSet);
            }
        }

        return variableSetMap;
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

    private Map<String, QueryParam.Type> getVariableMap(VariableSet variableSet) throws CatalogDBException {
        Map<String, QueryParam.Type> variableTypeMap = new HashMap<>();

        Queue<VariableDepthMap> queue = new LinkedList<>();

        // We first insert all the variables
        for (Variable variable : variableSet.getVariables()) {
            queue.add(new VariableDepthMap(variable, Collections.emptyList()));
        }

        // We iterate while there are elements in the queue
        while (!queue.isEmpty()) {
            VariableDepthMap variableDepthMap = queue.remove();
            Variable variable = variableDepthMap.getVariable();

            if (variable.getType() == Variable.VariableType.OBJECT) {
                // We add the new nested variables to the queue
                for (Variable nestedVariable : variable.getVariableSet()) {
                    List<String> keys = new ArrayList<>(variableDepthMap.getKeys());
                    keys.add(variable.getId());
                    queue.add(new VariableDepthMap(nestedVariable, keys));
                }
            } else {
                QueryParam.Type type;
                switch (variable.getType()) {
                    case BOOLEAN:
                        type = QueryParam.Type.BOOLEAN;
                        break;
                    case CATEGORICAL:
                    case TEXT:
                        if (variable.isMultiValue()) {
                            type = QueryParam.Type.TEXT_ARRAY;
                        } else {
                            type = QueryParam.Type.TEXT;
                        }
                        break;
                    case INTEGER:
                        if (variable.isMultiValue()) {
                            type = QueryParam.Type.INTEGER_ARRAY;
                        } else {
                            type = QueryParam.Type.INTEGER;
                        }
                        break;
                    case DOUBLE:
                        if (variable.isMultiValue()) {
                            type = QueryParam.Type.DECIMAL_ARRAY;
                        } else {
                            type = QueryParam.Type.DOUBLE;
                        }
                        break;
                    case OBJECT:
                    default:
                        throw new CatalogDBException("Unexpected variable type detected: " + variable.getType());
                }
                List<String> keys = new ArrayList<>(variableDepthMap.getKeys());
                keys.add(variable.getId());
                variableTypeMap.put(StringUtils.join(keys, "."), type);
            }
        }

        return variableTypeMap;
    }

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
                throw new CatalogException("VariableSetId " + annotationSet.getVariableSetId() + " not found in study " + studyId);
            }
            VariableSet variableSet = variableSetMap.get(annotationSet.getVariableSetId());

            // Check validity of annotations and duplicities
            CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, consideredAnnotationSetsList, true);

            // Add the annotation to the list of annotations
            consideredAnnotationSetsList.add(annotationSet);
        }

        return variableSetList;
    }

    public List<VariableSet> checkUpdateAnnotationsAndExtractVariableSets(MyResource<? extends Annotable> resource,
                                                                          ObjectMap parameters, QueryOptions options,
                                                                          AnnotationSetDBAdaptor dbAdaptor) throws CatalogException {
        List<VariableSet> variableSetList = null;
        boolean confidentialPermissionsChecked = false;

        Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());

        // Create or remove annotation sets
        if (parameters.containsKey(ANNOTATION_SETS)) {
            Object annotationSetsObject = parameters.get(ANNOTATION_SETS);
            if (annotationSetsObject != null) {
                if (annotationSetsObject instanceof List) {
                    ObjectMapper jsonObjectMapper = new ObjectMapper();

                    ParamUtils.UpdateAction action = (ParamUtils.UpdateAction) actionMap.getOrDefault(ANNOTATION_SETS,
                            ParamUtils.UpdateAction.ADD);
//                    ParamUtils.UpdateAction action = ParamUtils.UpdateAction.valueOf(
//                            (String) actionMap.getOrDefault(ANNOTATION_SETS, ParamUtils.UpdateAction.ADD.name()));

                    if (action == ParamUtils.UpdateAction.ADD || action == ParamUtils.UpdateAction.SET) {
                        /* We need to validate that the new annotationSets are fine to be stored */

                        // Obtain all the variable sets from the study
                        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(resource.getStudy().getUid(),
                                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));
                        if (studyQueryResult.getNumResults() == 0) {
                            throw new CatalogException("Internal error: Study " + resource.getStudy().getFqn()
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
                            QueryResult<AnnotationSet> annotationSetQueryResult =
                                    dbAdaptor.getAnnotationSet(resource.getResource().getUid(), null);

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
                                // Create new annotationSet

                                if (variableSetMap.get(annotationSet.getVariableSetId()).isConfidential()) {
                                    if (!confidentialPermissionsChecked) {
                                        authorizationManager.checkStudyPermission(resource.getStudy().getUid(), resource.getUser(),
                                                StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS, "");
                                        confidentialPermissionsChecked = true;
                                    }
                                }

                                // Validate the new annotationSet
                                CatalogAnnotationsValidator.checkAnnotationSet(variableSetMap.get(annotationSet.getVariableSetId()),
                                        annotationSet, annotationSetList, true);

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

                ObjectMapper jsonObjectMapper = new ObjectMapper();
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
                QueryResult<Study> studyQueryResult = studyDBAdaptor.get(resource.getStudy().getUid(),
                        new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

                if (studyQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Internal error: Study " + resource.getStudy().getFqn()
                            + " not found. Update could not be performed.");
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
                QueryResult<AnnotationSet> annotationSetQueryResult = dbAdaptor.getAnnotationSet(resource.getResource().getUid(),
                        annotationSet.getId());
                if (annotationSetQueryResult.getNumResults() == 0) {
                    throw new CatalogException("AnnotationSet " + annotationSet.getId() + " not found. Annotations could not be updated.");
                }
                AnnotationSet storedAnnotationSet = annotationSetQueryResult.first();

                // We apply the annotation changes to the storedAnotationSet object
                applyAnnotationChanges(storedAnnotationSet, annotationSet, variableSetMap.get(storedAnnotationSet.getVariableSetId()),
                        action);

                // Validate the annotationSet with the changes
                CatalogAnnotationsValidator.checkAnnotationSet(variableSetMap.get(storedAnnotationSet.getVariableSetId()),
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

    private class VariableDepthMap {
        private Variable variable;
        private List<String> keys;

        VariableDepthMap(Variable variable, List<String> keys) {
            this.variable = variable;
            this.keys = keys;
        }

        public Variable getVariable() {
            return variable;
        }

        public List<String> getKeys() {
            return keys;
        }
    }

}
