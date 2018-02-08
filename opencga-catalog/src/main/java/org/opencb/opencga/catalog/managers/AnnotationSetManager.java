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
public abstract class AnnotationSetManager<R> extends ResourceManager<R> {

    public static final Pattern ANNOTATION_PATTERN = Pattern.compile("^([^:=^<>~!$]+:)?([^=^<>~!:$]+)([=^<>~!$]+.+)$");

    public static final String ANNOTATION_SETS = "annotationSets";
    public static final String ANNOTATIONS = "annotationSets.annotations";
    public static final String ANNOTATION_SET_NAME = "annotationSets.name";
    public static final String VARIABLE_SET = "annotationSets.variableSetId";

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

    /**
     * General method to create an annotation set that will have to be implemented. The managers implementing it will have to check the
     * validity of the sessionId and permissions and call the general createAnnotationSet implemented above.
     *
     * @param id                id of the entity being annotated.
     * @param studyStr          study string.
     * @param variableSetId     variable set id or name under which the annotation will be made.
     * @param annotationSetName annotation set name that will be used for the annotation.
     * @param annotations       map of annotations to create the annotation set.
     * @param sessionId         session id of the user asking for the operation.
     * @return a queryResult object with the annotation set created.
     * @throws CatalogException when the session id is not valid, the user does not have permissions or any of the annotation
     *                          parameters are not valid.
     */
    @Deprecated
    public QueryResult<AnnotationSet> createAnnotationSet(String id, @Nullable String studyStr, String variableSetId,
                                                          String annotationSetName, Map<String, Object> annotations, String sessionId)
            throws CatalogException {
        MyResourceId resourceId = getId(id, studyStr, sessionId);
        MyResourceId variableSetResource = catalogManager.getStudyManager().getVariableSetId(variableSetId,
                Long.toString(resourceId.getStudyId()), sessionId);
        AnnotationSet annotationSet = new AnnotationSet(annotationSetName, variableSetResource.getResourceId(), annotations,
                Collections.emptyMap());
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
            return new QueryResult<>("Create annotation set", update.getDbTime(), 0, 0, update.getWarningMsg(), update.getErrorMsg(),
                    Collections.emptyList());
        } else {
            Query query = new Query("id", resourceId.getResourceId());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + "." + annotationSetName);

            QueryResult<Annotable> queryResult = (QueryResult<Annotable>) get(String.valueOf(resourceId.getStudyId()), query, options,
                    sessionId);
            return new QueryResult<>("Create annotation set", update.getDbTime(), queryResult.first().getAnnotationSets().size(),
                    queryResult.first().getAnnotationSets().size(), queryResult.getWarningMsg(), queryResult.getErrorMsg(),
                    queryResult.first().getAnnotationSets());
        }

    }

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
    public QueryResult<AnnotationSet> updateAnnotationSet(String id, @Nullable String studyStr,
               String annotationSetName, Map<String, Object> newAnnotations, String sessionId) throws CatalogException {
        MyResourceId resourceId = getId(id, studyStr, sessionId);
        AnnotationSet annotationSet = new AnnotationSet(annotationSetName, -1, newAnnotations, Collections.emptyMap());
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
            Query query = new Query("id", resourceId.getResourceId());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + "." + annotationSetName);

            QueryResult<Annotable> queryResult = (QueryResult<Annotable>) get(String.valueOf(resourceId.getStudyId()), query, options,
                    sessionId);
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

        ObjectMap params = new ObjectMap(Constants.DELETE_ANNOTATION_SET, annotationSetName);

        QueryResult<R> update = update(studyStr, id, params, new QueryOptions(), sessionId);
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

        String[] annotationArray = StringUtils.split(annotations, ",");
        List<String> annotationList = new ArrayList<>(annotationArray.length);
        for (String annotation : annotationArray) {
            annotationList.add(annotationSetName + ":" + annotation);
        }

        ObjectMap params = new ObjectMap(Constants.DELETE_ANNOTATION, StringUtils.join(annotationList, ","));

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
            if (projection.startsWith(ANNOTATIONS + ".")) {
                returnedProjection.add(projection.replace(ANNOTATIONS + ".", Constants.ANNOTATION + "."));
            } else if (projection.startsWith(VARIABLE_SET + ".")) {
                returnedProjection.add(projection.replace(VARIABLE_SET + ".", Constants.VARIABLE_SET + "."));
            } else if (projection.startsWith(ANNOTATION_SET_NAME + ".")) {
                returnedProjection.add(projection.replace(ANNOTATION_SET_NAME + ".", Constants.ANNOTATION_SET_NAME + "."));
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
        if (query == null || query.isEmpty() || !query.containsKey(Constants.ANNOTATION)) {
            return;
        }

        List<String> originalAnnotationList = query.getAsStringList(Constants.ANNOTATION, ";");
        Map<String, Map<String, QueryParam.Type>> variableTypeMap = null;

        List<String> annotationList = new ArrayList<>(originalAnnotationList.size());
        ObjectMap queriedVariableTypeMap = new ObjectMap();

        for (String annotation : originalAnnotationList) {
            if (annotation.startsWith(Constants.VARIABLE_SET) || annotation.startsWith(Constants.ANNOTATION_SET_NAME)) {
                annotationList.add(annotation);
                continue;
            }

            if (variableTypeMap == null) {
                variableTypeMap = getVariableTypeMap(studyId);
            }

            // Split the annotation by key - value
            Matcher matcher = ANNOTATION_PATTERN.matcher(annotation);
            String variableSet;
            String key;
            String valueString;
            if (matcher.find()) {
                variableSet = matcher.group(1);
                key = matcher.group(2);
                valueString = matcher.group(3);

                if (StringUtils.isEmpty(variableSet)) {
                    // Obtain the variable set for the annotations
                    variableSet = searchVariableSetForVariable(variableTypeMap, key);
                } else {
                    // Remove the : at the end of the variableSet
                    variableSet = variableSet.replace(":", "");

                    // Check if the variable set and the variable exist
                    if (!variableTypeMap.containsKey(variableSet)) {
                        throw new CatalogException("The variable " + variableSet + " does not exist in the study " + studyId);
                    }
                    if (!variableTypeMap.get(variableSet).containsKey(key)) {
                        throw new CatalogException("Variable " + key + " from variableSet " + variableSet + " does not exist. Cannot "
                                + "perform query " + annotation);
                    }
                }

                annotationList.add(variableSet + ":" + key + valueString);
                queriedVariableTypeMap.put(variableSet + ":" + key, variableTypeMap.get(variableSet).get(key));
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

    private Map<String, Map<String, QueryParam.Type>> getVariableTypeMap(long studyId) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key());
        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(studyId, options);

        if (studyQueryResult.getNumResults() == 0) {
            throw new CatalogDBException("Unexpected error: Study id " + studyId + " not found");
        }

        Map<String, Map<String, QueryParam.Type>> variableTypeMap = new HashMap<>();
        List<VariableSet> variableSets = studyQueryResult.first().getVariableSets();
        if (variableSets != null) {
            for (VariableSet variableSet : variableSets) {
                variableTypeMap.put(String.valueOf(variableSet.getId()), getVariableMap(variableSet));
            }
        }

        return variableTypeMap;
    }

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
                    keys.add(variable.getName());
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
                keys.add(variable.getName());
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

        Map<Long, VariableSet> variableSetMap = new HashMap<>();
        for (VariableSet variableSet : variableSetList) {
            variableSetMap.put(variableSet.getId(), variableSet);
        }

        List<AnnotationSet> consideredAnnotationSetsList = new ArrayList<>(annotationSetList.size());

        Iterator<AnnotationSet> iterator = annotationSetList.iterator();
        while (iterator.hasNext()) {
            AnnotationSet annotationSet = iterator.next();
            String annotationSetName = annotationSet.getName();
            ParamUtils.checkAlias(annotationSetName, "annotationSetName", -1);

            // Get the variable set
            if (!variableSetMap.containsKey(annotationSet.getVariableSetId())) {
                throw new CatalogException("VariableSetId " + annotationSet.getVariableSetId() + " not found in study " + studyId);
            }
            VariableSet variableSet = variableSetMap.get(annotationSet.getVariableSetId());

            // Check validity of annotations and duplicities
            CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, consideredAnnotationSetsList);

            // Add the annotation to the list of annotations
            consideredAnnotationSetsList.add(annotationSet);
        }

        return variableSetList;
    }

    protected List<VariableSet> checkUpdateAnnotationsAndExtractVariableSets(MyResourceId resource, ObjectMap parameters,
                                                                             AnnotationSetDBAdaptor dbAdaptor)
            throws CatalogException {
        List<VariableSet> variableSetList = null;

        boolean confidentialPermissionsChecked = false;

        if (parameters.containsKey(ANNOTATION_SETS)) {
            Object annotationSetsObject = parameters.get(ANNOTATION_SETS);
            if (annotationSetsObject != null) {
                if (annotationSetsObject instanceof List) {
                    // This variable will contain the annotationSet list to be updated after applying minor fixes to the data (if required)
                    List<AnnotationSet> finalAnnotationList = new ArrayList<>();

                    // This variable will contain the actual action to be performed over every different annotation set (update or create)
                    // AnnotationSetName - Action (CREATE, UPDATE). This will be used by the AnnotationMongoDBAdaptor
                    Map<String, Action> annotationSetAction = new HashMap<>();

                    // Obtain all the variable sets from the study
                    QueryResult<Study> studyQueryResult = studyDBAdaptor.get(resource.getStudyId(),
                            new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));
                    if (studyQueryResult.getNumResults() == 0) {
                        throw new CatalogException("Internal error: Study " + resource.getStudyId() + " not found. Update could not be "
                                + "performed.");
                    }
                    variableSetList = studyQueryResult.first().getVariableSets();
                    if (variableSetList == null || variableSetList.isEmpty()) {
                        throw new CatalogException("Cannot annotate anything until at least a VariableSet has been defined in the study");
                    }
                    // Create a map variableSetId - VariableSet
                    Map<Long, VariableSet> variableSetMap = new HashMap<>();
                    for (VariableSet variableSet : variableSetList) {
                        variableSetMap.put(variableSet.getId(), variableSet);
                    }

                    // Get all the annotation sets from the entry
                    QueryResult<AnnotationSet> annotationSetQueryResult = dbAdaptor.getAnnotationSet(resource.getResourceId(), null,
                            QueryOptions.empty());
                    // Create a map annotationSetName - AnnotationSet
                    Map<String, AnnotationSet> annotationSetMap = new HashMap<>();
                    List<AnnotationSet> annotationSetList = new ArrayList<>();
                    if (annotationSetQueryResult != null && annotationSetQueryResult.getNumResults() > 0) {
                        for (AnnotationSet annotationSet : annotationSetQueryResult.getResult()) {
                            annotationSetMap.put(annotationSet.getName(), annotationSet);
                        }
                        annotationSetList.addAll(annotationSetQueryResult.getResult());
                    }


                    ObjectMapper jsonObjectMapper = new ObjectMapper();
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

                        // Validate that the annotation changes will still keep the annotation sets persistent (if already existed)

                        if (annotationSetMap.containsKey(annotationSet.getName())) {
                            // The annotationSet already exists so user wants to perform an update of the annotations

                            AnnotationSet annotationSetDB = annotationSetMap.get(annotationSet.getName());
                            if (annotationSet.getVariableSetId() > 0
                                    && annotationSet.getVariableSetId() != annotationSetDB.getVariableSetId()) {
                                throw new CatalogException("The VariableSetId and the AnnotationSetName of the annotation to be updated "
                                        + "do not match any of the AnnotationSets stored in the DB");
                            }

                            VariableSet variableSet = variableSetMap.get(annotationSetDB.getVariableSetId());
                            if (variableSet == null) {
                                logger.error("Critical error. The AnnotationSet {} from the sample {} from the DB points to "
                                                + "the non-existing VariableSet {}!!", annotationSet.getName(), resource.getResourceId(),
                                        annotationSet.getVariableSetId());
                                throw new CatalogException("Internal error: Something unexpected happened. Please, report the error to "
                                        + "the OpenCGA admins");
                            }

                            if (variableSet.isConfidential()) {
                                if (!confidentialPermissionsChecked) {
                                    authorizationManager.checkStudyPermission(resource.getStudyId(), resource.getUser(),
                                            StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS, "");
                                    confidentialPermissionsChecked = true;
                                }
                            }

                            // Merge the new annotations with the DB annotations
                            CatalogAnnotationsValidator.mergeNewAnnotations(annotationSetDB, annotationSet.getAnnotations());
                            // And validate the annotationSet would still be valid
                            CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSetDB, null);

                            // We only keep the annotations that will actually be updated
                            annotationSetDB.getAnnotations().entrySet()
                                    .removeIf(annotationEntry -> !annotationSet.getAnnotations().containsKey(annotationEntry.getKey()));

                            // Add the new annotationSet to the list of annotations to be updated
                            finalAnnotationList.add(annotationSetDB);

                            annotationSetAction.put(annotationSetDB.getName(), Action.UPDATE);

                        } else if (variableSetMap.containsKey(annotationSet.getVariableSetId())) {
                            // Create new annotationSet

                            if (variableSetMap.get(annotationSet.getVariableSetId()).isConfidential()) {
                                if (!confidentialPermissionsChecked) {
                                    authorizationManager.checkStudyPermission(resource.getStudyId(), resource.getUser(),
                                            StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS, "");
                                    confidentialPermissionsChecked = true;
                                }
                            }

                            // Validate the new annotationSet
                            CatalogAnnotationsValidator.checkAnnotationSet(variableSetMap.get(annotationSet.getVariableSetId()),
                                    annotationSet, annotationSetList);

                            // Add the new annotationSet to the annotationSetList for validation of other annotationSets (if any)
                            annotationSetList.add(annotationSet);

                            // Add the new annotationSet to the list of annotations to be updated
                            finalAnnotationList.add(annotationSet);

                            annotationSetAction.put(annotationSet.getName(), Action.CREATE);
                        } else {
                            throw new CatalogException("Neither the annotationSetName nor the variableSetId matches an existing "
                                    + "AnnotationSet to perform an update or a VariableSet to create a new annotation.");
                        }
                    }

                    parameters.put(ANNOTATION_SETS, finalAnnotationList);
                    parameters.put(ANNOTATION_SET_ACTION, annotationSetAction);

                } else {
                    throw new CatalogException(ANNOTATION_SETS + " must be a list of AnnotationSets");
                }
            } else {
                // Remove AnnotationSets from the parameters
                parameters.remove(ANNOTATION_SETS);
            }
        }

        // Are there any annotations to be deleted?
        if (StringUtils.isNotEmpty(parameters.getString(Constants.DELETE_ANNOTATION))) {
            // There are some annotations to be deleted
            String deleteAnnotationString = parameters.getString(Constants.DELETE_ANNOTATION);

            if (variableSetList == null) {
                // Obtain all the variable sets from the study
                QueryResult<Study> studyQueryResult = studyDBAdaptor.get(resource.getStudyId(),
                        new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));
                if (studyQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Internal error: Study " + resource.getStudyId() + " not found. Update could not be "
                            + "performed.");
                }
                variableSetList = studyQueryResult.first().getVariableSets();
                if (variableSetList == null || variableSetList.isEmpty()) {
                    throw new CatalogException("Cannot annotate anything until at least a VariableSet has been defined in the study");
                }
            }

            // Create a map variableSetId - VariableSet
            Map<Long, VariableSet> variableSetMap = new HashMap<>();
            for (VariableSet variableSet : variableSetList) {
                variableSetMap.put(variableSet.getId(), variableSet);
            }

            // Get all the annotation sets from the entry
            QueryResult<AnnotationSet> annotationSetQueryResult = dbAdaptor.getAnnotationSet(resource.getResourceId(), null,
                    QueryOptions.empty());
            // Create a map annotationSetName - AnnotationSet
            Map<String, AnnotationSet> annotationSetMap = new HashMap<>();
            if (annotationSetQueryResult != null && annotationSetQueryResult.getNumResults() > 0) {
                for (AnnotationSet annotationSet : annotationSetQueryResult.getResult()) {
                    annotationSetMap.put(annotationSet.getName(), annotationSet);
                }
            }

            for (String deleteAnnotation : StringUtils.split(deleteAnnotationString, ",")) {
                String[] split = StringUtils.split(deleteAnnotation, ":");
                if (split.length != 2) {
                    throw new CatalogException("Cannot delete annotation " + deleteAnnotation
                            + ". The format is annotationSetName:variable.");
                }

                String annotationSetName = split[0];
                AnnotationSet annotationSet = annotationSetMap.get(annotationSetName);
                if (annotationSet == null) {
                    throw new CatalogException("Cannot delete annotation from annotationSet " + annotationSetName + ". AnnotationSet not "
                            + "found");
                }

                if (variableSetMap.get(annotationSet.getVariableSetId()).isConfidential()) {
                    if (!confidentialPermissionsChecked) {
                        authorizationManager.checkStudyPermission(resource.getStudyId(), resource.getUser(),
                                StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS, "");
                        confidentialPermissionsChecked = true;
                    }
                }

                Map<String, Object> annotations = annotationSet.getAnnotations();
                String[] annotationKeys = StringUtils.split(split[1], ".");
                for (int i = 0; i < annotationKeys.length; i++) {
                    String annotationKey = annotationKeys[i];

                    if (i + 1 == annotationKeys.length) {
                        annotations.remove(annotationKey);
                    } else {
                        annotations = (Map<String, Object>) annotations.get(annotationKey);
                        if (annotations == null) {
                            throw new CatalogException("Cannot delete annotation " + deleteAnnotation + ". Annotation not found");
                        }
                    }
                }
            }

            // After applying all the deletions, we now check if all the annotationSets would still be valid
            for (Map.Entry<String, AnnotationSet> stringAnnotationSetEntry : annotationSetMap.entrySet()) {
                AnnotationSet annotationSet = stringAnnotationSetEntry.getValue();
                try {
                    CatalogAnnotationsValidator.checkAnnotationSet(variableSetMap.get(annotationSet.getVariableSetId()), annotationSet,
                            null);
                } catch (CatalogException e) {
                    throw new CatalogException("Cannot remove required annotation: " + e.getMessage(), e);
                }
            }
        }


        return variableSetList;
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
