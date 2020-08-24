/*
 * Copyright 2015-2020 OpenCB
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.AnnotationSetDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.PrivateStudyUid;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.TsvAnnotationParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;

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

    public enum Action {
        CREATE,
        UPDATE,
        DELETE_ANNOTATION,
        DELETE_ANNOTATION_SET
    }

    AnnotationSetManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                         DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
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

    public OpenCGAResult<Job> loadTsvAnnotations(String studyStr, String variableSetId, String path, TsvAnnotationParams tsvParams,
                                                 ObjectMap params, String toolId, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        ParamUtils.checkObj(variableSetId, "VariableSetId");
        ParamUtils.checkObj(tsvParams, "AnnotationTsvParams");
        ParamUtils.checkObj(path, "path");

        boolean parents = params.getBoolean("parents");
        String annotationSetId = params.getString("annotationSetId");
        if (StringUtils.isEmpty(annotationSetId)) {
            annotationSetId = variableSetId;
        }

        // Check variable set exists
        boolean variableSetExists = false;
        for (VariableSet variableSet : study.getVariableSets()) {
            if (variableSet.getId().equals(variableSetId)) {
                variableSetExists = true;
                break;
            }
        }
        if (!variableSetExists) {
            throw new CatalogException("Variable set '" + variableSetId + "' not found.");
        }

        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), path);

        OpenCGAResult<File> search = catalogManager.getFileManager().search(study.getFqn(), query, FileManager.INCLUDE_FILE_URI_PATH,
                token);
        if (search.getNumResults() == 0) {
            // File not found under the path. User must have provided a content so we can create the file.
            if (StringUtils.isEmpty(tsvParams.getContent())) {
                throw new CatalogParameterException("Missing content of the TSV file");
            }

            OpenCGAResult<File> result = catalogManager.getFileManager().create(studyStr, new File().setPath(path), parents,
                    tsvParams.getContent(), FileManager.INCLUDE_FILE_URI_PATH, token);
            path = result.first().getPath();
        } else {
            // File found.
            path = search.first().getPath();
        }

        // Submit job to load TSV annotations
        Map<String, Object> jobParams = new HashMap<>();
        jobParams.put("file", path);
        jobParams.put("variableSetId", variableSetId);
        jobParams.put("annotationSetId", annotationSetId);
        return catalogManager.getJobManager().submit(study.getFqn(), toolId, Enums.Priority.MEDIUM, jobParams, token);
    }

    protected  <T extends Annotable> void checkUpdateAnnotations(Study study, T entry, ObjectMap parameters, QueryOptions options,
                                                              VariableSet.AnnotableDataModels annotableEntity,
                                                              AnnotationSetDBAdaptor dbAdaptor, String user) throws CatalogException {

        List<VariableSet> variableSetList = study.getVariableSets();
        boolean confidentialPermissionsChecked = false;

        Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());

        // Create or remove annotation sets
        if (actionMap.containsKey(ANNOTATION_SETS)) {
            Object annotationSetsObject = parameters.get(ANNOTATION_SETS);
            if (annotationSetsObject != null) {
                if (annotationSetsObject instanceof List) {
                    ObjectMapper jsonObjectMapper = getDefaultObjectMapper();

                    ParamUtils.UpdateAction action = ParamUtils.UpdateAction.from(actionMap, ANNOTATION_SETS, ParamUtils.UpdateAction.ADD);

                    if (action == ParamUtils.UpdateAction.ADD || action == ParamUtils.UpdateAction.SET) {
                        /* We need to validate that the new annotationSets are fine to be stored */

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
                            OpenCGAResult<AnnotationSet> annotationSetDataResult = dbAdaptor.getAnnotationSet(entry.getUid(), null);

                            if (annotationSetDataResult != null && annotationSetDataResult.getNumResults() > 0) {
                                for (AnnotationSet annotationSet : annotationSetDataResult.getResults()) {
                                    annotationSetMap.put(annotationSet.getId(), annotationSet);
                                }
                                // We add all the existing annotation sets to the list
                                annotationSetList.addAll(annotationSetDataResult.getResults());
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
        } else if (actionMap.containsKey(ANNOTATIONS)) {
            // Update annotations (ADD, SET, REMOVE or RESET)
            Object annotationSetObject = parameters.get(ANNOTATION_SETS);
            if (annotationSetObject != null) {
                ParamUtils.CompleteUpdateAction action =
                        ParamUtils.CompleteUpdateAction.from(actionMap, ANNOTATIONS, ParamUtils.CompleteUpdateAction.ADD);

                if (!(annotationSetObject instanceof List)) {
                    throw new CatalogException("Unexpected annotationSets object. Expected a list of annotationSets.");
                }
                if (((List) annotationSetObject).size() > 1) {
                    throw new CatalogException("Expected a single annotation set in the list. Only an update of a single annotation set "
                            + "is supported at a time");
                }

                ObjectMapper jsonObjectMapper = getDefaultObjectMapper();
                AnnotationSet annotationSet;
                try {
                    annotationSet = jsonObjectMapper.readValue(jsonObjectMapper.writeValueAsString(((List) annotationSetObject).get(0)),
                            AnnotationSet.class);
                } catch (IOException e) {
                    logger.error("Could not parse annotation set object {} to annotation set class", annotationSetObject);
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
                OpenCGAResult<Study> studyDataResult = studyDBAdaptor.get(study.getUid(),
                        new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

                if (studyDataResult.getNumResults() == 0) {
                    throw new CatalogException("Internal error: Study " + study.getFqn() + " not found. Update could not be performed.");
                }
                // Create a map variableSetId - VariableSet
                Map<String, VariableSet> variableSetMap = new HashMap<>();
                for (VariableSet variableSet : variableSetList) {
                    variableSetMap.put(variableSet.getId(), variableSet);
                }

                // Get the annotation set from the entry
                OpenCGAResult<AnnotationSet> annotationSetDataResult = dbAdaptor.getAnnotationSet(entry.getUid(), annotationSet.getId());
                if (annotationSetDataResult.getNumResults() == 0) {
                    throw new CatalogException("AnnotationSet " + annotationSet.getId() + " not found. Annotations could not be updated.");
                }
                AnnotationSet storedAnnotationSet = annotationSetDataResult.first();

                // We apply the annotation changes to the storedAnotationSet object
                applyAnnotationChanges(storedAnnotationSet, annotationSet, variableSetMap.get(storedAnnotationSet.getVariableSetId()),
                        action);

                // Validate the annotationSet with the changes
                AnnotationUtils.checkAnnotationSet(variableSetMap.get(storedAnnotationSet.getVariableSetId()),
                        storedAnnotationSet, null, false);

                parameters.put(ANNOTATION_SETS, Collections.singletonList(storedAnnotationSet));
//            } else {
//                // Remove Annotations from the parameters
//                parameters.remove(ANNOTATIONS);
            }
        }
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
