package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.authorization.CatalogPermission;
import org.opencb.opencga.catalog.authorization.StudyPermission;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IStudyManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StudyManager extends AbstractManager implements IStudyManager {

    protected static Logger logger = LoggerFactory.getLogger(StudyManager.class);

    @Deprecated
    public StudyManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager, AuditManager auditManager,
                        CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                        Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    public StudyManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager, AuditManager auditManager,
                        CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                        CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogConfiguration);
    }

    @Override
    public String getUserId(long studyId) throws CatalogException {
        return studyDBAdaptor.getStudyOwnerId(studyId);
    }

    @Override
    public Long getProjectId(long studyId) throws CatalogException {
        return studyDBAdaptor.getProjectIdByStudyId(studyId);
    }

    @Override
    public Long getStudyId(String studyId) throws CatalogException {
        if (StringUtils.isNumeric(studyId)) {
            return Long.parseLong(studyId);
        }

        String[] split = studyId.split("@");
        if (split.length != 2) {
            return -1L;
        }
        String[] projectStudy = split[1].replace(':', '/').split("/", 2);
        if (projectStudy.length != 2) {
            return -2L;
        }
        long projectId = projectDBAdaptor.getProjectId(split[0], projectStudy[0]);
        return studyDBAdaptor.getStudyId(projectId, projectStudy[1]);
    }

    @Deprecated
    @Override
    public QueryResult<Study> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(objectMap, "objectMap");
        // FIXME: Change the projectId, name... per CatalogStudyDBAdaptor.QueryParams...
        return create(
                objectMap.getInt("projectId", -1),
                objectMap.getString("name"),
                objectMap.getString("alias"),
                Study.Type.valueOf(objectMap.getString("type", Study.Type.CASE_CONTROL.toString())),
                objectMap.getString("creationDate"),
                objectMap.getString("description"),
                objectMap.get("status", Status.class, null),
                objectMap.getString("cipher"),
                objectMap.getString("uriScheme"),
                objectMap.get("uri", URI.class, null),
                objectMap.get("datastores", Map.class, null),
                objectMap.getMap("stats"),
                objectMap.getMap("attributes"),
                options, sessionId
        );
    }

    @Override
    public QueryResult<Study> create(long projectId, String name, String alias, Study.Type type, String creationDate,
                                     String description, Status status, String cipher, String uriScheme, URI uri,
                                     Map<File.Bioformat, DataStore> datastores, Map<String, Object> stats, Map<String, Object> attributes,
                                     QueryOptions options, String sessionId) throws CatalogException {

        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkParameter(alias, "alias");
        ParamUtils.checkObj(type, "type");
        ParamUtils.checkAlias(alias, "alias");
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        description = ParamUtils.defaultString(description, "");
//        creatorId = ParamUtils.defaultString(creatorId, userId);
        creationDate = ParamUtils.defaultString(creationDate, TimeUtils.getTime());
        status = ParamUtils.defaultObject(status, Status::new);
        cipher = ParamUtils.defaultString(cipher, "none");
        if (uri != null) {
            if (uri.getScheme() == null) {
                throw new CatalogException("StudyUri must specify the scheme");
            } else {
                if (uriScheme != null && !uriScheme.isEmpty()) {
                    if (!uriScheme.equals(uri.getScheme())) {
                        throw new CatalogException("StudyUri must specify the scheme");
                    }
                } else {
                    uriScheme = uri.getScheme();
                }
            }
        } else {
            uriScheme = catalogIOManagerFactory.getDefaultCatalogScheme();
        }
        datastores = ParamUtils.defaultObject(datastores, HashMap<File.Bioformat, DataStore>::new);
        stats = ParamUtils.defaultObject(stats, HashMap<String, Object>::new);
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        CatalogIOManager catalogIOManager = catalogIOManagerFactory.get(uriScheme);

//        String projectOwnerId = projectDBAdaptor.getProjectOwnerId(projectId);


        /* Check project permissions */
        authorizationManager.checkProjectPermission(projectId, userId, CatalogPermission.WRITE);

        LinkedList<File> files = new LinkedList<>();
        LinkedList<Experiment> experiments = new LinkedList<>();
        LinkedList<Job> jobs = new LinkedList<>();

        File rootFile = new File(".", File.Type.FOLDER, null, null, "", userId, "study root folder",
                new File.FileStatus(File.FileStatus.READY), 0);
        rootFile.setUri(uri);
        files.add(rootFile);

        Study study = new Study(-1, name, alias, type, userId, creationDate, description, status, TimeUtils.getTime(),
                0, cipher, new LinkedList<>(), AuthorizationManager.getDefaultRoles(new HashSet<>(Arrays.asList(userId))),
                experiments, files, jobs, new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), null, datastores,
                stats, attributes);

        /* CreateStudy */
        QueryResult<Study> result = studyDBAdaptor.createStudy(projectId, study, options);
        study = result.getResult().get(0);

        //URI studyUri;
        if (uri == null) {
            try {
                uri = catalogIOManager.createStudy(userId, Long.toString(projectId), Long.toString(study.getId()));
            } catch (CatalogIOException e) {
                e.printStackTrace();
                studyDBAdaptor.delete(study.getId(), false);
                throw e;
            }
        }

        study = studyDBAdaptor.update(study.getId(), new ObjectMap("uri", uri)).first();
        auditManager.recordCreation(AuditRecord.Resource.study, study.getId(), userId, study, null, null);
        long rootFileId = fileDBAdaptor.getFileId(study.getId(), "");    //Set studyUri to the root folder too
        rootFile = fileDBAdaptor.update(rootFileId, new ObjectMap("uri", uri)).first();
        auditManager.recordCreation(AuditRecord.Resource.file, rootFile.getId(), userId, rootFile, null, null);

        userDBAdaptor.updateUserLastActivity(userId);
        return result;
    }

    @Override
    public QueryResult<Study> share(long studyId, AclEntry acl) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult<Study> read(Long studyId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);

        QueryResult<Study> studyResult = studyDBAdaptor.getStudy(studyId, options);
        if (!studyResult.getResult().isEmpty()) {
            authorizationManager.filterFiles(userId, studyId, studyResult.getResult().get(0).getFiles());
        }

        return studyResult;
    }

    @Override
    public QueryResult<Study> readAll(Query query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        query = ParamUtils.defaultObject(query, Query::new);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        if (!options.containsKey("include") || options.get("include") == null || options.getAsStringList("include").isEmpty()) {
            options.addToListOption("exclude", "projects.studies.attributes.studyConfiguration");
        }

        QueryResult<Study> allStudies = studyDBAdaptor.get(query, options);
        List<Study> studies = allStudies.getResult();

        authorizationManager.filterStudies(userId, studies);
        allStudies.setResult(studies);
        allStudies.setNumResults(studies.size());
        allStudies.setNumTotalResults(studies.size());

        return allStudies;
    }

    @Override
    public QueryResult<Study> update(Long studyId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkId(studyId, "studyId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_STUDY);

        if (parameters.containsKey("alias")) {
            rename(studyId, parameters.getString("alias"), sessionId);

            //Clone and remove alias from parameters. Do not modify the original parameter
            parameters = new ObjectMap(parameters);
            parameters.remove("alias");
        }
        for (String s : parameters.keySet()) {
            if (!s.matches("name|type|description|status|attributes|stats")) {
                throw new CatalogDBException("Parameter '" + s + "' can't be changed");
            }
        }

        String ownerId = studyDBAdaptor.getStudyOwnerId(studyId);
        userDBAdaptor.updateUserLastActivity(ownerId);
        QueryResult<Study> result = studyDBAdaptor.update(studyId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.study, studyId, userId, parameters, null, null);
        return result;
    }

    private QueryResult rename(long studyId, String newStudyAlias, String sessionId)
            throws CatalogException {
        ParamUtils.checkAlias(newStudyAlias, "newStudyAlias");
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        String studyOwnerId = studyDBAdaptor.getStudyOwnerId(studyId);

        //User can't write/modify the study
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_STUDY);


        // Both users must bu updated
        userDBAdaptor.updateUserLastActivity(userId);
        userDBAdaptor.updateUserLastActivity(studyOwnerId);
        //TODO get all shared users to updateUserLastActivity

        //QueryResult queryResult = studyDBAdaptor.renameStudy(studyId, newStudyAlias);
        auditManager.recordUpdate(AuditRecord.Resource.study, studyId, userId, new ObjectMap("alias", newStudyAlias), null, null);
        return new QueryResult();

    }


    @Override
    public QueryResult<Study> delete(Long id, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult rank(long projectId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(projectId, "projectId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkProjectPermission(projectId, userId, CatalogPermission.READ);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = studyDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(long projectId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(projectId, "projectId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkProjectPermission(projectId, userId, CatalogPermission.READ);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = studyDBAdaptor.groupBy(query, field, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(long projectId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(projectId, "projectId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkProjectPermission(projectId, userId, CatalogPermission.READ);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = studyDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }



    /*
     * Variables Methods
     */

    @Override
    public QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique, String description,
                                                      Map<String, Object> attributes, List<Variable> variables, String sessionId)
            throws CatalogException {

        ParamUtils.checkObj(variables, "Variables List");
        Set<Variable> variablesSet = new HashSet<>(variables);
        if (variables.size() != variablesSet.size()) {
            throw new CatalogException("Error. Repeated variables");
        }
        return createVariableSet(studyId, name, unique, description, attributes, variablesSet, sessionId);
    }

    @Override
    public QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique, String description,
                                                      Map<String, Object> attributes, Set<Variable> variables, String sessionId)
            throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(variables, "Variables Set");
        unique = ParamUtils.defaultObject(unique, true);
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, new HashMap<String, Object>());

        for (Variable variable : variables) {
            ParamUtils.checkParameter(variable.getId(), "variable ID");
            ParamUtils.checkObj(variable.getType(), "variable Type");
            variable.setAllowedValues(ParamUtils.defaultObject(variable.getAllowedValues(), Collections.<String>emptyList()));
            variable.setAttributes(ParamUtils.defaultObject(variable.getAttributes(), Collections.<String, Object>emptyMap()));
            variable.setCategory(ParamUtils.defaultString(variable.getCategory(), ""));
            variable.setDependsOn(ParamUtils.defaultString(variable.getDependsOn(), ""));
            variable.setDescription(ParamUtils.defaultString(variable.getDescription(), ""));
//            variable.setRank(defaultString(variable.getDescription(), ""));
        }

        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);

        VariableSet variableSet = new VariableSet(-1, name, unique, description, variables, attributes);
        CatalogAnnotationsValidator.checkVariableSet(variableSet);

        QueryResult<VariableSet> queryResult = studyDBAdaptor.createVariableSet(studyId, variableSet);
        auditManager.recordCreation(AuditRecord.Resource.variableSet, queryResult.first().getId(), userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<VariableSet> readVariableSet(long variableSet, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long studyId = studyDBAdaptor.getStudyIdByVariableSetId(variableSet);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);
        return studyDBAdaptor.getVariableSet(variableSet, options);
    }

    @Override
    public QueryResult<VariableSet> readAllVariableSets(long studyId, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        return studyDBAdaptor.getAllVariableSets(studyId, options);
    }

    @Override
    public QueryResult<VariableSet> deleteVariableSet(long variableSetId, QueryOptions queryOptions, String sessionId) throws
            CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long studyId = studyDBAdaptor.getStudyIdByVariableSetId(variableSetId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);
        QueryResult<VariableSet> queryResult = studyDBAdaptor.deleteVariableSet(variableSetId, queryOptions);
        auditManager.recordDeletion(AuditRecord.Resource.variableSet, variableSetId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<VariableSet> addFieldToVariableSet(long variableSetId, Variable variable, String sessionId)
            throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long studyId = studyDBAdaptor.getStudyIdByVariableSetId(variableSetId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);
        QueryResult<VariableSet> queryResult = studyDBAdaptor.addFieldToVariableSet(variableSetId, variable);
        auditManager.recordDeletion(AuditRecord.Resource.variableSet, variableSetId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<VariableSet> removeFieldFromVariableSet(long variableSetId, String name, String sessionId)
            throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long studyId = studyDBAdaptor.getStudyIdByVariableSetId(variableSetId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);
        QueryResult<VariableSet> queryResult = studyDBAdaptor.removeFieldFromVariableSet(variableSetId, name);
        auditManager.recordDeletion(AuditRecord.Resource.variableSet, variableSetId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<VariableSet> renameFieldFromVariableSet(long variableSetId, String oldName, String newName, String sessionId)
            throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long studyId = studyDBAdaptor.getStudyIdByVariableSetId(variableSetId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);
        QueryResult<VariableSet> queryResult = studyDBAdaptor.renameFieldVariableSet(variableSetId, oldName, newName);
        auditManager.recordDeletion(AuditRecord.Resource.variableSet, variableSetId, userId, queryResult.first(), null, null);
        return queryResult;
    }
}
