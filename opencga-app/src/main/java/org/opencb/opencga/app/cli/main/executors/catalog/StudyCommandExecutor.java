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

package org.opencb.opencga.app.cli.main.executors.catalog;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.StudyCommandOptions;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.VariableSet;

import java.io.IOException;
import java.util.List;

/**
 * Created by imedina on 03/06/16.
 */
public class StudyCommandExecutor extends OpencgaCommandExecutor {
    // TODO: Add include/exclude/skip/... (queryOptions) to the client calls !!!!

    private StudyCommandOptions studiesCommandOptions;
    private AclCommandExecutor<Study> aclCommandExecutor;

    public StudyCommandExecutor(StudyCommandOptions studiesCommandOptions) {
        super(studiesCommandOptions.commonCommandOptions);
        this.studiesCommandOptions = studiesCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {

        String subCommandString = getParsedSubCommand(studiesCommandOptions.jCommander);
        DataResponse queryResponse = null;
        logger.debug("Executing studies command line: {}", subCommandString);
        switch (subCommandString) {
            case "create":
                queryResponse = create();
                break;
            case "info":
                queryResponse = info();
                break;
            case "update":
                queryResponse = update();
                break;
            case "delete":
                queryResponse = delete();
                break;
            case "stats":
                queryResponse = stats();
                break;
            case "search":
                queryResponse = search();
                break;
            case "scan-files":
                queryResponse = scanFiles();
                break;
            case "resync-files":
                queryResponse = resyncFiles();
                break;
            case "acl":
                queryResponse = getAcl();
                break;
            case "acl-update":
                queryResponse = updateAcl();
                break;
            case "groups":
                queryResponse = groups();
                break;
            case "groups-create":
                queryResponse = groupsCreate();
                break;
            case "groups-delete":
                queryResponse = groupsDelete();
                break;
            case "groups-update":
                queryResponse = groupsUpdate();
                break;
            case "members-update":
                queryResponse = membersUpdate();
                break;
            case "admins-update":
                queryResponse = adminsUpdate();
                break;
            case "variable-sets":
                queryResponse = variableSets();
                break;
            case "variable-sets-update":
                queryResponse = variableSetUpdate();
                break;
            case "variable-sets-variables-update":
                queryResponse = variableSetVariableUpdate();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    /**
     * This method selects a single valid study from these sources and in this order. First, checks if CLI param exists,
     * second it reads the configuration file, and third it reads the projects and studies from the session file.
     * @param study parameter from the CLI
     * @return a singe valid Study from the CLI, configuration or from the session file
     * @throws CatalogException when no possible single study can be chosen
     */
    private String getSingleValidStudy(String study) throws CatalogException {
        // First, check the study parameter, if is not empty we just return it, this the user's selection.
        if (StringUtils.isNotEmpty(study)) {
            return resolveStudy(study);
        } else {
            // Second, check if there is a default study in the client configuration.
            if (StringUtils.isNotEmpty(clientConfiguration.getDefaultStudy())) {
                return clientConfiguration.getDefaultStudy();
            } else {
                // Third, check if there is only one single project and study for this user in the current CLI session file.
                List<String> studies = cliSession == null ? null : cliSession.getStudies();
                if (ListUtils.isNotEmpty(studies) && studies.size() == 1) {
                    study = studies.get(0);
                } else {
                    throw new CatalogException("None or more than one study found");
                }
            }
        }
        return study;
    }

    /**********************************************  Administration Commands  ***********************************************/

    private DataResponse<Study> create() throws CatalogException, IOException {
        logger.debug("Creating a new study");

        StudyCommandOptions.CreateCommandOptions commandOptions = studiesCommandOptions.createCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.DESCRIPTION.key(), commandOptions.description);
        params.putIfNotNull(StudyDBAdaptor.QueryParams.TYPE.key(), Study.Type.valueOf(commandOptions.type));
        params.put(StudyDBAdaptor.QueryParams.NAME.key(), commandOptions.name);
        params.putIfNotEmpty("alias", commandOptions.alias);

        return openCGAClient.getStudyClient().create(commandOptions.project, commandOptions.id, params);
    }

    private DataResponse<Study> info() throws CatalogException, IOException {
        logger.debug("Getting the study info");

        studiesCommandOptions.infoCommandOptions.study = getSingleValidStudy(studiesCommandOptions.infoCommandOptions.study);
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, studiesCommandOptions.infoCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, studiesCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        return openCGAClient.getStudyClient().get(studiesCommandOptions.infoCommandOptions.study, queryOptions);
    }

    private DataResponse<Study> update() throws CatalogException, IOException {
        logger.debug("Updating the study");

        StudyCommandOptions.UpdateCommandOptions commandOptions = studiesCommandOptions.updateCommandOptions;

        commandOptions.study = getSingleValidStudy(commandOptions.study);

        ObjectMap params;
        if (StringUtils.isNotEmpty(commandOptions.json)) {
            params = loadFile(commandOptions.json);
        } else {
            params = new ObjectMap();
            params.putIfNotEmpty(StudyDBAdaptor.QueryParams.NAME.key(), commandOptions.name);
            params.putIfNotEmpty(StudyDBAdaptor.QueryParams.TYPE.key(), commandOptions.type);
            params.putIfNotEmpty(StudyDBAdaptor.QueryParams.DESCRIPTION.key(), commandOptions.description);
            params.putIfNotEmpty(StudyDBAdaptor.QueryParams.STATS.key(), commandOptions.stats);
            params.putIfNotEmpty(StudyDBAdaptor.QueryParams.ATTRIBUTES.key(), commandOptions.attributes);
        }

        return openCGAClient.getStudyClient().update(commandOptions.study, null, params);
    }

    private DataResponse<Study> delete() throws CatalogException, IOException {
        logger.debug("Deleting a study");

        return openCGAClient.getStudyClient().delete(studiesCommandOptions.deleteCommandOptions.study, new ObjectMap());
    }

    /************************************************  Summary and help Commands  ***********************************************/

    private DataResponse<ObjectMap> stats() throws CatalogException, IOException {
        logger.debug("Study stats");

        Query query = new Query("default", studiesCommandOptions.statsCommandOptions.defaultStats);
        query.putIfNotEmpty("individualFields", studiesCommandOptions.statsCommandOptions.individualFields);
        query.putIfNotEmpty("sampleFields", studiesCommandOptions.statsCommandOptions.sampleFields);
        query.putIfNotEmpty("cohortFields", studiesCommandOptions.statsCommandOptions.cohortFields);
        query.putIfNotEmpty("familyFields", studiesCommandOptions.statsCommandOptions.familyFields);
        query.putIfNotEmpty("fileFields", studiesCommandOptions.statsCommandOptions.fileFields);

        return openCGAClient.getStudyClient().getStats(studiesCommandOptions.statsCommandOptions.study, query, QueryOptions.empty());
    }

    /************************************************  Search Commands  ***********************************************/

    private DataResponse<Study> search() throws CatalogException, IOException {
        logger.debug("Searching study");

        Query query = new Query();
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), studiesCommandOptions.searchCommandOptions.project);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.NAME.key(), studiesCommandOptions.searchCommandOptions.name);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.ID.key(), studiesCommandOptions.searchCommandOptions.alias);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.CREATION_DATE.key(), studiesCommandOptions.searchCommandOptions.creationDate);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.STATUS_NAME.key(), studiesCommandOptions.searchCommandOptions.status);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.ATTRIBUTES.key(), studiesCommandOptions.searchCommandOptions.attributes);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.NATTRIBUTES.key(), studiesCommandOptions.searchCommandOptions.nattributes);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.BATTRIBUTES.key(), studiesCommandOptions.searchCommandOptions.battributes);
        query.putAll(studiesCommandOptions.searchCommandOptions.commonOptions.params);

        if (StringUtils.isNotEmpty(studiesCommandOptions.searchCommandOptions.type)) {
            try {
                query.put(StudyDBAdaptor.QueryParams.TYPE.key(),
                        Study.Type.valueOf(studiesCommandOptions.searchCommandOptions.type.toUpperCase()));
            } catch (IllegalArgumentException e) {
                logger.warn("{} not recognized as a proper study type", studiesCommandOptions.searchCommandOptions.type);
            }
        }

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, studiesCommandOptions.searchCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, studiesCommandOptions.searchCommandOptions.dataModelOptions.exclude);
        queryOptions.put(QueryOptions.LIMIT, studiesCommandOptions.searchCommandOptions.numericOptions.limit);
        queryOptions.put(QueryOptions.SKIP, studiesCommandOptions.searchCommandOptions.numericOptions.skip);
        queryOptions.put("count", studiesCommandOptions.searchCommandOptions.numericOptions.count);

        return openCGAClient.getStudyClient().search(query, queryOptions);
    }

    private DataResponse scanFiles() throws CatalogException, IOException {
        logger.debug("Scan the study folder to find changes.\n");

        return openCGAClient.getStudyClient().scanFiles(studiesCommandOptions.scanFilesCommandOptions.study, null);
    }

    private DataResponse resyncFiles() throws CatalogException, IOException {
        logger.debug("Scan the study folder to find changes.\n");

        return openCGAClient.getStudyClient().resyncFiles(studiesCommandOptions.resyncFilesCommandOptions.study, null);
    }

    /************************************************* Groups commands *********************************************************/
    private DataResponse<ObjectMap> groups() throws CatalogException,IOException {
        logger.debug("Groups");

        studiesCommandOptions.groupsCommandOptions.study = getSingleValidStudy(studiesCommandOptions.groupsCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("name", studiesCommandOptions.groupsCommandOptions.group);

        return openCGAClient.getStudyClient().groups(studiesCommandOptions.groupsCommandOptions.study, params);
    }

    private DataResponse<ObjectMap> groupsCreate() throws CatalogException,IOException {
        logger.debug("Creating groups");

        studiesCommandOptions.groupsCreateCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.groupsCreateCommandOptions.study);

        return openCGAClient.getStudyClient().createGroup(studiesCommandOptions.groupsCreateCommandOptions.study,
                studiesCommandOptions.groupsCreateCommandOptions.groupId,
                studiesCommandOptions.groupsCreateCommandOptions.groupName, studiesCommandOptions.groupsCreateCommandOptions.users);
    }

    private DataResponse<ObjectMap> groupsDelete() throws CatalogException,IOException {
        logger.debug("Deleting groups");

        studiesCommandOptions.groupsDeleteCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.groupsDeleteCommandOptions.study);

        QueryOptions queryOptions = new QueryOptions();
        return openCGAClient.getStudyClient().deleteGroup(studiesCommandOptions.groupsDeleteCommandOptions.study,
                studiesCommandOptions.groupsDeleteCommandOptions.groupId, queryOptions);
    }

    private DataResponse<ObjectMap> groupsUpdate() throws CatalogException,IOException {
        logger.debug("Updating groups");

        studiesCommandOptions.groupsUpdateCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.groupsUpdateCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("action", studiesCommandOptions.groupsUpdateCommandOptions.action);
        params.putIfNotNull("users", studiesCommandOptions.groupsUpdateCommandOptions.users);

        return openCGAClient.getStudyClient().updateGroup(studiesCommandOptions.groupsUpdateCommandOptions.study,
                studiesCommandOptions.groupsUpdateCommandOptions.groupId, params);
    }

    private DataResponse<ObjectMap> membersUpdate() throws CatalogException,IOException {
        logger.debug("Updating users from members group");

        studiesCommandOptions.memberGroupUpdateCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.memberGroupUpdateCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("action", studiesCommandOptions.memberGroupUpdateCommandOptions.action);
        params.putIfNotNull("users", studiesCommandOptions.memberGroupUpdateCommandOptions.users);

        return openCGAClient.getStudyClient().updateGroupMember(studiesCommandOptions.memberGroupUpdateCommandOptions.study, params);
    }

    private DataResponse<ObjectMap> adminsUpdate() throws CatalogException,IOException {
        logger.debug("Updating users from admins group");

        studiesCommandOptions.adminsGroupUpdateCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.adminsGroupUpdateCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("action", studiesCommandOptions.adminsGroupUpdateCommandOptions.action);
        params.putIfNotNull("users", studiesCommandOptions.adminsGroupUpdateCommandOptions.users);

        return openCGAClient.getStudyClient().updateGroupAdmins(studiesCommandOptions.adminsGroupUpdateCommandOptions.study, params);
    }

    /************************************************* Variable set commands *********************************************************/

    private DataResponse<VariableSet> variableSets() throws CatalogException, IOException {
        logger.debug("Get variable sets");
        StudyCommandOptions.VariableSetsCommandOptions commandOptions = studiesCommandOptions.variableSetsCommandOptions;

        commandOptions.study = getSingleValidStudy(commandOptions.study);

        Query query = new Query();
        query.putIfNotEmpty("id", commandOptions.variableSet);

        return openCGAClient.getStudyClient().getVariableSets(commandOptions.study, query);
    }

    private DataResponse<VariableSet> variableSetUpdate() throws CatalogException, IOException {
        logger.debug("Update variable set");
        StudyCommandOptions.VariableSetsUpdateCommandOptions commandOptions = studiesCommandOptions.variableSetsUpdateCommandOptions;

        commandOptions.study = getSingleValidStudy(commandOptions.study);

        Query query = new Query();
        query.putIfNotNull("action", commandOptions.action);

        // Load the variable set
        ObjectMap variableSet = loadFile(commandOptions.variableSet);

        return openCGAClient.getStudyClient().updateVariableSet(commandOptions.study, query, variableSet);
    }

    private DataResponse<VariableSet> variableSetVariableUpdate() throws CatalogException, IOException {
        logger.debug("Update variable");
        StudyCommandOptions.VariablesUpdateCommandOptions commandOptions = studiesCommandOptions.variablesUpdateCommandOptions;

        commandOptions.study = getSingleValidStudy(commandOptions.study);

        Query query = new Query();
        query.putIfNotNull("action", commandOptions.action);

        // Load the variable
        ObjectMap variable = loadFile(commandOptions.variable);

        return openCGAClient.getStudyClient().updateVariableSetVariable(commandOptions.study, commandOptions.variableSet, query, variable);
    }

    /************************************************* Acl commands *********************************************************/
    private DataResponse<ObjectMap> getAcl() throws IOException, CatalogException {
        logger.debug("Get Acl");
        studiesCommandOptions.aclsCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.aclsCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("member", studiesCommandOptions.aclsCommandOptions.memberId);
        return openCGAClient.getStudyClient().getAcls(studiesCommandOptions.aclsCommandOptions.study, params);
    }

    private DataResponse<ObjectMap> updateAcl() throws IOException, CatalogException {
        StudyCommandOptions.AclsUpdateCommandOptions commandOptions = studiesCommandOptions.aclsUpdateCommandOptions;

        ObjectMap bodyParams = new ObjectMap();
        bodyParams.putIfNotNull("permissions", commandOptions.permissions);
        bodyParams.putIfNotNull("action", commandOptions.action);
        bodyParams.putIfNotNull("study", commandOptions.study);
        bodyParams.putIfNotNull("template", commandOptions.template);

        return openCGAClient.getStudyClient().updateAcl(commandOptions.memberId, new ObjectMap(), bodyParams);
    }

}
