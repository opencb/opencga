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
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.StudyCommandOptions;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.common.CustomStatusParams;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.response.RestResponse;

import java.util.List;

/**
 * Created by imedina on 03/06/16.
 */
public class StudyCommandExecutor extends OpencgaCommandExecutor {
    // TODO: Add include/exclude/skip/... (queryOptions) to the client calls !!!!

    private StudyCommandOptions studiesCommandOptions;

    public StudyCommandExecutor(StudyCommandOptions studiesCommandOptions) {
        super(studiesCommandOptions.commonCommandOptions);
        this.studiesCommandOptions = studiesCommandOptions;
    }


    @Override
    public void execute() throws Exception {

        String subCommandString = getParsedSubCommand(studiesCommandOptions.jCommander);
        RestResponse queryResponse = null;
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
            case "stats":
                queryResponse = stats();
                break;
            case "search":
                queryResponse = search();
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
     * second it reads the projects and studies from the session file.
     * @param study parameter from the CLI
     * @return a single valid Study from the CLI, configuration or from the session file
     * @throws CatalogException when no possible single study can be chosen
     */
    private String getSingleValidStudy(String study) throws CatalogException {
        // First, check the study parameter, if is not empty we just return it, this the user's selection.
        if (StringUtils.isNotEmpty(study)) {
            return study;
        } else {
            // Third, check if there is only one single project and study for this user in the current CLI session file.
            List<String> studies = cliSession == null ? null : cliSession.getStudies();
            if (ListUtils.isNotEmpty(studies) && studies.size() == 1) {
                study = studies.get(0);
            } else {
                throw new CatalogException("None or more than one study found");
            }
        }
        return study;
    }

    /**********************************************  Administration Commands  ***********************************************/

    private RestResponse<Study> create() throws ClientException {
        logger.debug("Creating a new study");

        StudyCommandOptions.CreateCommandOptions commandOptions = studiesCommandOptions.createCommandOptions;

        StudyCreateParams createParams = new StudyCreateParams()
                .setId(commandOptions.id)
                .setAlias(commandOptions.alias)
                .setName(commandOptions.name)
                .setDescription(commandOptions.description);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("project", commandOptions.project);

        return openCGAClient.getStudyClient().create(createParams, params);
    }

    private RestResponse<Study> info() throws CatalogException, ClientException {
        logger.debug("Getting the study info");

        StudyCommandOptions.InfoCommandOptions c = studiesCommandOptions.infoCommandOptions;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, c.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, c.dataModelOptions.exclude);
        return openCGAClient.getStudyClient().info(getSingleValidStudy(c.study), queryOptions);
    }

    private RestResponse<Study> update() throws CatalogException, ClientException {
        logger.debug("Updating the study");

        StudyCommandOptions.UpdateCommandOptions c = studiesCommandOptions.updateCommandOptions;

        c.study = getSingleValidStudy(c.study);

        StudyUpdateParams updateParams = new StudyUpdateParams()
                .setName(c.name)
                .setAlias(c.alias)
                .setDescription(c.description);
        if (StringUtils.isNotEmpty(c.status)) {
            updateParams.setStatus(new CustomStatusParams(c.status, ""));
        }

        return openCGAClient.getStudyClient().update(getSingleValidStudy(c.study), updateParams);
    }

    /************************************************  Summary and help Commands  ***********************************************/

    private RestResponse<FacetField> stats() throws ClientException {
        logger.debug("Study stats");

        StudyCommandOptions.StatsCommandOptions c = studiesCommandOptions.statsCommandOptions;

        Query params = new Query("default", c.defaultStats);
        params.putIfNotEmpty("individualFields", c.individualFields);
        params.putIfNotEmpty("sampleFields", c.sampleFields);
        params.putIfNotEmpty("cohortFields", c.cohortFields);
        params.putIfNotEmpty("familyFields", c.familyFields);
        params.putIfNotEmpty("fileFields", c.fileFields);
        params.putIfNotEmpty("jobFields", c.jobFields);

        return openCGAClient.getStudyClient().aggregationStats(c.study, params);
    }

    /************************************************  Search Commands  ***********************************************/

    private RestResponse<Study> search() throws ClientException {
        logger.debug("Searching study");

        StudyCommandOptions.SearchCommandOptions c = studiesCommandOptions.searchCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.ID.key(), c.id);
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.NAME.key(), c.name);
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.ALIAS.key(), c.alias);
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.FQN.key(), c.fqn);
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.CREATION_DATE.key(), c.creationDate);
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.MODIFICATION_DATE.key(), c.modificationDate);
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), c.status);
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.ATTRIBUTES.key(), c.attributes);
        params.putIfNotNull(StudyDBAdaptor.QueryParams.RELEASE.key(), c.release);
        params.putAll(c.commonOptions.params);

        params.putIfNotEmpty(QueryOptions.INCLUDE, c.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, c.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, c.numericOptions.limit);
        params.put(QueryOptions.SKIP, c.numericOptions.skip);
        params.put("count", c.numericOptions.count);

        return openCGAClient.getStudyClient().search(c.project, params);
    }

    /************************************************* Groups commands *********************************************************/
    private RestResponse<Group> groups() throws CatalogException, ClientException {
        logger.debug("Groups");

        studiesCommandOptions.groupsCommandOptions.study = getSingleValidStudy(studiesCommandOptions.groupsCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("id", studiesCommandOptions.groupsCommandOptions.group);

        return openCGAClient.getStudyClient().groups(studiesCommandOptions.groupsCommandOptions.study, params);
    }

    private RestResponse<Group> groupsCreate() throws CatalogException, ClientException {
        logger.debug("Creating groups");

        StudyCommandOptions.GroupsCreateCommandOptions c = studiesCommandOptions.groupsCreateCommandOptions;

        GroupCreateParams createParams = new GroupCreateParams()
                .setId(c.groupId)
                .setUsers(c.users);

        ObjectMap params = new ObjectMap("action", ParamUtils.BasicUpdateAction.ADD);

        return openCGAClient.getStudyClient().updateGroups(getSingleValidStudy(c.study), createParams, params);
    }

    private RestResponse<Group> groupsDelete() throws CatalogException, ClientException {
        logger.debug("Deleting groups");
        StudyCommandOptions.GroupsDeleteCommandOptions c = studiesCommandOptions.groupsDeleteCommandOptions;

        GroupCreateParams createParams = new GroupCreateParams()
                .setId(c.groupId);
        ObjectMap params = new ObjectMap("action", ParamUtils.BasicUpdateAction.REMOVE);

        return openCGAClient.getStudyClient().updateGroups(getSingleValidStudy(c.study), createParams, params);
    }

    private RestResponse<Group> groupsUpdate() throws CatalogException, ClientException {
        logger.debug("Updating groups");

        StudyCommandOptions.GroupsUpdateCommandOptions c = studiesCommandOptions.groupsUpdateCommandOptions;

        GroupUpdateParams updateParams = new GroupUpdateParams()
                .setUsers(c.users);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("action", c.action);

        return openCGAClient.getStudyClient().updateUsers(getSingleValidStudy(c.study), c.groupId, updateParams, params);
    }

    /************************************************* Variable set commands *********************************************************/

    private RestResponse<VariableSet> variableSets() throws CatalogException, ClientException {
        logger.debug("Get variable sets");
        StudyCommandOptions.VariableSetsCommandOptions commandOptions = studiesCommandOptions.variableSetsCommandOptions;

        Query query = new Query();
        query.putIfNotEmpty("id", commandOptions.variableSet);

        return openCGAClient.getStudyClient().variableSets(getSingleValidStudy(commandOptions.study), query);
    }

    private RestResponse<VariableSet> variableSetUpdate() throws CatalogException, ClientException {
        logger.debug("Update variable set");
        StudyCommandOptions.VariableSetsUpdateCommandOptions commandOptions = studiesCommandOptions.variableSetsUpdateCommandOptions;

        VariableSetCreateParams createParams = loadFile(commandOptions.variableSet, VariableSetCreateParams.class);

        Query query = new Query();
        query.putIfNotNull("action", commandOptions.action);

        return openCGAClient.getStudyClient().updateVariableSets(getSingleValidStudy(commandOptions.study), createParams, query);
    }

    private RestResponse<VariableSet> variableSetVariableUpdate() throws CatalogException, ClientException {
        logger.debug("Update variable");
        StudyCommandOptions.VariablesUpdateCommandOptions c = studiesCommandOptions.variablesUpdateCommandOptions;

        c.study = getSingleValidStudy(c.study);

        Query params = new Query();
        params.putIfNotNull("action", c.action);

        // Load the variable
        Variable variable = loadFile(c.variable, Variable.class);

        return openCGAClient.getStudyClient().updateVariables(getSingleValidStudy(c.study), c.variableSet, variable, params);
    }

    /************************************************* Acl commands *********************************************************/
    private RestResponse<ObjectMap> getAcl() throws CatalogException, ClientException {
        logger.debug("Get Acl");

        StudyCommandOptions.AclsCommandOptions c = studiesCommandOptions.aclsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("member", c.memberId);

        return openCGAClient.getStudyClient().acl(getSingleValidStudy(c.study), params);
    }

    private RestResponse<ObjectMap> updateAcl() throws ClientException {
        StudyCommandOptions.AclsUpdateCommandOptions c = studiesCommandOptions.aclsUpdateCommandOptions;

        StudyAclUpdateParams updateParams = new StudyAclUpdateParams()
                .setStudy(c.study)
                .setPermissions(c.permissions)
                .setTemplate(c.template);

        return openCGAClient.getStudyClient().updateAcl(c.memberId, updateParams);
    }

}
