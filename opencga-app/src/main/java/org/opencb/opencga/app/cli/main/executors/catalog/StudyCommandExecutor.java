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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.StudyCommandOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.core.models.summaries.StudySummary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 03/06/16.
 */
public class StudyCommandExecutor extends OpencgaCommandExecutor {
    // TODO: Add include/exclude/skip/... (queryOptions) to the client calls !!!!

    private StudyCommandOptions studiesCommandOptions;
    private AclCommandExecutor<Study, StudyAclEntry> aclCommandExecutor;

    public StudyCommandExecutor(StudyCommandOptions studiesCommandOptions) {
        super(studiesCommandOptions.commonCommandOptions);
        this.studiesCommandOptions = studiesCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {

        String subCommandString = getParsedSubCommand(studiesCommandOptions.jCommander);
        QueryResponse queryResponse = null;
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
            case "summary":
                queryResponse = summary();
                break;
            case "help":
                queryResponse = help();
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
            case "files":
                queryResponse = files();
                break;
            case "samples":
                queryResponse = samples();
                break;
            case "jobs":
                queryResponse = jobs();
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
                Map<String, List<String>> projectsAndStudies = cliSession == null ? null : cliSession.getProjectsAndStudies();
                if (projectsAndStudies != null && projectsAndStudies.size() == 1) {
                    List<String> projectAliases = new ArrayList<>(projectsAndStudies.keySet());
                    // Get the study list of the only existing project
                    List<String> studyAlias = projectsAndStudies.get(projectAliases.get(0));
                    if (studyAlias.size() == 1) {
                        study = studyAlias.get(0);
                    } else {
                        throw new CatalogException("None or more than one study found");
                    }
                } else {
                    throw new CatalogException("None or more than one project found");
                }
            }
        }
        return study;
    }

    /**********************************************  Administration Commands  ***********************************************/

    private QueryResponse<Study> create() throws CatalogException, IOException {
        logger.debug("Creating a new study");

        String project = studiesCommandOptions.createCommandOptions.project;
        String name = studiesCommandOptions.createCommandOptions.name;
        String alias = studiesCommandOptions.createCommandOptions.alias;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.DESCRIPTION.key(), studiesCommandOptions.createCommandOptions.description);
        params.putIfNotNull(StudyDBAdaptor.QueryParams.TYPE.key(), Study.Type.valueOf(studiesCommandOptions.createCommandOptions.type));

        return openCGAClient.getStudyClient().create(project, name, alias, params);
    }

    private QueryResponse<Study> info() throws CatalogException, IOException {
        logger.debug("Getting the study info");

        studiesCommandOptions.infoCommandOptions.study = getSingleValidStudy(studiesCommandOptions.infoCommandOptions.study);
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, studiesCommandOptions.infoCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, studiesCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        return openCGAClient.getStudyClient().get(studiesCommandOptions.infoCommandOptions.study, queryOptions);
    }

    private QueryResponse<Study> update() throws CatalogException, IOException {
        logger.debug("Updating the study");

        studiesCommandOptions.updateCommandOptions.study = getSingleValidStudy(studiesCommandOptions.updateCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.NAME.key(), studiesCommandOptions.updateCommandOptions.name);
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.TYPE.key(), studiesCommandOptions.updateCommandOptions.type);
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.DESCRIPTION.key(), studiesCommandOptions.updateCommandOptions.description);
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.STATS.key(), studiesCommandOptions.updateCommandOptions.stats);
        params.putIfNotEmpty(StudyDBAdaptor.QueryParams.ATTRIBUTES.key(), studiesCommandOptions.updateCommandOptions.attributes);
        return openCGAClient.getStudyClient().update(studiesCommandOptions.updateCommandOptions.study, null, params);
    }

    private QueryResponse<Study> delete() throws CatalogException, IOException {
        logger.debug("Deleting a study");

        return openCGAClient.getStudyClient().delete(studiesCommandOptions.deleteCommandOptions.study, new ObjectMap());
    }

    /************************************************  Summary and help Commands  ***********************************************/

    private QueryResponse<StudySummary> summary() throws CatalogException, IOException {
        logger.debug("Doing summary with the general stats of a study");

        return openCGAClient.getStudyClient().getSummary(studiesCommandOptions.summaryCommandOptions.study, QueryOptions.empty());
    }

    private QueryResponse<Study> help() throws CatalogException, IOException {
        logger.debug("Helping");
        /*QueryOptions queryOptions = new QueryOptions();
        QueryResponse<Study> study =
                openCGAClient.getStudyClient().help(queryOptions);
        System.out.println("Help: " + study);*/
        System.out.println("PENDING");
        return null;
    }

    /************************************************  Search Commands  ***********************************************/

    private QueryResponse<Study> search() throws CatalogException, IOException {
        logger.debug("Searching study");

        Query query = new Query();
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), studiesCommandOptions.searchCommandOptions.project);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.NAME.key(), studiesCommandOptions.searchCommandOptions.name);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.ALIAS.key(), studiesCommandOptions.searchCommandOptions.alias);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.CREATION_DATE.key(), studiesCommandOptions.searchCommandOptions.creationDate);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.STATUS_NAME.key(), studiesCommandOptions.searchCommandOptions.status);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.ATTRIBUTES.key(), studiesCommandOptions.searchCommandOptions.attributes);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.NATTRIBUTES.key(), studiesCommandOptions.searchCommandOptions.nattributes);
        query.putIfNotEmpty(StudyDBAdaptor.QueryParams.BATTRIBUTES.key(), studiesCommandOptions.searchCommandOptions.battributes);
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

    private QueryResponse scanFiles() throws CatalogException, IOException {
        logger.debug("Scan the study folder to find changes.\n");

        return openCGAClient.getStudyClient().scanFiles(studiesCommandOptions.scanFilesCommandOptions.study, null);
    }

    private QueryResponse resyncFiles() throws CatalogException, IOException {
        logger.debug("Scan the study folder to find changes.\n");

        return openCGAClient.getStudyClient().resyncFiles(studiesCommandOptions.resyncFilesCommandOptions.study, null);
    }

    private QueryResponse<File> files() throws CatalogException, IOException {
        logger.debug("Listing files of a study [PENDING]");

        studiesCommandOptions.filesCommandOptions.study = getSingleValidStudy(studiesCommandOptions.filesCommandOptions.study);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.ID.key(), studiesCommandOptions.filesCommandOptions.file);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.NAME.key(), studiesCommandOptions.filesCommandOptions.name);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.PATH.key(), studiesCommandOptions.filesCommandOptions.path);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.TYPE.key(), studiesCommandOptions.filesCommandOptions.type);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.BIOFORMAT.key(), studiesCommandOptions.filesCommandOptions.bioformat);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.FORMAT.key(), studiesCommandOptions.filesCommandOptions.format);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.STATUS.key(), studiesCommandOptions.filesCommandOptions.status);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.DIRECTORY.key(), studiesCommandOptions.filesCommandOptions.directory);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.CREATION_DATE.key(), studiesCommandOptions.filesCommandOptions.creationDate);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.MODIFICATION_DATE.key(),
                studiesCommandOptions.filesCommandOptions.modificationDate);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.DESCRIPTION.key(), studiesCommandOptions.filesCommandOptions.description);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.SIZE.key(), studiesCommandOptions.filesCommandOptions.size);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), studiesCommandOptions.filesCommandOptions.sampleIds);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.JOB_ID.key(), studiesCommandOptions.filesCommandOptions.jobId);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), studiesCommandOptions.filesCommandOptions.attributes);
        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.NATTRIBUTES.key(), studiesCommandOptions.filesCommandOptions.nattributes);
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, studiesCommandOptions.filesCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, studiesCommandOptions.filesCommandOptions.dataModelOptions.exclude);
        queryOptions.put(QueryOptions.LIMIT, studiesCommandOptions.filesCommandOptions.numericOptions.limit);
        queryOptions.put(QueryOptions.SKIP, studiesCommandOptions.filesCommandOptions.numericOptions.skip);
        queryOptions.put("count", studiesCommandOptions.filesCommandOptions.numericOptions.count);

        return openCGAClient.getStudyClient().getFiles(studiesCommandOptions.filesCommandOptions.study, queryOptions);
    }

    private QueryResponse<Job> jobs() throws CatalogException, IOException {
        logger.debug("Listing jobs of a study. [PENDING]");

        studiesCommandOptions.jobsCommandOptions.study = getSingleValidStudy(studiesCommandOptions.jobsCommandOptions.study);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(JobDBAdaptor.QueryParams.NAME.key(), studiesCommandOptions.jobsCommandOptions.name);
        queryOptions.putIfNotEmpty(JobDBAdaptor.QueryParams.TOOL_NAME.key(), studiesCommandOptions.jobsCommandOptions.toolName);
        queryOptions.putIfNotEmpty(JobDBAdaptor.QueryParams.STATUS_NAME.key(), studiesCommandOptions.jobsCommandOptions.status);
        queryOptions.putIfNotEmpty(JobDBAdaptor.QueryParams.USER_ID.key(), studiesCommandOptions.jobsCommandOptions.ownerId);
        queryOptions.putIfNotEmpty(JobDBAdaptor.QueryParams.CREATION_DATE.key(), studiesCommandOptions.jobsCommandOptions.date);
        /*if (StringUtils.isNotEmpty(studiesCommandOptions.jobsCommandOptions.date)) {
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.CREATION_DATE.key(), studiesCommandOptions.jobsCommandOptions.date);
        }*/
        queryOptions.putIfNotEmpty(JobDBAdaptor.QueryParams.INPUT.key(), studiesCommandOptions.jobsCommandOptions.inputFiles);
        queryOptions.putIfNotEmpty(JobDBAdaptor.QueryParams.OUTPUT.key(), studiesCommandOptions.jobsCommandOptions.outputFiles);

        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, studiesCommandOptions.jobsCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, studiesCommandOptions.jobsCommandOptions.dataModelOptions.exclude);
        queryOptions.put(QueryOptions.LIMIT, studiesCommandOptions.jobsCommandOptions.numericOptions.limit);
        queryOptions.put(QueryOptions.SKIP, studiesCommandOptions.jobsCommandOptions.numericOptions.skip);
        queryOptions.put("count", studiesCommandOptions.jobsCommandOptions.numericOptions.count);

        return openCGAClient.getStudyClient().getJobs(studiesCommandOptions.jobsCommandOptions.study, queryOptions);
    }


    private QueryResponse<Sample> samples() throws CatalogException, IOException {
        logger.debug("Listing samples of a study. [PENDING]");

        studiesCommandOptions.samplesCommandOptions.study = getSingleValidStudy(studiesCommandOptions.samplesCommandOptions.study);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotEmpty(SampleDBAdaptor.QueryParams.NAME.key(), studiesCommandOptions.samplesCommandOptions.name);
        queryOptions.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), studiesCommandOptions.samplesCommandOptions.source);
        queryOptions.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), studiesCommandOptions.samplesCommandOptions.individual);

        queryOptions.putIfNotEmpty(SampleDBAdaptor.QueryParams.ANNOTATION.key(), studiesCommandOptions.samplesCommandOptions.annotation);

        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, studiesCommandOptions.samplesCommandOptions.dataModelOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, studiesCommandOptions.samplesCommandOptions.dataModelOptions.exclude);
        queryOptions.put(QueryOptions.LIMIT, studiesCommandOptions.samplesCommandOptions.numericOptions.limit);
        queryOptions.put(QueryOptions.SKIP, studiesCommandOptions.samplesCommandOptions.numericOptions.skip);
        queryOptions.put("count", studiesCommandOptions.samplesCommandOptions.numericOptions.count);

        return openCGAClient.getStudyClient().getSamples(studiesCommandOptions.samplesCommandOptions.study, queryOptions);
    }

    /************************************************* Groups commands *********************************************************/
    private QueryResponse<ObjectMap> groups() throws CatalogException,IOException {
        logger.debug("Groups");

        studiesCommandOptions.groupsCommandOptions.study = getSingleValidStudy(studiesCommandOptions.groupsCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("name", studiesCommandOptions.groupsCommandOptions.group);

        return openCGAClient.getStudyClient().groups(studiesCommandOptions.groupsCommandOptions.study, params);
    }

    private QueryResponse<ObjectMap> groupsCreate() throws CatalogException,IOException {
        logger.debug("Creating groups");

        studiesCommandOptions.groupsCreateCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.groupsCreateCommandOptions.study);

        return openCGAClient.getStudyClient().createGroup(studiesCommandOptions.groupsCreateCommandOptions.study,
                studiesCommandOptions.groupsCreateCommandOptions.groupId, studiesCommandOptions.groupsCreateCommandOptions.users);
    }

    private QueryResponse<ObjectMap> groupsDelete() throws CatalogException,IOException {
        logger.debug("Deleting groups");

        studiesCommandOptions.groupsDeleteCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.groupsDeleteCommandOptions.study);

        QueryOptions queryOptions = new QueryOptions();
        return openCGAClient.getStudyClient().deleteGroup(studiesCommandOptions.groupsDeleteCommandOptions.study,
                studiesCommandOptions.groupsDeleteCommandOptions.groupId, queryOptions);
    }

    private QueryResponse<ObjectMap> groupsUpdate() throws CatalogException,IOException {
        logger.debug("Updating groups");

        studiesCommandOptions.groupsUpdateCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.groupsUpdateCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("action", studiesCommandOptions.groupsUpdateCommandOptions.action);
        params.putIfNotNull("users", studiesCommandOptions.groupsUpdateCommandOptions.users);

        return openCGAClient.getStudyClient().updateGroup(studiesCommandOptions.groupsUpdateCommandOptions.study,
                studiesCommandOptions.groupsUpdateCommandOptions.groupId, params);
    }

    private QueryResponse<ObjectMap> membersUpdate() throws CatalogException,IOException {
        logger.debug("Updating users from members group");

        studiesCommandOptions.memberGroupUpdateCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.memberGroupUpdateCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("action", studiesCommandOptions.memberGroupUpdateCommandOptions.action);
        params.putIfNotNull("users", studiesCommandOptions.memberGroupUpdateCommandOptions.users);

        return openCGAClient.getStudyClient().updateGroupMember(studiesCommandOptions.memberGroupUpdateCommandOptions.study, params);
    }

    private QueryResponse<ObjectMap> adminsUpdate() throws CatalogException,IOException {
        logger.debug("Updating users from admins group");

        studiesCommandOptions.adminsGroupUpdateCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.adminsGroupUpdateCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("action", studiesCommandOptions.adminsGroupUpdateCommandOptions.action);
        params.putIfNotNull("users", studiesCommandOptions.adminsGroupUpdateCommandOptions.users);

        return openCGAClient.getStudyClient().updateGroupAdmins(studiesCommandOptions.adminsGroupUpdateCommandOptions.study, params);
    }

    /************************************************* Acl commands *********************************************************/
    private QueryResponse<StudyAclEntry> getAcl() throws IOException, CatalogException {
        logger.debug("Get Acl");
        studiesCommandOptions.aclsCommandOptions.study =
                getSingleValidStudy(studiesCommandOptions.aclsCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("member", studiesCommandOptions.aclsCommandOptions.memberId);
        return openCGAClient.getStudyClient().getAcls(studiesCommandOptions.aclsCommandOptions.study, params);
    }

    private QueryResponse<StudyAclEntry> updateAcl() throws IOException, CatalogException {
        StudyCommandOptions.AclsUpdateCommandOptions commandOptions = studiesCommandOptions.aclsUpdateCommandOptions;

        ObjectMap bodyParams = new ObjectMap();
        bodyParams.putIfNotNull("permissions", commandOptions.permissions);
        bodyParams.putIfNotNull("action", commandOptions.action);
        bodyParams.putIfNotNull("study", commandOptions.study);
        bodyParams.putIfNotNull("template", commandOptions.template);

        return openCGAClient.getStudyClient().updateAcl(commandOptions.memberId, new ObjectMap(), bodyParams);
    }

}
