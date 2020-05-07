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


import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.CohortCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AnnotationCommandOptions;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortAclUpdateParams;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.response.RestResponse;

import java.io.File;
import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class CohortCommandExecutor extends OpencgaCommandExecutor {

    private CohortCommandOptions cohortsCommandOptions;

    public CohortCommandExecutor(CohortCommandOptions cohortsCommandOptions) {
        super(cohortsCommandOptions.commonCommandOptions);
        this.cohortsCommandOptions = cohortsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing cohorts command line");

        String subCommandString = getParsedSubCommand(cohortsCommandOptions.jCommander);
        RestResponse queryResponse = null;
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
            case "search":
                queryResponse = search();
                break;
            case "stats":
                queryResponse = stats();
                break;
            case "acl":
                queryResponse = acl();
                break;
            case "acl-update":
                queryResponse = updateAcl();
                break;
            case "annotation-sets-update":
                queryResponse = updateAnnotations();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private RestResponse<Cohort> updateAnnotations() throws ClientException, IOException {
        AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions commandOptions = cohortsCommandOptions.annotationUpdateCommandOptions;

        ObjectMapper mapper = new ObjectMapper();
        ObjectMap annotations = mapper.readValue(new File(commandOptions.annotations), ObjectMap.class);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
//        queryParams.putIfNotNull("action", updateCommandOptions.action);

        return openCGAClient.getCohortClient().updateAnnotations(commandOptions.id, commandOptions.annotationSetId, annotations, params);
    }

    private RestResponse<ObjectMap> acl() throws ClientException {
        AclCommandOptions.AclsCommandOptions commandOptions = cohortsCommandOptions.aclsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
        params.putIfNotEmpty("member", commandOptions.memberId);

        params.putAll(commandOptions.commonOptions.params);

        return openCGAClient.getCohortClient().acl(commandOptions.id, params);
    }

    private RestResponse<Cohort> search() throws ClientException {
        CohortCommandOptions.SearchCommandOptions commandOptions = cohortsCommandOptions.searchCommandOptions;

        logger.debug("Searching cohorts");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(CohortDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty(CohortDBAdaptor.QueryParams.ID.key(), commandOptions.name);
        params.putIfNotNull(CohortDBAdaptor.QueryParams.TYPE.key(), commandOptions.type);
        params.putIfNotNull(CohortDBAdaptor.QueryParams.INTERNAL_STATUS.key(), commandOptions.status);
        params.putIfNotEmpty(CohortDBAdaptor.QueryParams.SAMPLES.key(), commandOptions.samples);
        params.putAll(commandOptions.commonOptions.params);
        params.put(QueryOptions.COUNT, commandOptions.numericOptions.count);
        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, commandOptions.numericOptions.limit);
        params.put(QueryOptions.SKIP, commandOptions.numericOptions.skip);

        return openCGAClient.getCohortClient().search(params);
    }


    private RestResponse<Cohort> create() throws ClientException {
        logger.debug("Creating a new cohort");

        CohortCreateParams createParams = new CohortCreateParams()
                .setId(cohortsCommandOptions.createCommandOptions.name)
                .setType(cohortsCommandOptions.createCommandOptions.type)
                .setDescription(cohortsCommandOptions.createCommandOptions.description)
                .setSamples(cohortsCommandOptions.createCommandOptions.sampleIds);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(CohortDBAdaptor.QueryParams.STUDY.key(), cohortsCommandOptions.createCommandOptions.study);
        params.putIfNotEmpty("variableSet", cohortsCommandOptions.createCommandOptions.variableSetId);
        params.putIfNotEmpty("variable", cohortsCommandOptions.createCommandOptions.variable);

        return openCGAClient.getCohortClient().create(createParams, params);
    }

    private RestResponse<Cohort> info() throws ClientException {
        logger.debug("Getting cohort information");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(CohortDBAdaptor.QueryParams.STUDY.key(), cohortsCommandOptions.infoCommandOptions.study);
        params.putIfNotEmpty(QueryOptions.INCLUDE, cohortsCommandOptions.infoCommandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, cohortsCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        return openCGAClient.getCohortClient().info(cohortsCommandOptions.infoCommandOptions.cohort, params);
    }

    private RestResponse<Cohort> update() throws ClientException {
        logger.debug("Updating cohort");

        CohortUpdateParams updateParams = new CohortUpdateParams()
                .setId(cohortsCommandOptions.updateCommandOptions.name)
                .setDescription(cohortsCommandOptions.updateCommandOptions.description)
                .setSamples(cohortsCommandOptions.updateCommandOptions.samples);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(CohortDBAdaptor.QueryParams.STUDY.key(), cohortsCommandOptions.updateCommandOptions.study);

        return openCGAClient.getCohortClient().update(cohortsCommandOptions.updateCommandOptions.cohort, updateParams, params);
    }

    private RestResponse<Cohort> delete() throws ClientException {
        logger.debug("Deleting cohort");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(CohortDBAdaptor.QueryParams.STUDY.key(), cohortsCommandOptions.deleteCommandOptions.study);

        return openCGAClient.getCohortClient().delete(cohortsCommandOptions.deleteCommandOptions.cohort, params);
    }

    private RestResponse<FacetField> stats() throws ClientException {
        logger.debug("Cohort aggregation stats");

        CohortCommandOptions.StatsCommandOptions commandOptions = cohortsCommandOptions.statsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(CohortDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);

        params.putIfNotEmpty("creationYear", commandOptions.creationYear);
        params.putIfNotEmpty("creationMonth", commandOptions.creationMonth);
        params.putIfNotEmpty("creationDay", commandOptions.creationDay);
        params.putIfNotEmpty("creationDayOfWeek", commandOptions.creationDayOfWeek);
        params.putIfNotEmpty("type", commandOptions.type);
        params.putIfNotEmpty("status", commandOptions.status);
        params.putIfNotEmpty("numSamples", commandOptions.numSamples);
        params.putIfNotEmpty("release", commandOptions.release);
        params.putIfNotEmpty(Constants.ANNOTATION, commandOptions.annotation);

        params.put("default", commandOptions.defaultStats);
        params.putIfNotNull("field", commandOptions.field);

        return openCGAClient.getCohortClient().aggregationStats(params);
    }

    private RestResponse<ObjectMap> updateAcl() throws ClientException, CatalogException {
        AclCommandOptions.AclsUpdateCommandOptions commandOptions = cohortsCommandOptions.aclsUpdateCommandOptions;

        CohortAclUpdateParams updateParams = new CohortAclUpdateParams()
                .setCohort(extractIdsFromListOrFile(commandOptions.id))
                .setPermissions(commandOptions.permissions)
                .setAction(commandOptions.action);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("study", commandOptions.study);

        return openCGAClient.getCohortClient().updateAcl(commandOptions.memberId, updateParams, params);
    }


}
