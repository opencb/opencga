/*
 * Copyright 2015 OpenCB
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


import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AnnotationCommandExecutor;
import org.opencb.opencga.app.cli.main.options.catalog.CohortCommandOptions;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.acls.permissions.CohortAclEntry;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class CohortsCommandExecutor extends OpencgaCommandExecutor {

    private CohortCommandOptions cohortsCommandOptions;
    private AclCommandExecutor<Cohort, CohortAclEntry> aclCommandExecutor;
    private AnnotationCommandExecutor<Cohort, CohortAclEntry> annotationCommandExecutor;

    public CohortsCommandExecutor(CohortCommandOptions cohortsCommandOptions) {
        super(cohortsCommandOptions.commonCommandOptions);
        this.cohortsCommandOptions = cohortsCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
        this.annotationCommandExecutor = new AnnotationCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing cohorts command line");

        String subCommandString = getParsedSubCommand(cohortsCommandOptions.jCommander);
        QueryResponse queryResponse = null;
        switch (subCommandString) {
            case "create":
                queryResponse = create();
                break;
            case "info":
                queryResponse = info();
                break;
            case "samples":
                queryResponse = samples();
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
            case "group-by":
                queryResponse = groupBy();
                break;
            case "acl":
                queryResponse = aclCommandExecutor.acls(cohortsCommandOptions.aclsCommandOptions, openCGAClient.getCohortClient());
                break;
            case "acl-create":
                queryResponse = aclCommandExecutor.aclsCreate(cohortsCommandOptions.aclsCreateCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "acl-member-delete":
                queryResponse = aclCommandExecutor.aclMemberDelete(cohortsCommandOptions.aclsMemberDeleteCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "acl-member-info":
                queryResponse = aclCommandExecutor.aclMemberInfo(cohortsCommandOptions.aclsMemberInfoCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "acl-member-update":
                queryResponse = aclCommandExecutor.aclMemberUpdate(cohortsCommandOptions.aclsMemberUpdateCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "annotation-sets-create":
                queryResponse = annotationCommandExecutor.createAnnotationSet(cohortsCommandOptions.annotationCreateCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "annotation-sets-all-info":
                queryResponse = annotationCommandExecutor.getAllAnnotationSets(cohortsCommandOptions.annotationAllInfoCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "annotation-sets-search":
                queryResponse = annotationCommandExecutor.searchAnnotationSets(cohortsCommandOptions.annotationSearchCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "annotation-sets-delete":
                queryResponse = annotationCommandExecutor.deleteAnnotationSet(cohortsCommandOptions.annotationDeleteCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "annotation-sets-info":
                queryResponse = annotationCommandExecutor.getAnnotationSet(cohortsCommandOptions.annotationInfoCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "annotation-sets-update":
                queryResponse = annotationCommandExecutor.updateAnnotationSet(cohortsCommandOptions.annotationUpdateCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);

    }


    private QueryResponse<Cohort> create() throws CatalogException, IOException {
        logger.debug("Creating a new cohort");
        String studyId = cohortsCommandOptions.createCommandOptions.studyId;
        String cohortName = cohortsCommandOptions.createCommandOptions.name;
        String description = cohortsCommandOptions.createCommandOptions.description;
        String variableSetId = cohortsCommandOptions.createCommandOptions.variableSetId;
        String sampleIds = cohortsCommandOptions.createCommandOptions.sampleIds;
        String variable = cohortsCommandOptions.createCommandOptions.variable;

        ObjectMap o = new ObjectMap();
        o.append(CohortDBAdaptor.QueryParams.TYPE.key(), cohortsCommandOptions.createCommandOptions.type);
        o.append(CohortDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),variableSetId);
        o.append(CohortDBAdaptor.QueryParams.DESCRIPTION.key(),description);
        o.append("sampleIds", sampleIds);
        o.append("variable", variable);
        if (cohortName == null){
            if( sampleIds != null) {
                System.out.println("Error: The name parameter is required when you create the cohort from samples");
                return null;
            }else if (variableSetId != null && variable != null){
                cohortName = "Cohort";
            }else{
                System.out.println("Error: Please, Insert the corrects params for create the cohort.");
                return null;
            }
        }
        return openCGAClient.getCohortClient().create(studyId, cohortName, o);
    }

    private QueryResponse<Cohort> info() throws CatalogException, IOException {
        logger.debug("Getting cohort information");
        QueryOptions queryOptions = new QueryOptions();

        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, cohortsCommandOptions.infoCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, cohortsCommandOptions.infoCommandOptions.exclude);

        return openCGAClient.getCohortClient().get(cohortsCommandOptions.infoCommandOptions.id, queryOptions);
    }

    private QueryResponse<Sample> samples() throws CatalogException, IOException {
        logger.debug("Listing samples belonging to a cohort");
        QueryOptions queryOptions = new QueryOptions();

        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, cohortsCommandOptions.samplesCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, cohortsCommandOptions.samplesCommandOptions.exclude);
        queryOptions.putIfNotEmpty(QueryOptions.LIMIT, cohortsCommandOptions.samplesCommandOptions.limit);
        queryOptions.putIfNotEmpty(QueryOptions.SKIP, cohortsCommandOptions.samplesCommandOptions.skip);
        queryOptions.put("count", cohortsCommandOptions.samplesCommandOptions.count);
        return openCGAClient.getCohortClient().getSamples(cohortsCommandOptions.samplesCommandOptions.id, queryOptions);
    }

    private QueryResponse<Cohort> update() throws CatalogException, IOException {
        logger.debug("Updating cohort");

        ObjectMap objectMap = new ObjectMap();

        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.NAME.key(), cohortsCommandOptions.updateCommandOptions.name);
        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.CREATION_DATE.key(), cohortsCommandOptions.updateCommandOptions.creationDate);
        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.DESCRIPTION.key(), cohortsCommandOptions.updateCommandOptions.description);
        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.SAMPLES.key(), cohortsCommandOptions.updateCommandOptions.samples);

        //TODO objectMap.put("method", "POST");

        return openCGAClient.getCohortClient().update(cohortsCommandOptions.updateCommandOptions.id, objectMap);
    }

    private QueryResponse<Cohort> delete() throws CatalogException, IOException {
        logger.debug("Deleting cohort");
        ObjectMap objectMap = new ObjectMap();
        return openCGAClient.getCohortClient().delete(cohortsCommandOptions.deleteCommandOptions.id, objectMap);
    }

    private QueryResponse<Object> stats() throws CatalogException, IOException {
        logger.debug("Calculating variant stats for a set of cohorts");
        QueryOptions queryOptions = new QueryOptions();
        Query query = new Query();
        queryOptions.put("calculate", cohortsCommandOptions.statsCommandOptions.calculate);
        queryOptions.put("delete", cohortsCommandOptions.statsCommandOptions.delete);
        queryOptions.putIfNotEmpty("log", cohortsCommandOptions.statsCommandOptions.log);
        queryOptions.putIfNotEmpty(JobDBAdaptor.QueryParams.OUT_DIR_ID.key(), cohortsCommandOptions.statsCommandOptions.outdirId);


        return openCGAClient.getCohortClient().getStats(cohortsCommandOptions.statsCommandOptions.id, query, queryOptions);
    }

    private QueryResponse<Cohort> groupBy() throws CatalogException, IOException {
        logger.debug("Group by cohorts");

        ObjectMap objectMap = new ObjectMap();

        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.ID.key(), cohortsCommandOptions.groupByCommandOptions.id);
        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.NAME.key(), cohortsCommandOptions.groupByCommandOptions.name);
        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.TYPE.key(), cohortsCommandOptions.groupByCommandOptions.type);
        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), cohortsCommandOptions.groupByCommandOptions.status);
        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.CREATION_DATE.key(), cohortsCommandOptions.groupByCommandOptions.creationDate);
        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.SAMPLES.key(), cohortsCommandOptions.groupByCommandOptions.sampleIds);
        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.ATTRIBUTES.key(), cohortsCommandOptions.groupByCommandOptions.attributes);
        objectMap.putIfNotEmpty(CohortDBAdaptor.QueryParams.NATTRIBUTES.key(), cohortsCommandOptions.groupByCommandOptions.nattributes);

        return openCGAClient.getCohortClient().groupBy(cohortsCommandOptions.groupByCommandOptions.studyId,
                cohortsCommandOptions.groupByCommandOptions.fields,objectMap);
    }

}
