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

package org.opencb.opencga.app.cli.main.executors;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AnnotationCommandExecutor;
import org.opencb.opencga.app.cli.main.options.CohortCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
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
        switch (subCommandString) {
            case "create":
                create();
                break;
            case "info":
                info();
                break;
            case "samples":
                samples();
                break;
            case "annotate":
                annotate();
                break;
            case "update":
                update();
                break;
            case "delete":
                delete();
                break;
            case "stats":
                stats();
                break;
            case "group-by":
                groupBy();
                break;
            case "acl":
                aclCommandExecutor.acls(cohortsCommandOptions.aclsCommandOptions, openCGAClient.getCohortClient());
                break;
            case "acl-create":
                aclCommandExecutor.aclsCreate(cohortsCommandOptions.aclsCreateCommandOptions, openCGAClient.getCohortClient());
                break;
            case "acl-member-delete":
                aclCommandExecutor.aclMemberDelete(cohortsCommandOptions.aclsMemberDeleteCommandOptions, openCGAClient.getCohortClient());
                break;
            case "acl-member-info":
                aclCommandExecutor.aclMemberInfo(cohortsCommandOptions.aclsMemberInfoCommandOptions, openCGAClient.getCohortClient());
                break;
            case "acl-member-update":
                aclCommandExecutor.aclMemberUpdate(cohortsCommandOptions.aclsMemberUpdateCommandOptions, openCGAClient.getCohortClient());
                break;
            case "annotation-sets-create":
                annotationCommandExecutor.createAnnotationSet(cohortsCommandOptions.annotationCreateCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "annotation-sets-all-info":
                annotationCommandExecutor.getAllAnnotationSets(cohortsCommandOptions.annotationAllInfoCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "annotation-sets-search":
                annotationCommandExecutor.searchAnnotationSets(cohortsCommandOptions.annotationSearchCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "annotation-sets-delete":
                annotationCommandExecutor.deleteAnnotationSet(cohortsCommandOptions.annotationDeleteCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "annotation-sets-info":
                annotationCommandExecutor.getAnnotationSet(cohortsCommandOptions.annotationInfoCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            case "annotation-sets-update":
                annotationCommandExecutor.updateAnnotationSet(cohortsCommandOptions.annotationUpdateCommandOptions,
                        openCGAClient.getCohortClient());
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }


    private void create() throws CatalogException, IOException {
        logger.debug("Creating a new cohort");
        String studyId = cohortsCommandOptions.createCommandOptions.studyId;
        String cohortName = cohortsCommandOptions.createCommandOptions.name;
        String description = cohortsCommandOptions.createCommandOptions.description;
        String variableSetId = cohortsCommandOptions.createCommandOptions.variableSetId;
        String sampleIds = cohortsCommandOptions.createCommandOptions.sampleIds;
        String variable = cohortsCommandOptions.createCommandOptions.variable;

        ObjectMap o = new ObjectMap();
        o.append(CatalogCohortDBAdaptor.QueryParams.TYPE.key(),description);
        o.append(CatalogCohortDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),variableSetId);
        o.append(CatalogCohortDBAdaptor.QueryParams.DESCRIPTION.key(),sampleIds);
        o.append(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key(),sampleIds);
        o.append(CatalogCohortDBAdaptor.QueryParams.VARIABLE_NAME.key(),variable);
        openCGAClient.getCohortClient().create(studyId, cohortName, o);
        logger.debug("Done");
    }

    private void info() throws CatalogException, IOException {
        logger.debug("Getting cohort information");
        QueryResponse<Cohort> info =
                openCGAClient.getCohortClient().get(cohortsCommandOptions.infoCommandOptions.id, null);
        System.out.println("Cohorts = " + info);

    }

    private void samples() throws CatalogException, IOException {
        logger.debug("Listing samples belonging to a cohort");
        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(cohortsCommandOptions.samplesCommandOptions.commonOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, cohortsCommandOptions.samplesCommandOptions.commonOptions.include);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.samplesCommandOptions.commonOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, cohortsCommandOptions.samplesCommandOptions.commonOptions.exclude);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.samplesCommandOptions.commonOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, cohortsCommandOptions.samplesCommandOptions.commonOptions.limit);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.samplesCommandOptions.commonOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, cohortsCommandOptions.samplesCommandOptions.commonOptions.skip);
        }
        queryOptions.put("count", cohortsCommandOptions.samplesCommandOptions.count);

        QueryResponse<Sample> samples = openCGAClient.getCohortClient().getSamples(cohortsCommandOptions.samplesCommandOptions.id,
                queryOptions);
        samples.first().getResult().stream().forEach(sample -> System.out.println(sample.toString()));
    }

    private void annotate() throws CatalogException, IOException {
        logger.debug("Annotating cohort");
    }

    private void update() throws CatalogException, IOException {
        logger.debug("Updating cohort");

        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(cohortsCommandOptions.updateCommandOptions.name)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.NAME.key(), cohortsCommandOptions.updateCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.updateCommandOptions.creationDate)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.CREATION_DATE.key(), cohortsCommandOptions.updateCommandOptions.creationDate);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.updateCommandOptions.description)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.DESCRIPTION.key(), cohortsCommandOptions.updateCommandOptions.description);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.updateCommandOptions.samples)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key(), cohortsCommandOptions.updateCommandOptions.samples);
        }

        QueryResponse<Cohort> cohort = openCGAClient.getCohortClient()
                .update(cohortsCommandOptions.updateCommandOptions.id, objectMap);
        System.out.println("Cohort: " + cohort);
    }

    private void delete() throws CatalogException, IOException {
        logger.debug("Deleting cohort");
        ObjectMap objectMap = new ObjectMap();
        QueryResponse<Cohort> cohort = openCGAClient.getCohortClient()
                .delete(cohortsCommandOptions.deleteCommandOptions.id, objectMap);
        System.out.println("Cohort: " + cohort);
    }

    private void stats() throws CatalogException, IOException {
        logger.debug("Calculating variant stats for a set of cohorts");
        QueryOptions queryOptions = new QueryOptions();
        Query query = new Query();
        queryOptions.put("calculate", cohortsCommandOptions.statsCommandOptions.calculate);
        queryOptions.put("delete", cohortsCommandOptions.statsCommandOptions.delete);
        if (StringUtils.isNotEmpty(cohortsCommandOptions.statsCommandOptions.log)){
            queryOptions.put("log", cohortsCommandOptions.statsCommandOptions.log);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.statsCommandOptions.outdirId)){
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.OUT_DIR_ID.key(), cohortsCommandOptions.statsCommandOptions.outdirId);
        }

        QueryResponse<Object> stats = openCGAClient.getCohortClient().getStats(cohortsCommandOptions.statsCommandOptions.id, query,
                queryOptions);
        System.out.println("Stats: " + stats.toString());

    }

    private void groupBy() throws CatalogException, IOException {
        logger.debug("Group by cohorts");

        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(cohortsCommandOptions.groupByCommandOptions.id)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.ID.key(), cohortsCommandOptions.groupByCommandOptions.id);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.groupByCommandOptions.name)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.NAME.key(), cohortsCommandOptions.groupByCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.groupByCommandOptions.type)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.TYPE.key(), cohortsCommandOptions.groupByCommandOptions.type);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.groupByCommandOptions.status)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.STATUS_NAME.key(), cohortsCommandOptions.groupByCommandOptions.status);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.groupByCommandOptions.creationDate)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.CREATION_DATE.key(), cohortsCommandOptions.groupByCommandOptions.creationDate);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.groupByCommandOptions.sampleIds)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key(), cohortsCommandOptions.groupByCommandOptions.sampleIds);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.groupByCommandOptions.attributes)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.ATTRIBUTES.key(), cohortsCommandOptions.groupByCommandOptions.attributes);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.groupByCommandOptions.nattributes)) {
            objectMap.put(CatalogCohortDBAdaptor.QueryParams.NATTRIBUTES.key(), cohortsCommandOptions.groupByCommandOptions.nattributes);
        }
        QueryResponse<Cohort> cohorts = openCGAClient.getCohortClient().groupBy(cohortsCommandOptions.groupByCommandOptions.studyId,
                cohortsCommandOptions.groupByCommandOptions.by,objectMap);
        cohorts.first().getResult().stream().forEach(cohort -> System.out.println(cohort.toString()));
    }

}
