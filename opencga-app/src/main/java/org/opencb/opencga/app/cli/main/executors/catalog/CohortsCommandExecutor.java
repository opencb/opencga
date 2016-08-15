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


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AnnotationCommandExecutor;
import org.opencb.opencga.app.cli.main.options.catalog.CohortCommandOptions;
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
                createOutput(create());
                break;
            case "info":
                createOutput(info());
                break;
            case "samples":
                createOutput(samples());
                break;
            case "update":
                createOutput(update());
                break;
            case "delete":
                createOutput(delete());
                break;
            case "stats":
                createOutput(stats());
                break;
            case "group-by":
                createOutput(groupBy());
                break;
            case "acl":
                createOutput(aclCommandExecutor.acls(cohortsCommandOptions.aclsCommandOptions, openCGAClient.getCohortClient()));
                break;
            case "acl-create":
                createOutput(aclCommandExecutor.aclsCreate(cohortsCommandOptions.aclsCreateCommandOptions,
                        openCGAClient.getCohortClient()));
                break;
            case "acl-member-delete":
                createOutput(aclCommandExecutor.aclMemberDelete(cohortsCommandOptions.aclsMemberDeleteCommandOptions,
                        openCGAClient.getCohortClient()));
                break;
            case "acl-member-info":
                createOutput(aclCommandExecutor.aclMemberInfo(cohortsCommandOptions.aclsMemberInfoCommandOptions,
                        openCGAClient.getCohortClient()));
                break;
            case "acl-member-update":
                createOutput(aclCommandExecutor.aclMemberUpdate(cohortsCommandOptions.aclsMemberUpdateCommandOptions,
                        openCGAClient.getCohortClient()));
                break;
            case "annotation-sets-create":
                createOutput(annotationCommandExecutor.createAnnotationSet(cohortsCommandOptions.annotationCreateCommandOptions,
                        openCGAClient.getCohortClient()));
                break;
            case "annotation-sets-all-info":
                createOutput(annotationCommandExecutor.getAllAnnotationSets(cohortsCommandOptions.annotationAllInfoCommandOptions,
                        openCGAClient.getCohortClient()));
                break;
            case "annotation-sets-search":
                createOutput(annotationCommandExecutor.searchAnnotationSets(cohortsCommandOptions.annotationSearchCommandOptions,
                        openCGAClient.getCohortClient()));
                break;
            case "annotation-sets-delete":
                createOutput(annotationCommandExecutor.deleteAnnotationSet(cohortsCommandOptions.annotationDeleteCommandOptions,
                        openCGAClient.getCohortClient()));
                break;
            case "annotation-sets-info":
                createOutput(annotationCommandExecutor.getAnnotationSet(cohortsCommandOptions.annotationInfoCommandOptions,
                        openCGAClient.getCohortClient()));
                break;
            case "annotation-sets-update":
                createOutput(annotationCommandExecutor.updateAnnotationSet(cohortsCommandOptions.annotationUpdateCommandOptions,
                        openCGAClient.getCohortClient()));
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

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
        o.append(CatalogCohortDBAdaptor.QueryParams.TYPE.key(),description);
        o.append(CatalogCohortDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),variableSetId);
        o.append(CatalogCohortDBAdaptor.QueryParams.DESCRIPTION.key(),sampleIds);
        o.append(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key(),sampleIds);
        o.append(CatalogCohortDBAdaptor.QueryParams.VARIABLE_NAME.key(),variable);
        return openCGAClient.getCohortClient().create(studyId, cohortName, o);
    }

    private QueryResponse<Cohort> info() throws CatalogException, IOException {
        logger.debug("Getting cohort information");
        return openCGAClient.getCohortClient().get(cohortsCommandOptions.infoCommandOptions.id, null);
    }

    private QueryResponse<Sample> samples() throws CatalogException, IOException {
        logger.debug("Listing samples belonging to a cohort");
        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(cohortsCommandOptions.samplesCommandOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, cohortsCommandOptions.samplesCommandOptions.include);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.samplesCommandOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, cohortsCommandOptions.samplesCommandOptions.exclude);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.samplesCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, cohortsCommandOptions.samplesCommandOptions.limit);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.samplesCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, cohortsCommandOptions.samplesCommandOptions.skip);
        }
        queryOptions.put("count", cohortsCommandOptions.samplesCommandOptions.count);

        return openCGAClient.getCohortClient().getSamples(cohortsCommandOptions.samplesCommandOptions.id, queryOptions);
    }

    private QueryResponse<Cohort> update() throws CatalogException, IOException {
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
        if (StringUtils.isNotEmpty(cohortsCommandOptions.statsCommandOptions.log)){
            queryOptions.put("log", cohortsCommandOptions.statsCommandOptions.log);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.statsCommandOptions.outdirId)){
            queryOptions.put(CatalogJobDBAdaptor.QueryParams.OUT_DIR_ID.key(), cohortsCommandOptions.statsCommandOptions.outdirId);
        }

        return openCGAClient.getCohortClient().getStats(cohortsCommandOptions.statsCommandOptions.id, query, queryOptions);
    }

    private QueryResponse<Cohort> groupBy() throws CatalogException, IOException {
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
        return openCGAClient.getCohortClient().groupBy(cohortsCommandOptions.groupByCommandOptions.studyId,
                cohortsCommandOptions.groupByCommandOptions.by,objectMap);
    }

}
