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
import org.opencb.opencga.app.cli.main.options.CohortCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.acls.CohortAclEntry;
import org.opencb.opencga.client.rest.CohortClient;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class CohortsCommandExecutor extends OpencgaCommandExecutor {

    private CohortCommandOptions cohortsCommandOptions;

    public CohortsCommandExecutor(CohortCommandOptions cohortsCommandOptions) {
        super(cohortsCommandOptions.commonCommandOptions);
        this.cohortsCommandOptions = cohortsCommandOptions;
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
            case "annotation-sets-all-info":
                annotationSetsAllInfo();
                break;
            case "annotation-sets-search":
                annotationSetsSearch();
                break;
            case "annotation-sets-delete":
                annotationSetsDelete();
                break;
            case "annotation-sets-info":
                annotationSetsInfo();
                break;
            case "acl":
                acls();
                break;
            case "acl-create":
                aclsCreate();
                break;
            case "acl-member-delete":
                aclMemberDelete();
                break;
            case "acl-member-info":
                aclMemberInfo();
                break;
            case "acl-member-update":
                aclMemberUpdate();
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
        if (StringUtils.isNotEmpty(cohortsCommandOptions.samplesCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, cohortsCommandOptions.samplesCommandOptions.limit);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.samplesCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, cohortsCommandOptions.samplesCommandOptions.skip);
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

    /********************************************  Annotation commands  ***********************************************/

    private void annotationSetsAllInfo() throws CatalogException, IOException {
        logger.debug("Searching annotationSets information");
        ObjectMap objectMap = new ObjectMap();

        objectMap.put("as-map", cohortsCommandOptions.annotationSetsAllInfoCommandOptions.asMap);

        QueryResponse<Cohort> cohort = openCGAClient.getCohortClient()
                .annotationSetsAllInfo(cohortsCommandOptions.annotationSetsAllInfoCommandOptions.id, objectMap);

        System.out.println(cohort.toString());
    }

    private void annotationSetsInfo() throws CatalogException, IOException {
        logger.debug("Searching annotationSets information");
        ObjectMap objectMap = new ObjectMap();

        objectMap.put("asMap", cohortsCommandOptions.annotationSetsInfoCommandOptions.asMap);

        if (StringUtils.isNotEmpty(cohortsCommandOptions.annotationSetsInfoCommandOptions.annotationSetName)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                    cohortsCommandOptions.annotationSetsInfoCommandOptions.annotationSetName);
        }

        if (StringUtils.isNotEmpty(cohortsCommandOptions.annotationSetsInfoCommandOptions.variableSetId)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                    cohortsCommandOptions.annotationSetsInfoCommandOptions.variableSetId);
        }

        QueryResponse<Cohort> cohort = openCGAClient.getCohortClient()
                .annotationSetsInfo(cohortsCommandOptions.annotationSetsInfoCommandOptions.id,
                cohortsCommandOptions.annotationSetsInfoCommandOptions.annotationSetName,
                cohortsCommandOptions.annotationSetsInfoCommandOptions.variableSetId, objectMap);

        System.out.println(cohort.toString());
    }

    private void annotationSetsSearch() throws CatalogException, IOException {
        logger.debug("Searching annotationSets");
        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(cohortsCommandOptions.annotationSetsInfoCommandOptions.annotation)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION.key(),
                    cohortsCommandOptions.annotationSetsInfoCommandOptions.annotation);
        }

        objectMap.put("as-map", cohortsCommandOptions.annotationSetsAllInfoCommandOptions.asMap);

        QueryResponse<Cohort> cohorts = openCGAClient.getCohortClient()
                .annotationSetsSearch(cohortsCommandOptions.annotationSetsSearchCommandOptions.id,
                        cohortsCommandOptions.annotationSetsSearchCommandOptions.annotationSetName,
                        cohortsCommandOptions.annotationSetsSearchCommandOptions.variableSetId, objectMap);

        cohorts.first().getResult().stream().forEach(cohort -> System.out.println(cohort.toString()));
    }

    private void annotationSetsDelete() throws CatalogException, IOException {
        logger.debug("Searching annotationSets");
        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(cohortsCommandOptions.annotationSetsDeleteCommandOptions.annotation)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION.key(),
                    cohortsCommandOptions.annotationSetsDeleteCommandOptions.annotation);
        }
        QueryResponse<Cohort> cohorts = openCGAClient.getCohortClient()
                .annotationSetsDelete(cohortsCommandOptions.annotationSetsDeleteCommandOptions.id,
                        cohortsCommandOptions.annotationSetsDeleteCommandOptions.annotationSetName,
                        cohortsCommandOptions.annotationSetsDeleteCommandOptions.variableSetId, objectMap);

        cohorts.first().getResult().stream().forEach(cohort -> System.out.println(cohort.toString()));

    }

    /********************************************  Administration ACL commands  ***********************************************/

    private void acls() throws CatalogException,IOException {

        logger.debug("Acls");
        QueryResponse<CohortAclEntry> acls = openCGAClient.getCohortClient().getAcls(cohortsCommandOptions.aclsCommandOptions.id);

        System.out.println(acls.toString());

    }
    private void aclsCreate() throws CatalogException,IOException{

        logger.debug("Creating acl");

        QueryOptions queryOptions = new QueryOptions();

        QueryResponse<CohortAclEntry> acl =
                openCGAClient.getCohortClient().createAcl(cohortsCommandOptions.aclsCreateCommandOptions.id,
                        cohortsCommandOptions.aclsCreateCommandOptions.permissions, cohortsCommandOptions.aclsCreateCommandOptions.members,
                        queryOptions);
        System.out.println(acl.toString());
    }
    private void aclMemberDelete() throws CatalogException,IOException {

        logger.debug("Creating acl");

        QueryOptions queryOptions = new QueryOptions();
        QueryResponse<Object> acl = openCGAClient.getCohortClient().deleteAcl(cohortsCommandOptions.aclsMemberDeleteCommandOptions.id,
                cohortsCommandOptions.aclsMemberDeleteCommandOptions.memberId, queryOptions);
        System.out.println(acl.toString());
    }
    private void aclMemberInfo() throws CatalogException,IOException {

        logger.debug("Creating acl");

        QueryResponse<CohortAclEntry> acls = openCGAClient.getCohortClient().getAcl(cohortsCommandOptions.aclsMemberInfoCommandOptions.id,
                cohortsCommandOptions.aclsMemberInfoCommandOptions.memberId);
        System.out.println(acls.toString());
    }

    private void aclMemberUpdate() throws CatalogException,IOException {

        logger.debug("Updating acl");

        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(cohortsCommandOptions.aclsMemberUpdateCommandOptions.addPermissions)) {
            objectMap.put(CohortClient.AclParams.ADD_PERMISSIONS.key(), cohortsCommandOptions.aclsMemberUpdateCommandOptions.addPermissions);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.aclsMemberUpdateCommandOptions.removePermissions)) {
            objectMap.put(CohortClient.AclParams.REMOVE_PERMISSIONS.key(), cohortsCommandOptions.aclsMemberUpdateCommandOptions.removePermissions);
        }
        if (StringUtils.isNotEmpty(cohortsCommandOptions.aclsMemberUpdateCommandOptions.setPermissions)) {
            objectMap.put(CohortClient.AclParams.SET_PERMISSIONS.key(), cohortsCommandOptions.aclsMemberUpdateCommandOptions.setPermissions);
        }

        QueryResponse<CohortAclEntry> acl = openCGAClient.getCohortClient().updateAcl(cohortsCommandOptions.aclsMemberUpdateCommandOptions.id,
                cohortsCommandOptions.aclsMemberUpdateCommandOptions.memberId, objectMap);
        System.out.println(acl.toString());
    }

}
