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
import org.opencb.opencga.app.cli.main.options.IndividualCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogIndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.acls.IndividualAclEntry;
import org.opencb.opencga.client.rest.IndividualClient;

import java.io.IOException;

/**
 * Created by agaor on 6/06/16.
 */
public class IndividualsCommandExecutor extends OpencgaCommandExecutor {

    private IndividualCommandOptions individualsCommandOptions;

    public IndividualsCommandExecutor(IndividualCommandOptions individualsCommandOptions) {
        super(individualsCommandOptions.commonCommandOptions);
        this.individualsCommandOptions = individualsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing jobs command line");

        String subCommandString = getParsedSubCommand(individualsCommandOptions.jCommander);
        switch (subCommandString) {

            case "create":
                create();
                break;
            case "info":
                info();
                break;
            case "search":
                search();
                break;
            case "update":
                update();
                break;
            case "delete":
                delete();
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
        logger.debug("Creating individual");
        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(individualsCommandOptions.createCommandOptions.family)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.FAMILY.key(), individualsCommandOptions.createCommandOptions.family);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.createCommandOptions.fatherId)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.FATHER_ID.key(), individualsCommandOptions.createCommandOptions.fatherId);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.createCommandOptions.motherId)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.MOTHER_ID.key(), individualsCommandOptions.createCommandOptions.motherId);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.createCommandOptions.gender)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.GENDER.key(), individualsCommandOptions.createCommandOptions.gender);
        }

        QueryResponse<Individual> individuals = openCGAClient.getIndividualClient()
                .create(individualsCommandOptions.createCommandOptions.studyId, individualsCommandOptions.createCommandOptions.name,
                        objectMap);
        individuals.first().getResult().stream().forEach(individual -> System.out.println(individual.toString()));
    }

    private void info() throws CatalogException, IOException {
        logger.debug("Getting individual information");

        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(individualsCommandOptions.infoCommandOptions.commonOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, individualsCommandOptions.infoCommandOptions.commonOptions.include);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.infoCommandOptions.commonOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, individualsCommandOptions.infoCommandOptions.commonOptions.exclude);
        }

        QueryResponse<Individual> individuals = openCGAClient.getIndividualClient()
                .get(individualsCommandOptions.infoCommandOptions.id, queryOptions);
        System.out.println(individuals.toString());
    }

    private void search() throws CatalogException, IOException {
        logger.debug("Searching individuals");

        Query query = new Query();

        QueryOptions queryOptions = new QueryOptions();

        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.id)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.ID.key(), individualsCommandOptions.searchCommandOptions.id);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.name)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.NAME.key(), individualsCommandOptions.searchCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.fatherId)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.FATHER_ID.key(), individualsCommandOptions.searchCommandOptions.fatherId);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.motherId)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.MOTHER_ID.key(), individualsCommandOptions.searchCommandOptions.motherId);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.family)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.FAMILY.key(), individualsCommandOptions.searchCommandOptions.family);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.gender)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.GENDER.key(), individualsCommandOptions.searchCommandOptions.gender);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.race)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.RACE.key(), individualsCommandOptions.searchCommandOptions.race);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.species)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.SPECIES.key(), individualsCommandOptions.searchCommandOptions.species);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.population)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                    individualsCommandOptions.searchCommandOptions.population);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.variableSetId)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                    individualsCommandOptions.searchCommandOptions.variableSetId);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.annotation)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.ANNOTATION.key(),
                    individualsCommandOptions.searchCommandOptions.annotation);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.annotationSetName)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                    individualsCommandOptions.searchCommandOptions.annotationSetName);
        }

        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, individualsCommandOptions.searchCommandOptions.skip);
        }

        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, individualsCommandOptions.searchCommandOptions.limit);
        }

        QueryResponse<Individual> individuals = openCGAClient.getIndividualClient().search(query, queryOptions);
        individuals.first().getResult().stream().forEach(individual -> System.out.println(individual.toString()));
    }


    private void update() throws CatalogException, IOException {
        logger.debug("Updating individual information");

        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(individualsCommandOptions.updateCommandOptions.id)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.ID.key(), individualsCommandOptions.updateCommandOptions.id);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.updateCommandOptions.name)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.NAME.key(), individualsCommandOptions.updateCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.updateCommandOptions.family)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.FAMILY.key(), individualsCommandOptions.updateCommandOptions.family);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.updateCommandOptions.fatherId)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.FATHER_ID.key(), individualsCommandOptions.updateCommandOptions.fatherId);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.updateCommandOptions.motherId)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.MOTHER_ID.key(), individualsCommandOptions.updateCommandOptions.motherId);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.updateCommandOptions.gender)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.GENDER.key(), individualsCommandOptions.updateCommandOptions.gender);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.updateCommandOptions.race)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.RACE.key(), individualsCommandOptions.updateCommandOptions.race);
        }

        QueryResponse<Individual> individuals = openCGAClient.getIndividualClient()
                .update(individualsCommandOptions.updateCommandOptions.id, objectMap);
        individuals.first().getResult().stream().forEach(individual -> System.out.println(individual.toString()));
    }

    private void delete() throws CatalogException, IOException {
        logger.debug("Deleting individual information");
        ObjectMap objectMap = new ObjectMap();
        QueryResponse<Individual> individuals = openCGAClient.getIndividualClient()
                .delete(individualsCommandOptions.deleteCommandOptions.id, objectMap);
        individuals.first().getResult().stream().forEach(individual -> System.out.println(individual.toString()));
    }

    private void groupBy() throws CatalogException, IOException {
        logger.debug("Group by individuals");

        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.id)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.ID.key(), individualsCommandOptions.groupByCommandOptions.id);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.name)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.NAME.key(), individualsCommandOptions.groupByCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.fatherId)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.FATHER_ID.key(), individualsCommandOptions.groupByCommandOptions.fatherId);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.motherId)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.MOTHER_ID.key(), individualsCommandOptions.groupByCommandOptions.motherId);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.family)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.FAMILY.key(), individualsCommandOptions.groupByCommandOptions.family);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.gender)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.GENDER.key(), individualsCommandOptions.groupByCommandOptions.gender);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.race)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.RACE.key(), individualsCommandOptions.groupByCommandOptions.race);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.species)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.SPECIES.key(), individualsCommandOptions.groupByCommandOptions.species);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.population)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                    individualsCommandOptions.groupByCommandOptions.population);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.variableSetId)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                    individualsCommandOptions.groupByCommandOptions.variableSetId);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.annotation)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.ANNOTATION.key(),
                    individualsCommandOptions.groupByCommandOptions.annotation);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.annotationSetName)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                    individualsCommandOptions.groupByCommandOptions.annotationSetName);
        }

        QueryResponse<Individual> individuals = openCGAClient.getIndividualClient().groupBy(
                individualsCommandOptions.groupByCommandOptions.studyId, individualsCommandOptions.groupByCommandOptions.by, objectMap);
        individuals.first().getResult().stream().forEach(individual -> System.out.println(individual.toString()));
    }


    /********************************************  Annotation commands  ***********************************************/

    private void annotationSetsAllInfo() throws CatalogException, IOException {
        logger.debug("Searching annotationSets information");
        ObjectMap objectMap = new ObjectMap();

        objectMap.put("as-map", individualsCommandOptions.annotationSetsAllInfoCommandOptions.asMap);

        QueryResponse<Individual> individual = openCGAClient.getIndividualClient()
                .annotationSetsAllInfo(individualsCommandOptions.annotationSetsAllInfoCommandOptions.id, objectMap);

        System.out.println(individual.toString());
    }

    private void annotationSetsInfo() throws CatalogException, IOException {
        logger.debug("Searching annotationSets information");
        ObjectMap objectMap = new ObjectMap();

        objectMap.put("asMap", individualsCommandOptions.annotationSetsInfoCommandOptions.asMap);

        if (StringUtils.isNotEmpty(individualsCommandOptions.annotationSetsInfoCommandOptions.annotationSetName)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                    individualsCommandOptions.annotationSetsInfoCommandOptions.annotationSetName);
        }

        if (StringUtils.isNotEmpty(individualsCommandOptions.annotationSetsInfoCommandOptions.variableSetId)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                    individualsCommandOptions.annotationSetsInfoCommandOptions.variableSetId);
        }

        QueryResponse<Individual> individual = openCGAClient.getIndividualClient()
                .annotationSetsInfo(individualsCommandOptions.annotationSetsInfoCommandOptions.id,
                        individualsCommandOptions.annotationSetsInfoCommandOptions.annotationSetName,
                        individualsCommandOptions.annotationSetsInfoCommandOptions.variableSetId, objectMap);

        System.out.println(individual.toString());
    }

    private void annotationSetsSearch() throws CatalogException, IOException {
        logger.debug("Searching annotationSets");
        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(individualsCommandOptions.annotationSetsInfoCommandOptions.annotation)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.ANNOTATION.key(),
                    individualsCommandOptions.annotationSetsInfoCommandOptions.annotation);
        }

        objectMap.put("as-map", individualsCommandOptions.annotationSetsAllInfoCommandOptions.asMap);

        QueryResponse<Individual> samples = openCGAClient.getIndividualClient()
                .annotationSetsSearch(individualsCommandOptions.annotationSetsSearchCommandOptions.id,
                        individualsCommandOptions.annotationSetsSearchCommandOptions.annotationSetName,
                        individualsCommandOptions.annotationSetsSearchCommandOptions.variableSetId, objectMap);

        samples.first().getResult().stream().forEach(sample -> System.out.println(sample.toString()));
    }

    private void annotationSetsDelete() throws CatalogException, IOException {
        logger.debug("Searching annotationSets");
        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(individualsCommandOptions.annotationSetsDeleteCommandOptions.annotation)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.ANNOTATION.key(),
                    individualsCommandOptions.annotationSetsDeleteCommandOptions.annotation);
        }
        QueryResponse<Individual> individuals = openCGAClient.getIndividualClient()
                .annotationSetsDelete(individualsCommandOptions.annotationSetsDeleteCommandOptions.id,
                        individualsCommandOptions.annotationSetsDeleteCommandOptions.annotationSetName,
                        individualsCommandOptions.annotationSetsDeleteCommandOptions.variableSetId, objectMap);

        individuals.first().getResult().stream().forEach(individual -> System.out.println(individual.toString()));

    }

    /********************************************  Administration ACL commands  ***********************************************/

    private void acls() throws CatalogException,IOException {

        logger.debug("Acls");
        QueryResponse<IndividualAclEntry> acls = openCGAClient.getIndividualClient().getAcls(individualsCommandOptions.aclsCommandOptions.id);

        System.out.println(acls.toString());

    }
    private void aclsCreate() throws CatalogException,IOException{

        logger.debug("Creating acl");

        QueryOptions queryOptions = new QueryOptions();

        QueryResponse<IndividualAclEntry> acl =
                openCGAClient.getIndividualClient().createAcl(individualsCommandOptions.aclsCreateCommandOptions.id,
                        individualsCommandOptions.aclsCreateCommandOptions.permissions, individualsCommandOptions.aclsCreateCommandOptions.members,
                        queryOptions);
        System.out.println(acl.toString());
    }
    private void aclMemberDelete() throws CatalogException,IOException {

        logger.debug("Creating acl");

        QueryOptions queryOptions = new QueryOptions();
        QueryResponse<Object> acl = openCGAClient.getCohortClient().deleteAcl(individualsCommandOptions.aclsMemberDeleteCommandOptions.id,
                individualsCommandOptions.aclsMemberDeleteCommandOptions.memberId, queryOptions);
        System.out.println(acl.toString());
    }
    private void aclMemberInfo() throws CatalogException,IOException {

        logger.debug("Creating acl");

        QueryResponse<IndividualAclEntry> acls = openCGAClient.getIndividualClient().getAcl(individualsCommandOptions.aclsMemberInfoCommandOptions.id,
                individualsCommandOptions.aclsMemberInfoCommandOptions.memberId);
        System.out.println(acls.toString());
    }

    private void aclMemberUpdate() throws CatalogException,IOException {

        logger.debug("Updating acl");

        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(individualsCommandOptions.aclsMemberUpdateCommandOptions.addPermissions)) {
            objectMap.put(IndividualClient.AclParams.ADD_PERMISSIONS.key(), individualsCommandOptions.aclsMemberUpdateCommandOptions.addPermissions);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.aclsMemberUpdateCommandOptions.removePermissions)) {
            objectMap.put(IndividualClient.AclParams.REMOVE_PERMISSIONS.key(), individualsCommandOptions.aclsMemberUpdateCommandOptions.removePermissions);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.aclsMemberUpdateCommandOptions.setPermissions)) {
            objectMap.put(IndividualClient.AclParams.SET_PERMISSIONS.key(), individualsCommandOptions.aclsMemberUpdateCommandOptions.setPermissions);
        }

        QueryResponse<IndividualAclEntry> acl = openCGAClient.getIndividualClient().updateAcl(individualsCommandOptions.aclsMemberUpdateCommandOptions.id,
                individualsCommandOptions.aclsMemberUpdateCommandOptions.memberId, objectMap);
        System.out.println(acl.toString());
    }

}
