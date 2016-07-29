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
import org.opencb.opencga.app.cli.main.options.IndividualCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogIndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.acls.permissions.IndividualAclEntry;

import java.io.IOException;

/**
 * Created by agaor on 6/06/16.
 */
public class IndividualsCommandExecutor extends OpencgaCommandExecutor {

    private IndividualCommandOptions individualsCommandOptions;
    private AclCommandExecutor<Individual, IndividualAclEntry> aclCommandExecutor;
    private AnnotationCommandExecutor<Individual, IndividualAclEntry> annotationCommandExecutor;

    public IndividualsCommandExecutor(IndividualCommandOptions individualsCommandOptions) {
        super(individualsCommandOptions.commonCommandOptions);
        this.individualsCommandOptions = individualsCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
        this.annotationCommandExecutor = new AnnotationCommandExecutor<>();
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
            case "acl":
                aclCommandExecutor.acls(individualsCommandOptions.aclsCommandOptions, openCGAClient.getIndividualClient());
                break;
            case "acl-create":
                aclCommandExecutor.aclsCreate(individualsCommandOptions.aclsCreateCommandOptions, openCGAClient.getIndividualClient());
                break;
            case "acl-member-delete":
                aclCommandExecutor.aclMemberDelete(individualsCommandOptions.aclsMemberDeleteCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "acl-member-info":
                aclCommandExecutor.aclMemberInfo(individualsCommandOptions.aclsMemberInfoCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "acl-member-update":
                aclCommandExecutor.aclMemberUpdate(individualsCommandOptions.aclsMemberUpdateCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-create":
                annotationCommandExecutor.createAnnotationSet(individualsCommandOptions.annotationCreateCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-all-info":
                annotationCommandExecutor.getAllAnnotationSets(individualsCommandOptions.annotationAllInfoCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-search":
                annotationCommandExecutor.searchAnnotationSets(individualsCommandOptions.annotationSearchCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-delete":
                annotationCommandExecutor.deleteAnnotationSet(individualsCommandOptions.annotationDeleteCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-info":
                annotationCommandExecutor.getAnnotationSet(individualsCommandOptions.annotationInfoCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-update":
                annotationCommandExecutor.updateAnnotationSet(individualsCommandOptions.annotationUpdateCommandOptions,
                        openCGAClient.getIndividualClient());
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
        if (StringUtils.isNotEmpty(individualsCommandOptions.createCommandOptions.sex)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.SEX.key(), individualsCommandOptions.createCommandOptions.sex);
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
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.sex)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.SEX.key(), individualsCommandOptions.searchCommandOptions.sex);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.ethnicity)) {
            query.put(CatalogIndividualDBAdaptor.QueryParams.ETHNICITY.key(), individualsCommandOptions.searchCommandOptions.ethnicity);
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

        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.commonOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, individualsCommandOptions.searchCommandOptions.commonOptions.skip);
        }

        if (StringUtils.isNotEmpty(individualsCommandOptions.searchCommandOptions.commonOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, individualsCommandOptions.searchCommandOptions.commonOptions.limit);
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
        if (StringUtils.isNotEmpty(individualsCommandOptions.updateCommandOptions.sex)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.SEX.key(), individualsCommandOptions.updateCommandOptions.sex);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.updateCommandOptions.ethnicity)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.ETHNICITY.key(), individualsCommandOptions.updateCommandOptions.ethnicity);
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
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.sex)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.SEX.key(), individualsCommandOptions.groupByCommandOptions.sex);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.groupByCommandOptions.ethnicity)) {
            objectMap.put(CatalogIndividualDBAdaptor.QueryParams.ETHNICITY.key(), individualsCommandOptions.groupByCommandOptions.ethnicity);
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

}
