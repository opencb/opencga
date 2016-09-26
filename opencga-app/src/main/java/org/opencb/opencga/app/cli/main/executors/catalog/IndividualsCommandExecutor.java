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
import org.opencb.opencga.app.cli.main.options.catalog.IndividualCommandOptions;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
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
        logger.debug("Executing individuals command line");

        String subCommandString = getParsedSubCommand(individualsCommandOptions.jCommander);
        QueryResponse queryResponse = null;
        switch (subCommandString) {
            case "create":
                queryResponse = create();
                break;
            case "info":
                queryResponse = info();
                break;
            case "search":
                queryResponse = search();
                break;
            case "update":
                queryResponse = update();
                break;
            case "delete":
                queryResponse = delete();
                break;
            case "group-by":
                queryResponse = groupBy();
                break;
            case "acl":
                queryResponse = aclCommandExecutor.acls(individualsCommandOptions.aclsCommandOptions, openCGAClient.getIndividualClient());
                break;
            case "acl-create":
                queryResponse = aclCommandExecutor.aclsCreate(individualsCommandOptions.aclsCreateCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "acl-member-delete":
                queryResponse = aclCommandExecutor.aclMemberDelete(individualsCommandOptions.aclsMemberDeleteCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "acl-member-info":
                queryResponse = aclCommandExecutor.aclMemberInfo(individualsCommandOptions.aclsMemberInfoCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "acl-member-update":
                queryResponse = aclCommandExecutor.aclMemberUpdate(individualsCommandOptions.aclsMemberUpdateCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-create":
                queryResponse = annotationCommandExecutor.createAnnotationSet(individualsCommandOptions.annotationCreateCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-all-info":
                queryResponse = annotationCommandExecutor.getAllAnnotationSets(individualsCommandOptions.annotationAllInfoCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-search":
                queryResponse = annotationCommandExecutor.searchAnnotationSets(individualsCommandOptions.annotationSearchCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-delete":
                queryResponse = annotationCommandExecutor.deleteAnnotationSet(individualsCommandOptions.annotationDeleteCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-info":
                queryResponse = annotationCommandExecutor.getAnnotationSet(individualsCommandOptions.annotationInfoCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            case "annotation-sets-update":
                queryResponse = annotationCommandExecutor.updateAnnotationSet(individualsCommandOptions.annotationUpdateCommandOptions,
                        openCGAClient.getIndividualClient());
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private QueryResponse<Individual> create() throws CatalogException, IOException {
        logger.debug("Creating individual");
        ObjectMap objectMap = new ObjectMap();

        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FAMILY.key(), individualsCommandOptions.createCommandOptions.family);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FATHER_ID.key(), individualsCommandOptions.createCommandOptions.fatherId);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.MOTHER_ID.key(), individualsCommandOptions.createCommandOptions.motherId);
        String sex = individualsCommandOptions.createCommandOptions.sex;
        if (individualsCommandOptions.createCommandOptions.sex != null) {
            try {
                objectMap.put(IndividualDBAdaptor.QueryParams.SEX.key(), Individual.Sex.valueOf(sex));
            } catch (IllegalArgumentException e) {
                logger.error("{} not recognized as a proper individual sex", sex);
                return null;
            }
        }

        return openCGAClient.getIndividualClient().create(individualsCommandOptions.createCommandOptions.studyId,
                individualsCommandOptions.createCommandOptions.name, objectMap);
    }

    private QueryResponse<Individual> info() throws CatalogException, IOException {
        logger.debug("Getting individual information");

        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(individualsCommandOptions.infoCommandOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, individualsCommandOptions.infoCommandOptions.include);
        }
        if (StringUtils.isNotEmpty(individualsCommandOptions.infoCommandOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, individualsCommandOptions.infoCommandOptions.exclude);
        }

        return openCGAClient.getIndividualClient().get(individualsCommandOptions.infoCommandOptions.id, queryOptions);
    }

    private QueryResponse<Individual> search() throws CatalogException, IOException {
        logger.debug("Searching individuals");

        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();

        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ID.key(), individualsCommandOptions.searchCommandOptions.id);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), individualsCommandOptions.searchCommandOptions.studyId);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.NAME.key(), individualsCommandOptions.searchCommandOptions.name);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FATHER_ID.key(), individualsCommandOptions.searchCommandOptions.fatherId);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.MOTHER_ID.key(), individualsCommandOptions.searchCommandOptions.motherId);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FAMILY.key(), individualsCommandOptions.searchCommandOptions.family);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.SEX.key(), individualsCommandOptions.searchCommandOptions.sex);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ETHNICITY.key(), individualsCommandOptions.searchCommandOptions.ethnicity);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.SPECIES.key(), individualsCommandOptions.searchCommandOptions.species);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                individualsCommandOptions.searchCommandOptions.population);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                individualsCommandOptions.searchCommandOptions.variableSetId);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ANNOTATION.key(),
                individualsCommandOptions.searchCommandOptions.annotation);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                individualsCommandOptions.searchCommandOptions.annotationSetName);
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, individualsCommandOptions.searchCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, individualsCommandOptions.searchCommandOptions.exclude);
        queryOptions.putIfNotEmpty(QueryOptions.SKIP, individualsCommandOptions.searchCommandOptions.skip);
        queryOptions.putIfNotEmpty(QueryOptions.LIMIT, individualsCommandOptions.searchCommandOptions.limit);
        queryOptions.put("count", individualsCommandOptions.searchCommandOptions.count);

        return openCGAClient.getIndividualClient().search(query, queryOptions);
    }


    private QueryResponse<Individual> update() throws CatalogException, IOException {
        logger.debug("Updating individual information");

        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.NAME.key(), individualsCommandOptions.updateCommandOptions.name);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FAMILY.key(), individualsCommandOptions.updateCommandOptions.family);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FATHER_ID.key(), individualsCommandOptions.updateCommandOptions.fatherId);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.MOTHER_ID.key(), individualsCommandOptions.updateCommandOptions.motherId);

        String sex = individualsCommandOptions.updateCommandOptions.sex;
        if (individualsCommandOptions.updateCommandOptions.sex != null) {
            try {
                objectMap.put(IndividualDBAdaptor.QueryParams.SEX.key(), Individual.Sex.valueOf(sex));
            } catch (IllegalArgumentException e) {
                logger.error("{} not recognized as a proper individual sex", sex);
                return null;
            }
        }

        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ETHNICITY.key(), individualsCommandOptions.updateCommandOptions.ethnicity);

        return openCGAClient.getIndividualClient().update(individualsCommandOptions.updateCommandOptions.id, objectMap);
    }

    private QueryResponse<Individual> delete() throws CatalogException, IOException {
        logger.debug("Deleting individual information");
        ObjectMap objectMap = new ObjectMap();
        return openCGAClient.getIndividualClient().delete(individualsCommandOptions.deleteCommandOptions.id, objectMap);
    }

    private QueryResponse<ObjectMap> groupBy() throws CatalogException, IOException {
        logger.debug("Group by individuals");

        ObjectMap objectMap = new ObjectMap();

        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ID.key(), individualsCommandOptions.groupByCommandOptions.id);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.NAME.key(), individualsCommandOptions.groupByCommandOptions.name);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FATHER_ID.key(), individualsCommandOptions.groupByCommandOptions.fatherId);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.MOTHER_ID.key(), individualsCommandOptions.groupByCommandOptions.motherId);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FAMILY.key(), individualsCommandOptions.groupByCommandOptions.family);
        objectMap.put(IndividualDBAdaptor.QueryParams.SEX.key(), individualsCommandOptions.groupByCommandOptions.sex);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ETHNICITY.key(), individualsCommandOptions.groupByCommandOptions.ethnicity);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.SPECIES.key(), individualsCommandOptions.groupByCommandOptions.species);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                individualsCommandOptions.groupByCommandOptions.population);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                individualsCommandOptions.groupByCommandOptions.variableSetId);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ANNOTATION.key(),
                individualsCommandOptions.groupByCommandOptions.annotation);
        objectMap.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                    individualsCommandOptions.groupByCommandOptions.annotationSetName);

        return openCGAClient.getIndividualClient().groupBy(
                individualsCommandOptions.groupByCommandOptions.studyId, individualsCommandOptions.groupByCommandOptions.fields, objectMap);
    }

}
