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
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AnnotationCommandExecutor;
import org.opencb.opencga.app.cli.main.options.IndividualCommandOptions;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.acls.permissions.IndividualAclEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by agaor on 6/06/16.
 */
public class IndividualCommandExecutor extends OpencgaCommandExecutor {

    private IndividualCommandOptions individualsCommandOptions;
    private AclCommandExecutor<Individual, IndividualAclEntry> aclCommandExecutor;
    private AnnotationCommandExecutor<Individual, IndividualAclEntry> annotationCommandExecutor;

    public IndividualCommandExecutor(IndividualCommandOptions individualsCommandOptions) {
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
            case "stats":
                queryResponse = stats();
                break;
            case "samples":
                queryResponse = getSamples();
                break;
            case "acl":
                queryResponse = aclCommandExecutor.acls(individualsCommandOptions.aclsCommandOptions, openCGAClient.getIndividualClient());
                break;
            case "acl-update":
                queryResponse = updateAcl();
                break;
            case "annotation-sets-create":
                queryResponse = annotationCommandExecutor.createAnnotationSet(individualsCommandOptions.annotationCreateCommandOptions,
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
            case "annotation-sets":
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

        IndividualCommandOptions.CreateCommandOptions commandOptions = individualsCommandOptions.createCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ID.key(), commandOptions.id);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.NAME.key(), commandOptions.name);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FATHER.key(), commandOptions.fatherId);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.MOTHER.key(), commandOptions.motherId);
        String sex = commandOptions.sex;
        if (commandOptions.sex != null) {
            try {
                params.put(IndividualDBAdaptor.QueryParams.SEX.key(), IndividualProperty.Sex.valueOf(sex));
            } catch (IllegalArgumentException e) {
                logger.error("{} not recognized as a proper individual sex", sex);
                return null;
            }
        }

        params.putIfNotNull(IndividualDBAdaptor.QueryParams.PARENTAL_CONSANGUINITY.key(), commandOptions.parentalConsanguinity);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ETHNICITY.key(), commandOptions.ethnicity);

        Individual.Population population = new Individual.Population();
        if (commandOptions.populationName != null) {
            population.setName(commandOptions.populationName);
        }
        if (commandOptions.populationSubpopulation != null) {
            population.setSubpopulation(commandOptions.populationSubpopulation);
        }
        if (commandOptions.populationDescription != null) {
            population.setDescription(commandOptions.populationDescription);
        }
        params.put("population", population);

        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(), commandOptions.karyotypicSex);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(), commandOptions.lifeStatus);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.AFFECTATION_STATUS.key(), commandOptions.affectationStatus);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.DATE_OF_BIRTH.key(), commandOptions.dateOfBirth);

        if (StringUtils.isNotEmpty(commandOptions.samples)) {
            String[] samples = StringUtils.split(commandOptions.samples, ",");
            List<Map<String, String>> sampleList = new ArrayList<>(samples.length);
            for (String sample : samples) {
                Map<String, String> map = new HashMap<>();
                map.put("id", sample);
                sampleList.add(map);
            }
            params.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), sampleList);
        }

        return openCGAClient.getIndividualClient().create(resolveStudy(commandOptions.study), params);
    }

    private QueryResponse<Individual> info() throws CatalogException, IOException {
        logger.debug("Getting individual information");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.STUDY.key(), resolveStudy(individualsCommandOptions.infoCommandOptions.study));
        params.putIfNotNull(QueryOptions.INCLUDE, individualsCommandOptions.infoCommandOptions.dataModelOptions.include);
        params.putIfNotNull(QueryOptions.EXCLUDE, individualsCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        params.put("flattenAnnotations", individualsCommandOptions.searchCommandOptions.flattenAnnotations);
        return openCGAClient.getIndividualClient().get(individualsCommandOptions.infoCommandOptions.individual, params);
    }

    private QueryResponse<Individual> search() throws CatalogException, IOException {
        logger.debug("Searching individuals");

        Query query = new Query();
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.STUDY.key(),
                resolveStudy(individualsCommandOptions.searchCommandOptions.study));
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ID.key(), individualsCommandOptions.searchCommandOptions.name);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FATHER.key(), individualsCommandOptions.searchCommandOptions.fatherId);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.MOTHER.key(), individualsCommandOptions.searchCommandOptions.motherId);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.SEX.key(), individualsCommandOptions.searchCommandOptions.sex);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ETHNICITY.key(), individualsCommandOptions.searchCommandOptions.ethnicity);
        // TODO: Remove these 2 deprecated parameters in future release
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                individualsCommandOptions.searchCommandOptions.population);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                individualsCommandOptions.searchCommandOptions.populationName);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_SUBPOPULATION.key(),
                individualsCommandOptions.searchCommandOptions.populationSubpopulation);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_DESCRIPTION.key(),
                individualsCommandOptions.searchCommandOptions.populationDescription);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(),
                individualsCommandOptions.searchCommandOptions.karyotypicSex);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(), individualsCommandOptions.searchCommandOptions.lifeStatus);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.AFFECTATION_STATUS.key(),
                individualsCommandOptions.searchCommandOptions.affectationStatus);
        query.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), individualsCommandOptions.searchCommandOptions.annotation);
        query.put("flattenAnnotations", individualsCommandOptions.searchCommandOptions.flattenAnnotations);
        query.putAll(individualsCommandOptions.searchCommandOptions.commonOptions.params);

        if (individualsCommandOptions.searchCommandOptions.numericOptions.count) {
            return openCGAClient.getIndividualClient().count(query);
        } else {
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, individualsCommandOptions.searchCommandOptions.dataModelOptions.include);
            queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, individualsCommandOptions.searchCommandOptions.dataModelOptions.exclude);
            queryOptions.put(QueryOptions.SKIP, individualsCommandOptions.searchCommandOptions.numericOptions.skip);
            queryOptions.put(QueryOptions.LIMIT, individualsCommandOptions.searchCommandOptions.numericOptions.limit);

            return openCGAClient.getIndividualClient().search(query, queryOptions);
        }
    }


    private QueryResponse<Individual> update() throws CatalogException, IOException {
        logger.debug("Updating individual information");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ID.key(), individualsCommandOptions.updateCommandOptions.name);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FATHER.key(), individualsCommandOptions.updateCommandOptions.fatherId);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.MOTHER.key(), individualsCommandOptions.updateCommandOptions.motherId);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.DATE_OF_BIRTH.key(),
                individualsCommandOptions.updateCommandOptions.dateOfBirth);

        String sex = individualsCommandOptions.updateCommandOptions.sex;
        if (individualsCommandOptions.updateCommandOptions.sex != null) {
            try {
                params.put(IndividualDBAdaptor.QueryParams.SEX.key(), IndividualProperty.Sex.valueOf(sex));
            } catch (IllegalArgumentException e) {
                logger.error("{} not recognized as a proper individual sex", sex);
                return null;
            }
        }

        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ETHNICITY.key(), individualsCommandOptions.updateCommandOptions.ethnicity);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                individualsCommandOptions.updateCommandOptions.populationName);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_DESCRIPTION.key(),
                individualsCommandOptions.updateCommandOptions.populationDescription);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_SUBPOPULATION.key(),
                individualsCommandOptions.updateCommandOptions.populationSubpopulation);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(),
                individualsCommandOptions.updateCommandOptions.karyotypicSex);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(),
                individualsCommandOptions.updateCommandOptions.lifeStatus);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.AFFECTATION_STATUS.key(),
                individualsCommandOptions.updateCommandOptions.affectationStatus);
        params.putIfNotNull("annotationSetsAction", individualsCommandOptions.updateCommandOptions.annotationSetsAction);

        return openCGAClient.getIndividualClient().update(individualsCommandOptions.updateCommandOptions.individual,
                resolveStudy(individualsCommandOptions.updateCommandOptions.study), params);
    }

    private QueryResponse<Individual> delete() throws CatalogException, IOException {
        logger.debug("Deleting individual information");
        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.STUDY.key(), resolveStudy(individualsCommandOptions.deleteCommandOptions.study));
        return openCGAClient.getIndividualClient().delete(individualsCommandOptions.deleteCommandOptions.individual, params);
    }

    private QueryResponse<ObjectMap> groupBy() throws CatalogException, IOException {
        logger.debug("Group by individuals");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.STUDY.key(), resolveStudy(individualsCommandOptions.groupByCommandOptions.study));
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.UID.key(), individualsCommandOptions.groupByCommandOptions.id);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ID.key(), individualsCommandOptions.groupByCommandOptions.name);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FATHER.key(), individualsCommandOptions.groupByCommandOptions.fatherId);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.MOTHER.key(), individualsCommandOptions.groupByCommandOptions.motherId);
        params.put(IndividualDBAdaptor.QueryParams.SEX.key(), individualsCommandOptions.groupByCommandOptions.sex);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ETHNICITY.key(), individualsCommandOptions.groupByCommandOptions.ethnicity);
        // TODO: Remove this deprecated parameters in future release
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                individualsCommandOptions.groupByCommandOptions.population);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                individualsCommandOptions.groupByCommandOptions.populationName);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_DESCRIPTION.key(),
                individualsCommandOptions.groupByCommandOptions.populationDescription);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_SUBPOPULATION.key(),
                individualsCommandOptions.groupByCommandOptions.populationSubpopulation);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(),
                individualsCommandOptions.groupByCommandOptions.karyotypicSex);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(),
                individualsCommandOptions.groupByCommandOptions.lifeStatus);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.AFFECTATION_STATUS.key(),
                individualsCommandOptions.groupByCommandOptions.affectationStatus);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                individualsCommandOptions.groupByCommandOptions.variableSetId);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ANNOTATION.key(),
                individualsCommandOptions.groupByCommandOptions.annotation);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                individualsCommandOptions.groupByCommandOptions.annotationSetName);

        return openCGAClient.getIndividualClient().groupBy(
                individualsCommandOptions.groupByCommandOptions.study, individualsCommandOptions.groupByCommandOptions.fields, params);
    }

    private QueryResponse<Sample> getSamples() throws CatalogException, IOException {
        logger.debug("Getting samples of individual(s)");

        Query query = new Query();
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), resolveStudy(individualsCommandOptions.sampleCommandOptions.study));
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key(), individualsCommandOptions.sampleCommandOptions.individual);

        QueryOptions options = new QueryOptions();
        options.putIfNotNull(QueryOptions.INCLUDE, individualsCommandOptions.sampleCommandOptions.dataModelOptions.include);
        options.putIfNotNull(QueryOptions.EXCLUDE, individualsCommandOptions.sampleCommandOptions.dataModelOptions.exclude);

        return openCGAClient.getSampleClient().search(query, options);
    }

    private QueryResponse<IndividualAclEntry> updateAcl() throws IOException, CatalogException {
        IndividualCommandOptions.IndividualAclCommandOptions.AclsUpdateCommandOptions commandOptions =
                individualsCommandOptions.aclsUpdateCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotNull("study", commandOptions.study);

        ObjectMap bodyParams = new ObjectMap();
        bodyParams.putIfNotNull("permissions", commandOptions.permissions);
        bodyParams.putIfNotNull("action", commandOptions.action);
        bodyParams.putIfNotNull("propagate", commandOptions.propagate);
        bodyParams.putIfNotNull("individual", extractIdsFromListOrFile(commandOptions.id));
        bodyParams.putIfNotNull("sample", extractIdsFromListOrFile(commandOptions.sample));

        return openCGAClient.getIndividualClient().updateAcl(commandOptions.memberId, queryParams, bodyParams);
    }

    private QueryResponse stats() throws IOException {
        logger.debug("Individual stats");

        IndividualCommandOptions.StatsCommandOptions commandOptions = individualsCommandOptions.statsCommandOptions;

        Query query = new Query();
        query.putIfNotEmpty("creationYear", commandOptions.creationYear);
        query.putIfNotEmpty("creationMonth", commandOptions.creationMonth);
        query.putIfNotEmpty("creationDay", commandOptions.creationDay);
        query.putIfNotEmpty("creationDayOfWeek", commandOptions.creationDayOfWeek);
        query.putIfNotEmpty("status", commandOptions.status);
        query.putIfNotEmpty("lifeStatus", commandOptions.lifeStatus);
        query.putIfNotEmpty("affectationStatus", commandOptions.affectationStatus);
        query.putIfNotEmpty("numSamples", commandOptions.numSamples);
        query.putIfNotEmpty("numMultiples", commandOptions.numMultiples);
        query.putIfNotEmpty("multiplesType", commandOptions.multiplesType);
        query.putIfNotEmpty("sex", commandOptions.sex);
        query.putIfNotEmpty("karyotypicSex", commandOptions.karyotypicSex);
        query.putIfNotEmpty("ethnicity", commandOptions.ethnicity);
        query.putIfNotEmpty("population", commandOptions.population);
        query.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        query.putIfNotEmpty("release", commandOptions.release);
        query.putIfNotEmpty("version", commandOptions.version);
        query.putIfNotNull("hasFather", commandOptions.hasFather);
        query.putIfNotNull("hasMother", commandOptions.hasMother);
        query.putIfNotNull("parentalConsanguinity", commandOptions.parentalConsanguinity);
        query.putIfNotEmpty(Constants.ANNOTATION, commandOptions.annotation);

        QueryOptions options = new QueryOptions();
        options.put("default", commandOptions.defaultStats);
        options.putIfNotNull("field", commandOptions.field);

        return openCGAClient.getIndividualClient().stats(commandOptions.study, query, options);
    }

}
