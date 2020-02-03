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
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.IndividualCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AnnotationCommandOptions;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualAclUpdateParams;
import org.opencb.opencga.core.models.individual.IndividualCreateParams;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleCreateParams;
import org.opencb.opencga.core.response.RestResponse;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Created by agaor on 6/06/16.
 */
public class IndividualCommandExecutor extends OpencgaCommandExecutor {

    private IndividualCommandOptions individualsCommandOptions;

    public IndividualCommandExecutor(IndividualCommandOptions individualsCommandOptions) {
        super(individualsCommandOptions.commonCommandOptions);
        this.individualsCommandOptions = individualsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing individuals command line");

        String subCommandString = getParsedSubCommand(individualsCommandOptions.jCommander);
        RestResponse queryResponse = null;
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
            case "stats":
                queryResponse = stats();
                break;
            case "samples":
                queryResponse = getSamples();
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

    private RestResponse<Individual> create() throws ClientException {
        logger.debug("Creating individual");

        IndividualCommandOptions.CreateCommandOptions commandOptions = individualsCommandOptions.createCommandOptions;

        IndividualCreateParams createParams = new IndividualCreateParams()
                .setId(commandOptions.id)
                .setName(commandOptions.name)
                .setFather(commandOptions.fatherId)
                .setMother(commandOptions.motherId)
                .setSex(commandOptions.sex)
                .setParentalConsanguinity(commandOptions.parentalConsanguinity)
                .setEthnicity(commandOptions.ethnicity)
                .setPopulation(new Individual.Population(commandOptions.populationName, commandOptions.populationSubpopulation,
                        commandOptions.populationDescription))
                .setKaryotypicSex(commandOptions.karyotypicSex)
                .setLifeStatus(commandOptions.lifeStatus)
                .setAffectationStatus(commandOptions.affectationStatus)
                .setDateOfBirth(commandOptions.dateOfBirth)
                .setSamples(commandOptions.samples != null
                        ? commandOptions.samples.stream().map(s -> new SampleCreateParams().setId(s)).collect(Collectors.toList())
                        : Collections.emptyList());

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.STUDY.key(), resolveStudy(commandOptions.study));

        return openCGAClient.getIndividualClient().create(createParams, params);
    }

    private RestResponse<Individual> info() throws ClientException {
        logger.debug("Getting individual information");

        IndividualCommandOptions.InfoCommandOptions commandOptions = individualsCommandOptions.infoCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.STUDY.key(), resolveStudy(commandOptions.study));
        params.putIfNotNull(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotNull(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);
        params.put("flattenAnnotations", commandOptions.flattenAnnotations);

        return openCGAClient.getIndividualClient().info(commandOptions.individual, params);
    }

    private RestResponse<Individual> search() throws ClientException {
        logger.debug("Searching individuals");

        IndividualCommandOptions.SearchCommandOptions commandOptions = individualsCommandOptions.searchCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.STUDY.key(), resolveStudy(commandOptions.study));
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ID.key(), commandOptions.name);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.FATHER.key(), commandOptions.fatherId);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.MOTHER.key(), commandOptions.motherId);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.SEX.key(), commandOptions.sex);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ETHNICITY.key(), commandOptions.ethnicity);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.SAMPLES.key(), commandOptions.samples);

        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(), commandOptions.populationName);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_SUBPOPULATION.key(), commandOptions.populationSubpopulation);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.POPULATION_DESCRIPTION.key(), commandOptions.populationDescription);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(), commandOptions.karyotypicSex);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(), commandOptions.lifeStatus);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.AFFECTATION_STATUS.key(), commandOptions.affectationStatus);
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), commandOptions.annotation);
        params.put("flattenAnnotations", commandOptions.flattenAnnotations);
        params.putAll(commandOptions.commonOptions.params);

        params.put(QueryOptions.COUNT, commandOptions.numericOptions.count);
        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);
        params.put(QueryOptions.SKIP, commandOptions.numericOptions.skip);
        params.put(QueryOptions.LIMIT, commandOptions.numericOptions.limit);

        return openCGAClient.getIndividualClient().search(params);
    }


    private RestResponse<Individual> update() throws ClientException {
        logger.debug("Updating individual information");

        IndividualCommandOptions.UpdateCommandOptions commandOptions = individualsCommandOptions.updateCommandOptions;

        IndividualUpdateParams updateParams = new IndividualUpdateParams()
                .setId(commandOptions.name)
                .setName(commandOptions.name)
                .setFather(commandOptions.fatherId)
                .setMother(commandOptions.motherId)
                .setDateOfBirth(commandOptions.dateOfBirth)
                .setSex(commandOptions.sex)
                .setEthnicity(commandOptions.ethnicity)
                .setKaryotypicSex(commandOptions.karyotypicSex)
                .setLifeStatus(commandOptions.lifeStatus)
                .setAffectationStatus(commandOptions.affectationStatus);
        if (StringUtils.isNotEmpty(commandOptions.populationDescription) || StringUtils.isNotEmpty(commandOptions.populationName)
                || StringUtils.isNotEmpty(commandOptions.populationSubpopulation)) {
            updateParams.setPopulation(new Individual.Population(commandOptions.name, commandOptions.populationSubpopulation,
                    commandOptions.populationDescription));
        }

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.STUDY.key(), resolveStudy(commandOptions.study));

        return openCGAClient.getIndividualClient().update(commandOptions.individual, updateParams, params);
    }

    private RestResponse<Individual> delete() throws ClientException {
        logger.debug("Deleting individual information");
        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.STUDY.key(), resolveStudy(individualsCommandOptions.deleteCommandOptions.study));

        return openCGAClient.getIndividualClient().delete(individualsCommandOptions.deleteCommandOptions.individual, params);
    }

    private RestResponse<Sample> getSamples() throws ClientException {
        logger.debug("Getting samples of individual(s)");

        IndividualCommandOptions.SampleCommandOptions commandOptions = individualsCommandOptions.sampleCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), resolveStudy(commandOptions.study));
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key(), commandOptions.individual);

        params.putIfNotNull(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotNull(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);

        return openCGAClient.getSampleClient().search(params);
    }

    private RestResponse<ObjectMap> updateAcl() throws CatalogException, ClientException {
        IndividualCommandOptions.IndividualAclCommandOptions.AclsUpdateCommandOptions commandOptions =
                individualsCommandOptions.aclsUpdateCommandOptions;

        IndividualAclUpdateParams updateParams = new IndividualAclUpdateParams()
                .setIndividual(extractIdsFromListOrFile(commandOptions.id))
                .setSample(extractIdsFromListOrFile(commandOptions.sample))
                .setPermissions(commandOptions.permissions)
                .setAction(commandOptions.action)
                .setPropagate(commandOptions.propagate);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("study", commandOptions.study);

        return openCGAClient.getIndividualClient().updateAcl(commandOptions.memberId, updateParams, params);
    }

    private RestResponse<FacetField> stats() throws ClientException {
        logger.debug("Individual stats");

        IndividualCommandOptions.StatsCommandOptions commandOptions = individualsCommandOptions.statsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IndividualDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty("creationYear", commandOptions.creationYear);
        params.putIfNotEmpty("creationMonth", commandOptions.creationMonth);
        params.putIfNotEmpty("creationDay", commandOptions.creationDay);
        params.putIfNotEmpty("creationDayOfWeek", commandOptions.creationDayOfWeek);
        params.putIfNotEmpty("status", commandOptions.status);
        params.putIfNotEmpty("lifeStatus", commandOptions.lifeStatus);
        params.putIfNotEmpty("affectationStatus", commandOptions.affectationStatus);
        params.putIfNotEmpty("numSamples", commandOptions.numSamples);
        params.putIfNotEmpty("numMultiples", commandOptions.numMultiples);
        params.putIfNotEmpty("multiplesType", commandOptions.multiplesType);
        params.putIfNotEmpty("sex", commandOptions.sex);
        params.putIfNotEmpty("karyotypicSex", commandOptions.karyotypicSex);
        params.putIfNotEmpty("ethnicity", commandOptions.ethnicity);
        params.putIfNotEmpty("population", commandOptions.population);
        params.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        params.putIfNotEmpty("release", commandOptions.release);
        params.putIfNotEmpty("version", commandOptions.version);
        params.putIfNotNull("hasFather", commandOptions.hasFather);
        params.putIfNotNull("hasMother", commandOptions.hasMother);
        params.putIfNotNull("parentalConsanguinity", commandOptions.parentalConsanguinity);
        params.putIfNotEmpty(Constants.ANNOTATION, commandOptions.annotation);

        params.put("default", commandOptions.defaultStats);
        params.putIfNotNull("field", commandOptions.field);

        return openCGAClient.getIndividualClient().aggregationStats(params);
    }

    private RestResponse<Individual> updateAnnotations() throws ClientException, IOException {
        AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions commandOptions =
                individualsCommandOptions.annotationUpdateCommandOptions;

        ObjectMapper mapper = new ObjectMapper();
        ObjectMap annotations = mapper.readValue(new File(commandOptions.annotations), ObjectMap.class);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
//        queryParams.putIfNotNull("action", updateCommandOptions.action);

        return openCGAClient.getIndividualClient().updateAnnotations(commandOptions.id, commandOptions.annotationSetId, annotations,
                params);
    }

    private RestResponse<ObjectMap> acl() throws ClientException {
        AclCommandOptions.AclsCommandOptions commandOptions = individualsCommandOptions.aclsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
        params.putIfNotEmpty("member", commandOptions.memberId);

        params.putAll(commandOptions.commonOptions.params);

        return openCGAClient.getIndividualClient().acl(commandOptions.id, params);
    }


}
