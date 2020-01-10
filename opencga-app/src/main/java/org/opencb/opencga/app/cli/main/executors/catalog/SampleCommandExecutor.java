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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AnnotationCommandExecutor;
import org.opencb.opencga.app.cli.main.options.SampleCommandOptions;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.util.*;

/**
 * Created by imedina on 03/06/16.
 */
public class SampleCommandExecutor extends OpencgaCommandExecutor {

    private SampleCommandOptions samplesCommandOptions;
    private AclCommandExecutor<Sample> aclCommandExecutor;
    private AnnotationCommandExecutor<Sample> annotationCommandExecutor;

    public SampleCommandExecutor(SampleCommandOptions samplesCommandOptions) {
        super(samplesCommandOptions.commonCommandOptions);
        this.samplesCommandOptions = samplesCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
        this.annotationCommandExecutor = new AnnotationCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing samples command line");

        String subCommandString = getParsedSubCommand(samplesCommandOptions.jCommander);
        RestResponse queryResponse = null;
        switch (subCommandString) {
            case "create":
                queryResponse = create();
                break;
            case "load":
                queryResponse = load();
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
            case "groupBy":
                queryResponse = groupBy();
                break;
            case "stats":
                queryResponse = stats();
                break;
            case "individuals":
                queryResponse = getIndividuals();
                break;
            case "acl":
                queryResponse = aclCommandExecutor.acls(samplesCommandOptions.aclsCommandOptions, openCGAClient.getSampleClient());
                break;
            case "acl-update":
                queryResponse = updateAcl();
                break;
            case "annotation-sets-create":
                queryResponse = annotationCommandExecutor.createAnnotationSet(samplesCommandOptions.annotationCreateCommandOptions,
                        openCGAClient.getSampleClient());
                break;
            case "annotation-sets-search":
                queryResponse = annotationCommandExecutor.searchAnnotationSets(samplesCommandOptions.annotationSearchCommandOptions,
                        openCGAClient.getSampleClient());
                break;
            case "annotation-sets-delete":
                queryResponse = annotationCommandExecutor.deleteAnnotationSet(samplesCommandOptions.annotationDeleteCommandOptions,
                        openCGAClient.getSampleClient());
                break;
            case "annotation-sets":
                queryResponse = annotationCommandExecutor.getAnnotationSet(samplesCommandOptions.annotationInfoCommandOptions,
                        openCGAClient.getSampleClient());
                break;
            case "annotation-sets-update":
                queryResponse = annotationCommandExecutor.updateAnnotationSet(samplesCommandOptions.annotationUpdateCommandOptions,
                        openCGAClient.getSampleClient());
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private RestResponse<Sample> create() throws CatalogException, IOException {
        logger.debug("Creating sample");

        SampleCommandOptions.CreateCommandOptions commandOptions = samplesCommandOptions.createCommandOptions;

        ObjectMap params;
        if (StringUtils.isNotEmpty(commandOptions.json)) {
            params = loadFile(commandOptions.json);
        } else {
            params = new ObjectMap();
            params.putIfNotEmpty(SampleDBAdaptor.QueryParams.DESCRIPTION.key(), commandOptions.description);
            params.putIfNotEmpty(SampleDBAdaptor.QueryParams.NAME.key(), commandOptions.name);
            params.putIfNotEmpty(SampleDBAdaptor.QueryParams.TYPE.key(), commandOptions.type);
            params.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), commandOptions.source);
            params.putIfNotEmpty(SampleDBAdaptor.QueryParams.ID.key(), commandOptions.name);
            params.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), commandOptions.individual);
            params.put(SampleDBAdaptor.QueryParams.SOMATIC.key(), commandOptions.somatic);
        }

        return openCGAClient.getSampleClient().create(commandOptions.study, commandOptions.id, params);
    }

    private RestResponse<Sample> load() throws CatalogException, IOException {
        logger.debug("Loading samples from a pedigree file");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("file", samplesCommandOptions.loadCommandOptions.pedFile);
        params.putIfNotEmpty(Constants.VARIABLE_SET, samplesCommandOptions.loadCommandOptions.variableSetId);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), samplesCommandOptions.loadCommandOptions.study);
        return openCGAClient.getSampleClient().loadFromPed(samplesCommandOptions.loadCommandOptions.study, params);
    }

    private RestResponse<Sample> info() throws CatalogException, IOException  {
        logger.debug("Getting samples information");

        ObjectMap params = new ObjectMap();
        params.putIfNotNull(SampleDBAdaptor.QueryParams.STUDY.key(), resolveStudy(samplesCommandOptions.infoCommandOptions.study));
        params.putIfNotEmpty(QueryOptions.INCLUDE, samplesCommandOptions.infoCommandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, samplesCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        params.put("flattenAnnotations", samplesCommandOptions.searchCommandOptions.flattenAnnotations);
        if (samplesCommandOptions.infoCommandOptions.noLazy) {
            params.put("lazy", false);
        }
        return openCGAClient.getSampleClient().get(samplesCommandOptions.infoCommandOptions.sample, params);
    }

    private RestResponse<Sample> search() throws CatalogException, IOException  {
        logger.debug("Searching samples");

        Query query = new Query();
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), resolveStudy(samplesCommandOptions.searchCommandOptions.study));
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.ID.key(), samplesCommandOptions.searchCommandOptions.name);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.searchCommandOptions.source);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.TYPE.key(), samplesCommandOptions.searchCommandOptions.type);
        query.putIfNotNull(SampleDBAdaptor.QueryParams.SOMATIC.key(), samplesCommandOptions.searchCommandOptions.somatic);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), samplesCommandOptions.searchCommandOptions.individual);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.ANNOTATION.key(), samplesCommandOptions.searchCommandOptions.annotation);
        query.put("flattenAnnotations", samplesCommandOptions.searchCommandOptions.flattenAnnotations);
        query.putAll(samplesCommandOptions.searchCommandOptions.commonOptions.params);

        if (samplesCommandOptions.searchCommandOptions.numericOptions.count) {
            return openCGAClient.getSampleClient().count(query);
        } else {
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, samplesCommandOptions.searchCommandOptions.dataModelOptions.include);
            queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, samplesCommandOptions.searchCommandOptions.dataModelOptions.exclude);
            queryOptions.put(QueryOptions.LIMIT, samplesCommandOptions.searchCommandOptions.numericOptions.limit);
            queryOptions.put(QueryOptions.SKIP, samplesCommandOptions.searchCommandOptions.numericOptions.skip);

            return openCGAClient.getSampleClient().search(query, queryOptions);
        }
    }

    private RestResponse<Sample> update() throws CatalogException, IOException {
        logger.debug("Updating samples");

        SampleCommandOptions.UpdateCommandOptions commandOptions = samplesCommandOptions.updateCommandOptions;

        ObjectMap params;
        if (StringUtils.isNotEmpty(commandOptions.json)) {
            params = loadFile(commandOptions.json);
        } else {
            params = new ObjectMap();
            params.putIfNotEmpty(SampleDBAdaptor.QueryParams.NAME.key(), commandOptions.name);
            params.putIfNotEmpty(SampleDBAdaptor.QueryParams.DESCRIPTION.key(), commandOptions.description);
            params.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), commandOptions.source);
            params.putIfNotEmpty(SampleDBAdaptor.QueryParams.TYPE.key(), commandOptions.type);
            params.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), commandOptions.individual);
            params.putIfNotNull(SampleDBAdaptor.QueryParams.SOMATIC.key(), commandOptions.somatic);
        }

        return openCGAClient.getSampleClient().update(commandOptions.study, commandOptions.sample,
                commandOptions.annotationSetsAction.name(), params);
    }

    private RestResponse<Sample> delete() throws CatalogException, IOException {
        logger.debug("Deleting the selected sample");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), samplesCommandOptions.deleteCommandOptions.study);
        return openCGAClient.getSampleClient().delete(samplesCommandOptions.deleteCommandOptions.sample, params);
    }

    private RestResponse<ObjectMap> groupBy() throws CatalogException, IOException {
        logger.debug("Group By samples");

        String study = resolveStudy(samplesCommandOptions.groupByCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), study);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.UID.key(), samplesCommandOptions.groupByCommandOptions.id);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.ID.key(), samplesCommandOptions.groupByCommandOptions.name);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.groupByCommandOptions.source);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), samplesCommandOptions.groupByCommandOptions.individual);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.ANNOTATION.key(), samplesCommandOptions.groupByCommandOptions.annotation);

        return openCGAClient.getSampleClient().groupBy(samplesCommandOptions.groupByCommandOptions.study,
                samplesCommandOptions.groupByCommandOptions.fields, params);
    }

    private RestResponse<Individual> getIndividuals() throws IOException {
        logger.debug("Getting individuals of sample(s)");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), resolveStudy(samplesCommandOptions.individualCommandOptions.study));
        params.put("includeIndividual", true); // Obtain the whole individual entity
        params.putIfNotNull(QueryOptions.INCLUDE, samplesCommandOptions.individualCommandOptions.dataModelOptions.include);
        params.putIfNotNull(QueryOptions.EXCLUDE, samplesCommandOptions.individualCommandOptions.dataModelOptions.exclude);

        RestResponse<Sample> sampleQueryResponse =
                openCGAClient.getSampleClient().get(samplesCommandOptions.individualCommandOptions.sample, params);

        if (sampleQueryResponse.allResultsSize() == 0) {
            return new RestResponse<>(sampleQueryResponse.getApiVersion(), -1, sampleQueryResponse.getEvents(),
                    sampleQueryResponse.getParams(), new LinkedList<>());
        }

        // We get the individuals from the sample response
        List<Individual> individualList = new ArrayList<>();
        for (Sample sample : sampleQueryResponse.allResults()) {
            Map<String, Object> attributes = sample.getAttributes();
            ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
            if (attributes != null && attributes.containsKey("OPENCGA_INDIVIDUAL")) {
                String individualStr = objectMapper.writeValueAsString(attributes.get("OPENCGA_INDIVIDUAL"));
                individualList.add(objectMapper.readValue(individualStr, Individual.class));
            }
        }

        return new RestResponse<>(sampleQueryResponse.getApiVersion(), -1, sampleQueryResponse.getEvents(), sampleQueryResponse.getParams(),
                Collections.singletonList(new OpenCGAResult<>(-1, Collections.emptyList(), individualList.size(), individualList,
                        individualList.size())));
    }


    private RestResponse<ObjectMap> updateAcl() throws IOException, CatalogException {
        SampleCommandOptions.SampleAclCommandOptions.AclsUpdateCommandOptions commandOptions =
                samplesCommandOptions.aclsUpdateCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotNull("study", commandOptions.study);

        ObjectMap bodyParams = new ObjectMap();
        bodyParams.putIfNotNull("permissions", commandOptions.permissions);
        bodyParams.putIfNotNull("propagate", commandOptions.propagate);
        bodyParams.putIfNotNull("action", commandOptions.action);
        bodyParams.putIfNotNull("sample", extractIdsFromListOrFile(commandOptions.id));
        bodyParams.putIfNotNull("individual", extractIdsFromListOrFile(commandOptions.individual));
        bodyParams.putIfNotNull("cohort", extractIdsFromListOrFile(commandOptions.cohort));
        bodyParams.putIfNotNull("file", extractIdsFromListOrFile(commandOptions.file));

        return openCGAClient.getSampleClient().updateAcl(commandOptions.memberId, queryParams, bodyParams);
    }

    private RestResponse stats() throws IOException {
        logger.debug("Individual stats");

        SampleCommandOptions.StatsCommandOptions commandOptions = samplesCommandOptions.statsCommandOptions;

        Query query = new Query();
        query.putIfNotEmpty("creationYear", commandOptions.creationYear);
        query.putIfNotEmpty("creationMonth", commandOptions.creationMonth);
        query.putIfNotEmpty("creationDay", commandOptions.creationDay);
        query.putIfNotEmpty("creationDayOfWeek", commandOptions.creationDayOfWeek);
        query.putIfNotEmpty("status", commandOptions.status);
        query.putIfNotEmpty("source", commandOptions.source);
        query.putIfNotEmpty("type", commandOptions.type);
        query.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        query.putIfNotEmpty("release", commandOptions.release);
        query.putIfNotEmpty("version", commandOptions.version);
        query.putIfNotNull("somatic", commandOptions.somatic);
        query.putIfNotEmpty(Constants.ANNOTATION, commandOptions.annotation);

        QueryOptions options = new QueryOptions();
        options.put("default", commandOptions.defaultStats);
        options.putIfNotNull("field", commandOptions.field);

        return openCGAClient.getSampleClient().stats(commandOptions.study, query, options);
    }

}
