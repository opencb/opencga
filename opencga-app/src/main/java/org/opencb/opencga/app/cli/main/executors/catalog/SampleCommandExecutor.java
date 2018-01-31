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


import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AnnotationCommandExecutor;
import org.opencb.opencga.app.cli.main.options.SampleCommandOptions;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.acls.permissions.SampleAclEntry;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by imedina on 03/06/16.
 */
public class SampleCommandExecutor extends OpencgaCommandExecutor {

    private SampleCommandOptions samplesCommandOptions;
    private AclCommandExecutor<Sample, SampleAclEntry> aclCommandExecutor;
    private AnnotationCommandExecutor<Sample, SampleAclEntry> annotationCommandExecutor;

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
        QueryResponse queryResponse = null;
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

    private QueryResponse<Sample> create() throws CatalogException, IOException {
        logger.debug("Creating sample");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.DESCRIPTION.key(), samplesCommandOptions.createCommandOptions.description);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.TYPE.key(), samplesCommandOptions.createCommandOptions.type);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.createCommandOptions.source);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.NAME.key(), samplesCommandOptions.createCommandOptions.name);
        params.put(SampleDBAdaptor.QueryParams.SOMATIC.key(), samplesCommandOptions.createCommandOptions.somatic);

        return openCGAClient.getSampleClient().create(samplesCommandOptions.createCommandOptions.study,
                samplesCommandOptions.createCommandOptions.individual, params);
    }

    private QueryResponse<Sample> load() throws CatalogException, IOException {
        logger.debug("Loading samples from a pedigree file");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("file", samplesCommandOptions.loadCommandOptions.pedFile);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), samplesCommandOptions.loadCommandOptions.variableSetId);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), samplesCommandOptions.loadCommandOptions.study);
        return openCGAClient.getSampleClient().loadFromPed(samplesCommandOptions.loadCommandOptions.study, params);
    }

    private QueryResponse<Sample> info() throws CatalogException, IOException  {
        logger.debug("Getting samples information");

        ObjectMap params = new ObjectMap();
        params.putIfNotNull(SampleDBAdaptor.QueryParams.STUDY.key(), resolveStudy(samplesCommandOptions.infoCommandOptions.study));
        params.putIfNotEmpty(QueryOptions.INCLUDE, samplesCommandOptions.infoCommandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, samplesCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        if (samplesCommandOptions.infoCommandOptions.noLazy) {
            params.put("lazy", false);
        }
        return openCGAClient.getSampleClient().get(samplesCommandOptions.infoCommandOptions.sample, params);
    }

    private QueryResponse<Sample> search() throws CatalogException, IOException  {
        logger.debug("Searching samples");

        Query query = new Query();
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), resolveStudy(samplesCommandOptions.searchCommandOptions.study));
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.NAME.key(), samplesCommandOptions.searchCommandOptions.name);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.searchCommandOptions.source);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.TYPE.key(), samplesCommandOptions.searchCommandOptions.type);
        query.putIfNotNull(SampleDBAdaptor.QueryParams.SOMATIC.key(), samplesCommandOptions.searchCommandOptions.somatic);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), samplesCommandOptions.searchCommandOptions.individual);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.ANNOTATION.key(), samplesCommandOptions.searchCommandOptions.annotation);

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

    private QueryResponse<Sample> update() throws CatalogException, IOException {
        logger.debug("Updating samples");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.NAME.key(), samplesCommandOptions.updateCommandOptions.name);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.DESCRIPTION.key(), samplesCommandOptions.updateCommandOptions.description);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.updateCommandOptions.source);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.TYPE.key(), samplesCommandOptions.updateCommandOptions.type);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), samplesCommandOptions.updateCommandOptions.individual);
        params.putIfNotNull(SampleDBAdaptor.QueryParams.SOMATIC.key(), samplesCommandOptions.updateCommandOptions.somatic);
        return openCGAClient.getSampleClient().update(samplesCommandOptions.updateCommandOptions.sample,
                samplesCommandOptions.updateCommandOptions.study, params);
    }

    private QueryResponse<Sample> delete() throws CatalogException, IOException {
        logger.debug("Deleting the selected sample");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), samplesCommandOptions.deleteCommandOptions.study);
        return openCGAClient.getSampleClient().delete(samplesCommandOptions.deleteCommandOptions.sample, params);
    }

    private QueryResponse<ObjectMap> groupBy() throws CatalogException, IOException {
        logger.debug("Group By samples");

        String study = resolveStudy(samplesCommandOptions.groupByCommandOptions.study);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), study);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.ID.key(), samplesCommandOptions.groupByCommandOptions.id);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.NAME.key(), samplesCommandOptions.groupByCommandOptions.name);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.groupByCommandOptions.source);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), samplesCommandOptions.groupByCommandOptions.individual);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                samplesCommandOptions.groupByCommandOptions.annotationSetName);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), samplesCommandOptions.groupByCommandOptions.variableSetId);
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.ANNOTATION.key(), samplesCommandOptions.groupByCommandOptions.annotation);

        return openCGAClient.getSampleClient().groupBy(samplesCommandOptions.groupByCommandOptions.study,
                samplesCommandOptions.groupByCommandOptions.fields, params);
    }

    private QueryResponse<Individual> getIndividuals() throws CatalogException, IOException {
        logger.debug("Getting individuals of sample(s)");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), resolveStudy(samplesCommandOptions.individualCommandOptions.study));
        params.put("lazy", false); // Obtain the whole individual entity
        params.putIfNotNull(QueryOptions.INCLUDE, samplesCommandOptions.individualCommandOptions.dataModelOptions.include);
        params.putIfNotNull(QueryOptions.EXCLUDE, samplesCommandOptions.individualCommandOptions.dataModelOptions.exclude);

        QueryResponse<Sample> sampleQueryResponse =
                openCGAClient.getSampleClient().get(samplesCommandOptions.individualCommandOptions.sample, params);

        if (sampleQueryResponse.allResultsSize() == 0) {
            return new QueryResponse<>(sampleQueryResponse.getApiVersion(), -1, sampleQueryResponse.getWarning(),
                    sampleQueryResponse.getError(), sampleQueryResponse.getQueryOptions(), new LinkedList<>());
        }

        // We get the individuals from the sample response
        List<Individual> individualList = sampleQueryResponse.allResults()
                .stream()
                .map(Sample::getIndividual)
                .collect(Collectors.toCollection(LinkedList::new));

        return new QueryResponse<>(sampleQueryResponse.getApiVersion(), -1, sampleQueryResponse.getWarning(),
                sampleQueryResponse.getError(), sampleQueryResponse.getQueryOptions(),
                Arrays.asList(new QueryResult<>(samplesCommandOptions.individualCommandOptions.sample,
                        -1, individualList.size(), individualList.size(), "", "", individualList)));
    }


    private QueryResponse<SampleAclEntry> updateAcl() throws IOException, CatalogException {
        SampleCommandOptions.SampleAclCommandOptions.AclsUpdateCommandOptions commandOptions =
                samplesCommandOptions.aclsUpdateCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotNull("study", commandOptions.study);

        ObjectMap bodyParams = new ObjectMap();
        bodyParams.putIfNotNull("permissions", commandOptions.permissions);
        bodyParams.putIfNotNull("propagate", commandOptions.propagate);
        bodyParams.putIfNotNull("action", commandOptions.action);
        bodyParams.putIfNotNull("sample", commandOptions.id);
        bodyParams.putIfNotNull("individual", commandOptions.individual);
        bodyParams.putIfNotNull("cohort", commandOptions.cohort);
        bodyParams.putIfNotNull("file", commandOptions.file);

        return openCGAClient.getSampleClient().updateAcl(commandOptions.memberId, queryParams, bodyParams);
    }

}
