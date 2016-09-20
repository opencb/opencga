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
import org.opencb.opencga.app.cli.main.options.catalog.SampleCommandOptions;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.acls.permissions.SampleAclEntry;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class SamplesCommandExecutor extends OpencgaCommandExecutor {

    private SampleCommandOptions samplesCommandOptions;
    private AclCommandExecutor<Sample, SampleAclEntry> aclCommandExecutor;
    private AnnotationCommandExecutor<Sample, SampleAclEntry> annotationCommandExecutor;

    public SamplesCommandExecutor(SampleCommandOptions samplesCommandOptions) {
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
            case "acl":
                queryResponse = aclCommandExecutor.acls(samplesCommandOptions.aclsCommandOptions, openCGAClient.getSampleClient());
                break;
            case "acl-create":
                queryResponse = aclCommandExecutor.aclsCreate(samplesCommandOptions.aclsCreateCommandOptions,
                        openCGAClient.getSampleClient());
                break;
            case "acl-member-delete":
                queryResponse = aclCommandExecutor.aclMemberDelete(samplesCommandOptions.aclsMemberDeleteCommandOptions,
                        openCGAClient.getSampleClient());
                break;
            case "acl-member-info":
                queryResponse = aclCommandExecutor.aclMemberInfo(samplesCommandOptions.aclsMemberInfoCommandOptions,
                        openCGAClient.getSampleClient());
                break;
            case "acl-member-update":
                queryResponse = aclCommandExecutor.aclMemberUpdate(samplesCommandOptions.aclsMemberUpdateCommandOptions,
                        openCGAClient.getSampleClient());
                break;
            case "annotation-sets-create":
                queryResponse = annotationCommandExecutor.createAnnotationSet(samplesCommandOptions.annotationCreateCommandOptions,
                        openCGAClient.getSampleClient());
                break;
            case "annotation-sets-all-info":
                queryResponse = annotationCommandExecutor.getAllAnnotationSets(samplesCommandOptions.annotationAllInfoCommandOptions,
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
            case "annotation-sets-info":
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
    /********************************************  Administration commands  ***********************************************/
    private QueryResponse<Sample> create() throws CatalogException, IOException {
        logger.debug("Creating sample");
        ObjectMap objectMap = new ObjectMap();

        objectMap.putIfNotEmpty(SampleDBAdaptor.QueryParams.DESCRIPTION.key(), samplesCommandOptions.createCommandOptions.description);
        objectMap.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.createCommandOptions.source);

        return openCGAClient.getSampleClient().create(samplesCommandOptions.createCommandOptions.studyId,
                samplesCommandOptions.createCommandOptions.name, objectMap);
    }

    private QueryResponse<Sample> load() throws CatalogException, IOException {
        logger.debug("Loading samples from a pedigree file");

        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotEmpty("fileId", samplesCommandOptions.loadCommandOptions.fileId);
        objectMap.putIfNotEmpty(SampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), samplesCommandOptions.loadCommandOptions.variableSetId);

        return openCGAClient.getSampleClient().loadFromPed(samplesCommandOptions.loadCommandOptions.studyId, objectMap);
    }

    private QueryResponse<Sample> info() throws CatalogException, IOException  {
        logger.debug("Getting samples information");
        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(samplesCommandOptions.infoCommandOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, samplesCommandOptions.infoCommandOptions.include);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.infoCommandOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, samplesCommandOptions.infoCommandOptions.exclude);
        }

        return openCGAClient.getSampleClient().get(samplesCommandOptions.infoCommandOptions.id, queryOptions);
    }

    private QueryResponse<Sample> search() throws CatalogException, IOException  {
        logger.debug("Searching samples");

        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();

        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY_ID.key(),samplesCommandOptions.searchCommandOptions.studyId);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.ID.key(), samplesCommandOptions.searchCommandOptions.id);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.NAME.key(), samplesCommandOptions.searchCommandOptions.name);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.searchCommandOptions.source);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(),
                samplesCommandOptions.searchCommandOptions.individualId);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                samplesCommandOptions.searchCommandOptions.annotationSetName);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                samplesCommandOptions.searchCommandOptions.variableSetId);
        query.putIfNotEmpty(SampleDBAdaptor.QueryParams.ANNOTATION.key(), samplesCommandOptions.searchCommandOptions.annotation);
        queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, samplesCommandOptions.searchCommandOptions.include);
        queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, samplesCommandOptions.searchCommandOptions.exclude);
        queryOptions.putIfNotEmpty(QueryOptions.LIMIT, samplesCommandOptions.searchCommandOptions.limit);
        queryOptions.putIfNotEmpty(QueryOptions.SKIP, samplesCommandOptions.searchCommandOptions.skip);

        queryOptions.put("count", samplesCommandOptions.searchCommandOptions.count);

        return openCGAClient.getSampleClient().search(query, queryOptions);
    }

    private QueryResponse<Sample> update() throws CatalogException, IOException {
        logger.debug("Updating samples");

        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotEmpty(SampleDBAdaptor.QueryParams.NAME.key(), samplesCommandOptions.updateCommandOptions.name);
        objectMap.putIfNotEmpty(SampleDBAdaptor.QueryParams.DESCRIPTION.key(), samplesCommandOptions.updateCommandOptions.description);
        objectMap.putIfNotEmpty(SampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.updateCommandOptions.source);
        objectMap.putIfNotEmpty(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), samplesCommandOptions.updateCommandOptions.individualId);


        return openCGAClient.getSampleClient().update(samplesCommandOptions.updateCommandOptions.id, objectMap);
    }

    private QueryResponse<Sample> delete() throws CatalogException, IOException {
        logger.debug("Deleting the select sample");

        ObjectMap objectMap = new ObjectMap();
        return openCGAClient.getSampleClient().delete(samplesCommandOptions.deleteCommandOptions.id, objectMap);
    }

    private QueryResponse<Sample> groupBy() throws CatalogException, IOException {
        logger.debug("Group By samples");

        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.id)) {
            objectMap.put(SampleDBAdaptor.QueryParams.ID.key(), samplesCommandOptions.groupByCommandOptions.id);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.name)) {
            objectMap.put(SampleDBAdaptor.QueryParams.NAME.key(), samplesCommandOptions.groupByCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.source)) {
            objectMap.put(SampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.groupByCommandOptions.source);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.individualId)) {
            objectMap.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), samplesCommandOptions.groupByCommandOptions.individualId);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.annotationSetName)) {
            objectMap.put(SampleDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                    samplesCommandOptions.groupByCommandOptions.annotationSetName);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.variableSetId)) {
            objectMap.put(SampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                    samplesCommandOptions.groupByCommandOptions.variableSetId);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.annotation)) {
            objectMap.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(),
                    samplesCommandOptions.groupByCommandOptions.annotation);
        }
        return openCGAClient.getSampleClient().groupBy(samplesCommandOptions.groupByCommandOptions.studyId,
                samplesCommandOptions.groupByCommandOptions.fields,objectMap);
    }

}
