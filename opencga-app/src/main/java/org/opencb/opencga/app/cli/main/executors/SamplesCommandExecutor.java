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
import org.opencb.opencga.app.cli.main.options.SampleCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
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
        switch (subCommandString) {
            case "create":
                createOutput(create());
                break;
            case "load":
                createOutput(load());
                break;
            case "info":
                createOutput(info());
                break;
            case "search":
                createOutput(search());
                break;
            case "update":
                createOutput(update());
                break;
            case "delete":
                createOutput(delete());
                break;
            case "groupBy":
                createOutput(groupBy());
                break;
            case "acl":
                createOutput(aclCommandExecutor.acls(samplesCommandOptions.aclsCommandOptions, openCGAClient.getSampleClient()));
                break;
            case "acl-create":
                createOutput(aclCommandExecutor.aclsCreate(samplesCommandOptions.aclsCreateCommandOptions,
                        openCGAClient.getSampleClient()));
                break;
            case "acl-member-delete":
                createOutput(aclCommandExecutor.aclMemberDelete(samplesCommandOptions.aclsMemberDeleteCommandOptions,
                        openCGAClient.getSampleClient()));
                break;
            case "acl-member-info":
                createOutput(aclCommandExecutor.aclMemberInfo(samplesCommandOptions.aclsMemberInfoCommandOptions,
                        openCGAClient.getSampleClient()));
                break;
            case "acl-member-update":
                createOutput(aclCommandExecutor.aclMemberUpdate(samplesCommandOptions.aclsMemberUpdateCommandOptions,
                        openCGAClient.getSampleClient()));
                break;
            case "annotation-sets-create":
                createOutput(annotationCommandExecutor.createAnnotationSet(samplesCommandOptions.annotationCreateCommandOptions,
                        openCGAClient.getSampleClient()));
                break;
            case "annotation-sets-all-info":
                createOutput(annotationCommandExecutor.getAllAnnotationSets(samplesCommandOptions.annotationAllInfoCommandOptions,
                        openCGAClient.getSampleClient()));
                break;
            case "annotation-sets-search":
                createOutput(annotationCommandExecutor.searchAnnotationSets(samplesCommandOptions.annotationSearchCommandOptions,
                        openCGAClient.getSampleClient()));
                break;
            case "annotation-sets-delete":
                createOutput(annotationCommandExecutor.deleteAnnotationSet(samplesCommandOptions.annotationDeleteCommandOptions,
                        openCGAClient.getSampleClient()));
                break;
            case "annotation-sets-info":
                createOutput(annotationCommandExecutor.getAnnotationSet(samplesCommandOptions.annotationInfoCommandOptions,
                        openCGAClient.getSampleClient()));
                break;
            case "annotation-sets-update":
                createOutput(annotationCommandExecutor.updateAnnotationSet(samplesCommandOptions.annotationUpdateCommandOptions,
                        openCGAClient.getSampleClient()));
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }
    /********************************************  Administration commands  ***********************************************/
    private QueryResponse<Sample> create() throws CatalogException, IOException {
        logger.debug("Creating sample");
        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(samplesCommandOptions.createCommandOptions.description)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.DESCRIPTION.key(), samplesCommandOptions.createCommandOptions.description);
        }

        if (StringUtils.isNotEmpty(samplesCommandOptions.createCommandOptions.source)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.createCommandOptions.source);
        }

        return openCGAClient.getSampleClient().create(samplesCommandOptions.createCommandOptions.studyId,
                samplesCommandOptions.createCommandOptions.name, objectMap);
    }

    private QueryResponse<Sample> load() throws CatalogException, IOException {
        logger.debug("Loading samples from a pedigree file");

        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(samplesCommandOptions.loadCommandOptions.fileId)) {
            objectMap.put("fileId", samplesCommandOptions.loadCommandOptions.fileId);
        }

        if (StringUtils.isNotEmpty(samplesCommandOptions.loadCommandOptions.variableSetId)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), samplesCommandOptions.loadCommandOptions.variableSetId);
        }

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
        query.put(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(),samplesCommandOptions.searchCommandOptions.studyId);


        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.id)) {
            query.put(CatalogSampleDBAdaptor.QueryParams.ID.key(), samplesCommandOptions.searchCommandOptions.id);
        }

        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.name)) {
            query.put(CatalogSampleDBAdaptor.QueryParams.NAME.key(), samplesCommandOptions.searchCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.source)) {
            query.put(CatalogSampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.searchCommandOptions.source);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.individualId)) {
            query.put(CatalogSampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(),
                    samplesCommandOptions.searchCommandOptions.individualId);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.annotationSetName)) {
            query.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                    samplesCommandOptions.searchCommandOptions.annotationSetName);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.variableSetId)) {
            query.put(CatalogSampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                    samplesCommandOptions.searchCommandOptions.variableSetId);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.annotation)) {
            query.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION.key(), samplesCommandOptions.searchCommandOptions.annotation);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, samplesCommandOptions.searchCommandOptions.include);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, samplesCommandOptions.searchCommandOptions.exclude);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, samplesCommandOptions.searchCommandOptions.limit);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, samplesCommandOptions.searchCommandOptions.skip);
        }

        queryOptions.put("count", samplesCommandOptions.searchCommandOptions.count);

        return openCGAClient.getSampleClient().search(query, queryOptions);
    }

    private QueryResponse<Sample> update() throws CatalogException, IOException {
        logger.debug("Updating samples");

        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(samplesCommandOptions.updateCommandOptions.name)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.NAME.key(), samplesCommandOptions.updateCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.updateCommandOptions.description)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.DESCRIPTION.key(), samplesCommandOptions.updateCommandOptions.description);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.updateCommandOptions.source)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.updateCommandOptions.source);
        }

        if (StringUtils.isNotEmpty(samplesCommandOptions.updateCommandOptions.individualId)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), samplesCommandOptions.updateCommandOptions.individualId);
        }

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
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.ID.key(), samplesCommandOptions.groupByCommandOptions.id);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.name)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.NAME.key(), samplesCommandOptions.groupByCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.source)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.groupByCommandOptions.source);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.individualId)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), samplesCommandOptions.groupByCommandOptions.individualId);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.annotationSetName)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(),
                    samplesCommandOptions.groupByCommandOptions.annotationSetName);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.variableSetId)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                    samplesCommandOptions.groupByCommandOptions.variableSetId);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.groupByCommandOptions.annotation)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION.key(),
                    samplesCommandOptions.groupByCommandOptions.annotation);
        }
        return openCGAClient.getSampleClient().groupBy(samplesCommandOptions.groupByCommandOptions.studyId,
                samplesCommandOptions.groupByCommandOptions.by,objectMap);
    }

}
