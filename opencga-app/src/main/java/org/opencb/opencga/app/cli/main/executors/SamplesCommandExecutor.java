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
import org.opencb.opencga.app.cli.main.options.SampleCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.acls.SampleAclEntry;
import org.opencb.opencga.client.rest.SampleClient;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class SamplesCommandExecutor extends OpencgaCommandExecutor {

    private SampleCommandOptions samplesCommandOptions;

    public SamplesCommandExecutor(SampleCommandOptions samplesCommandOptions) {
        super(samplesCommandOptions.commonCommandOptions);
        this.samplesCommandOptions = samplesCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing samples command line");

        String subCommandString = getParsedSubCommand(samplesCommandOptions.jCommander);
        switch (subCommandString) {
            case "create":
                create();
                break;
            case "load":
                load();
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
            case "groupBy":
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
    /********************************************  Administration commands  ***********************************************/
    private void create() throws CatalogException, IOException {
        logger.debug("Creating sample");
        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(samplesCommandOptions.createCommandOptions.description)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.DESCRIPTION.key(), samplesCommandOptions.createCommandOptions.description);
        }

        if (StringUtils.isNotEmpty(samplesCommandOptions.createCommandOptions.source)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.SOURCE.key(), samplesCommandOptions.createCommandOptions.source);
        }

        QueryResponse<Sample> sample = openCGAClient.getSampleClient().create(samplesCommandOptions.createCommandOptions.studyId,
                samplesCommandOptions.createCommandOptions.name, objectMap);

        System.out.println(sample.toString());
    }

    private void load() throws CatalogException, IOException {
        logger.debug("Loading samples from a pedigree file");

        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(samplesCommandOptions.loadCommandOptions.fileId)) {
            objectMap.put("fileId", samplesCommandOptions.loadCommandOptions.fileId);
        }

        if (StringUtils.isNotEmpty(samplesCommandOptions.loadCommandOptions.variableSetId)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), samplesCommandOptions.loadCommandOptions.variableSetId);
        }

        QueryResponse<Sample> sample = openCGAClient.getSampleClient()
                .loadFromPed(samplesCommandOptions.loadCommandOptions.studyId, objectMap);

        System.out.println(sample.toString());
    }

    private void info() throws CatalogException, IOException  {
        logger.debug("Getting samples information");
        QueryOptions queryOptions = new QueryOptions();
        if (StringUtils.isNotEmpty(samplesCommandOptions.infoCommandOptions.commonOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, samplesCommandOptions.infoCommandOptions.commonOptions.include);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.infoCommandOptions.commonOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, samplesCommandOptions.infoCommandOptions.commonOptions.exclude);
        }

        QueryResponse<Sample> samples = openCGAClient.getSampleClient().get(samplesCommandOptions.infoCommandOptions.id, queryOptions);
        samples.first().getResult().stream().forEach(sample -> System.out.println(sample.toString()));
    }

    private void search() throws CatalogException, IOException  {
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
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.commonOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, samplesCommandOptions.searchCommandOptions.commonOptions.include);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.commonOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, samplesCommandOptions.searchCommandOptions.commonOptions.exclude);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.limit)) {
            queryOptions.put(QueryOptions.LIMIT, samplesCommandOptions.searchCommandOptions.limit);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.searchCommandOptions.skip)) {
            queryOptions.put(QueryOptions.SKIP, samplesCommandOptions.searchCommandOptions.skip);
        }

        queryOptions.put("count", samplesCommandOptions.searchCommandOptions.count);

        QueryResponse<Sample> samples = openCGAClient.getSampleClient().search(query, queryOptions);
        samples.first().getResult().stream().forEach(sample -> System.out.println(sample.toString()));
    }

    private void update() throws CatalogException, IOException {
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

        QueryResponse<Sample> samples = openCGAClient.getSampleClient().update(samplesCommandOptions.updateCommandOptions.id, objectMap);
        samples.first().getResult().stream().forEach(sample -> System.out.println(sample.toString()));
    }

    private void delete() throws CatalogException, IOException {
        logger.debug("Deleting the select sample");

        ObjectMap objectMap = new ObjectMap();
        QueryResponse<Sample> samples = openCGAClient.getSampleClient().delete(samplesCommandOptions.deleteCommandOptions.id, objectMap);
        System.out.println(samples.toString());
    }

    private void groupBy() throws CatalogException, IOException {
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
        QueryResponse<Sample> samples = openCGAClient.getSampleClient().groupBy(samplesCommandOptions.groupByCommandOptions.studyId,
                samplesCommandOptions.groupByCommandOptions.by,objectMap);
        samples.first().getResult().stream().forEach(sample -> System.out.println(sample.toString()));
    }

    /********************************************  Annotation commands  ***********************************************/

    private void annotationSetsAllInfo() throws CatalogException, IOException {
        logger.debug("Searching annotationSets information");
        ObjectMap objectMap = new ObjectMap();

        objectMap.put("as-map", samplesCommandOptions.annotationSetsAllInfoCommandOptions.asMap);

        QueryResponse<Sample> sample = openCGAClient.getSampleClient()
                .annotationSetsAllInfo(samplesCommandOptions.annotationSetsAllInfoCommandOptions.id, objectMap);

        System.out.println(sample.toString());
    }

    private void annotationSetsInfo() throws CatalogException, IOException {
        logger.debug("Searching annotationSets information");
        ObjectMap objectMap = new ObjectMap();

        objectMap.put("asMap", samplesCommandOptions.annotationSetsInfoCommandOptions.asMap);

        if (StringUtils.isNotEmpty(samplesCommandOptions.annotationSetsInfoCommandOptions.annotationSetName)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION_SET_NAME.key(), samplesCommandOptions.annotationSetsInfoCommandOptions.annotationSetName);
        }

        if (StringUtils.isNotEmpty(samplesCommandOptions.annotationSetsInfoCommandOptions.variableSetId)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                    samplesCommandOptions.annotationSetsInfoCommandOptions.variableSetId);
        }

        QueryResponse<Sample> sample = openCGAClient.getSampleClient().annotationSetsInfo(samplesCommandOptions.annotationSetsInfoCommandOptions.id,
                samplesCommandOptions.annotationSetsInfoCommandOptions.annotationSetName,
                samplesCommandOptions.annotationSetsInfoCommandOptions.variableSetId, objectMap);

        System.out.println(sample.toString());
    }

    private void annotationSetsSearch() throws CatalogException, IOException {
        logger.debug("Searching annotationSets");
        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(samplesCommandOptions.annotationSetsInfoCommandOptions.annotation)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION.key(), samplesCommandOptions.annotationSetsInfoCommandOptions.annotation);
        }

        objectMap.put("as-map", samplesCommandOptions.annotationSetsAllInfoCommandOptions.asMap);

        QueryResponse<Sample> samples = openCGAClient.getSampleClient()
                .annotationSetsSearch(samplesCommandOptions.annotationSetsSearchCommandOptions.id,
                        samplesCommandOptions.annotationSetsSearchCommandOptions.annotationSetName,
                        samplesCommandOptions.annotationSetsSearchCommandOptions.variableSetId, objectMap);

        samples.first().getResult().stream().forEach(sample -> System.out.println(sample.toString()));
    }

    private void annotationSetsDelete() throws CatalogException, IOException {
        logger.debug("Searching annotationSets");
        ObjectMap objectMap = new ObjectMap();

        if (StringUtils.isNotEmpty(samplesCommandOptions.annotationSetsDeleteCommandOptions.annotation)) {
            objectMap.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION.key(),
                    samplesCommandOptions.annotationSetsDeleteCommandOptions.annotation);
        }
        QueryResponse<Sample> samples = openCGAClient.getSampleClient()
                .annotationSetsDelete(samplesCommandOptions.annotationSetsDeleteCommandOptions.id,
                        samplesCommandOptions.annotationSetsDeleteCommandOptions.annotationSetName,
                        samplesCommandOptions.annotationSetsDeleteCommandOptions.variableSetId, objectMap);

        samples.first().getResult().stream().forEach(sample -> System.out.println(sample.toString()));

    }
    /********************************************  Administration ACL commands  ***********************************************/

    private void acls() throws CatalogException,IOException {

        logger.debug("Acls");
        ObjectMap objectMap = new ObjectMap();
        QueryResponse<SampleAclEntry> acls = openCGAClient.getSampleClient().getAcls(samplesCommandOptions.aclsCommandOptions.id);

        System.out.println(acls.toString());

    }
    private void aclsCreate() throws CatalogException,IOException{

        logger.debug("Creating acl");

        QueryOptions queryOptions = new QueryOptions();

        /*if (StringUtils.isNotEmpty(studiesCommandOptions.aclsCreateCommandOptions.templateId)) {
            queryOptions.put(CatalogStudyDBAdaptor.QueryParams.TEMPLATE_ID.key(), studiesCommandOptions.aclsCreateCommandOptions.templateId);
        }*/

        QueryResponse<SampleAclEntry> acl =
                openCGAClient.getSampleClient().createAcl(samplesCommandOptions.aclsCreateCommandOptions.id,
                        samplesCommandOptions.aclsCreateCommandOptions.permissions, samplesCommandOptions.aclsCreateCommandOptions.members,
                        queryOptions);
        System.out.println(acl.toString());
    }
    private void aclMemberDelete() throws CatalogException,IOException {

        logger.debug("Creating acl");

        QueryOptions queryOptions = new QueryOptions();
        QueryResponse<Object> acl = openCGAClient.getSampleClient().deleteAcl(samplesCommandOptions.aclsMemberDeleteCommandOptions.id,
                samplesCommandOptions.aclsMemberDeleteCommandOptions.memberId, queryOptions);
        System.out.println(acl.toString());
    }
    private void aclMemberInfo() throws CatalogException,IOException {

        logger.debug("Creating acl");

        QueryResponse<SampleAclEntry> acls = openCGAClient.getSampleClient().getAcl(samplesCommandOptions.aclsMemberInfoCommandOptions.id,
                samplesCommandOptions.aclsMemberInfoCommandOptions.memberId);
        System.out.println(acls.toString());
    }

    private void aclMemberUpdate() throws CatalogException,IOException {

        logger.debug("Updating acl");

        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(samplesCommandOptions.aclsMemberUpdateCommandOptions.addPermissions)) {
            objectMap.put(SampleClient.AclParams.ADD_PERMISSIONS.key(), samplesCommandOptions.aclsMemberUpdateCommandOptions.addPermissions);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.aclsMemberUpdateCommandOptions.removePermissions)) {
            objectMap.put(SampleClient.AclParams.REMOVE_PERMISSIONS.key(), samplesCommandOptions.aclsMemberUpdateCommandOptions.removePermissions);
        }
        if (StringUtils.isNotEmpty(samplesCommandOptions.aclsMemberUpdateCommandOptions.setPermissions)) {
            objectMap.put(SampleClient.AclParams.SET_PERMISSIONS.key(), samplesCommandOptions.aclsMemberUpdateCommandOptions.setPermissions);
        }

        QueryResponse<SampleAclEntry> acl = openCGAClient.getSampleClient().updateAcl(samplesCommandOptions.aclsMemberUpdateCommandOptions.id,
                samplesCommandOptions.aclsMemberUpdateCommandOptions.memberId, objectMap);
        System.out.println(acl.toString());
    }

}
