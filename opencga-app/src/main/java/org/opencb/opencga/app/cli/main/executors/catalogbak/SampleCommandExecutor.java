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
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.SampleCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AnnotationCommandOptions;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclUpdateParams;
import org.opencb.opencga.core.models.sample.SampleCreateParams;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.response.RestResponse;

import java.io.File;
import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class SampleCommandExecutor extends OpencgaCommandExecutor {

    private SampleCommandOptions samplesCommandOptions;

    public SampleCommandExecutor(SampleCommandOptions samplesCommandOptions) {
        super(samplesCommandOptions.commonCommandOptions);
        this.samplesCommandOptions = samplesCommandOptions;
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
            case "stats":
                queryResponse = stats();
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

    private RestResponse<Sample> create() throws ClientException {
        logger.debug("Creating sample");

        SampleCommandOptions.CreateCommandOptions commandOptions = samplesCommandOptions.createCommandOptions;

        ObjectMap params = new ObjectMap(SampleDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);

        SampleCreateParams createParams = new SampleCreateParams()
                .setId(commandOptions.id)
                .setDescription(commandOptions.description)
                .setIndividualId(commandOptions.individual)
                .setSomatic(commandOptions.somatic);

        return openCGAClient.getSampleClient().create(createParams, params);
    }

    private RestResponse<Sample> load() throws ClientException {
        logger.debug("Loading samples from a pedigree file");

        SampleCommandOptions.LoadCommandOptions c = samplesCommandOptions.loadCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), c.study);
        params.putIfNotEmpty(Constants.VARIABLE_SET, c.variableSetId);

        return openCGAClient.getSampleClient().load(c.pedFile, params);
    }

    private RestResponse<Sample> info() throws ClientException  {
        logger.debug("Getting samples information");

        SampleCommandOptions.InfoCommandOptions c = samplesCommandOptions.infoCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotNull(SampleDBAdaptor.QueryParams.STUDY.key(), c.study);
        params.putIfNotEmpty(QueryOptions.INCLUDE, c.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, c.dataModelOptions.exclude);
        params.putIfNotNull(ParamConstants.FLATTEN_ANNOTATIONS, c.flattenAnnotations);
        params.putIfNotNull(ParamConstants.SAMPLE_VERSION_PARAM, c.version);
        params.putIfNotNull(ParamConstants.DELETED_PARAM, c.deleted);

        return openCGAClient.getSampleClient().info(c.sample, params);
    }

    private RestResponse<Sample> search() throws ClientException  {
        logger.debug("Searching samples");

        SampleCommandOptions.SearchCommandOptions c = samplesCommandOptions.searchCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), c.study);
        params.putIfNotEmpty(ParamConstants.SAMPLE_ID_PARAM, c.sampleId);
        params.putIfNotEmpty(ParamConstants.CREATION_DATE_PARAM, c.creationDate);
        params.putIfNotEmpty(ParamConstants.MODIFICATION_DATE_PARAM, c.modificationDate);
        params.putIfNotEmpty(ParamConstants.PHENOTYPES_PARAM, c.phenotypes);
        params.putIfNotEmpty(ParamConstants.ACL_PARAM, c.acl);
        params.putIfNotEmpty(ParamConstants.ATTRIBUTES_PARAM, c.attributes);
        params.putIfNotNull(ParamConstants.SAMPLE_SOMATIC_PARAM, c.somatic);
        params.putIfNotEmpty(ParamConstants.SAMPLE_INDIVIDUAL_ID_PARAM, c.individual);
        params.putIfNotEmpty(ParamConstants.SAMPLE_FILE_IDS_PARAM, c.fileIds);
        params.putIfNotEmpty(Constants.ANNOTATION, c.annotation);
        params.putIfNotNull(ParamConstants.DELETED_PARAM, c.deleted);
        params.putIfNotNull(ParamConstants.RELEASE_PARAM, c.release);
        params.putIfNotNull(ParamConstants.SNAPSHOT_PARAM, c.snapshot);
        params.putIfNotNull(ParamConstants.FLATTEN_ANNOTATIONS, c.flattenAnnotations);
        params.putAll(c.commonOptions.params);

        params.put(QueryOptions.COUNT, c.numericOptions.count);
        params.putIfNotEmpty(QueryOptions.INCLUDE, c.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, c.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, c.numericOptions.limit);
        params.put(QueryOptions.SKIP, c.numericOptions.skip);

        return openCGAClient.getSampleClient().search(params);
    }

    private RestResponse<Sample> update() throws ClientException {
        logger.debug("Updating samples");

        SampleCommandOptions.UpdateCommandOptions commandOptions = samplesCommandOptions.updateCommandOptions;

        SampleUpdateParams updateParams = new SampleUpdateParams()
                .setId(commandOptions.id)
                .setDescription(commandOptions.description)
                .setIndividualId(commandOptions.individual)
                .setSomatic(commandOptions.somatic);

        ObjectMap params = new ObjectMap(SampleDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);

        return openCGAClient.getSampleClient().update(commandOptions.sample, updateParams, params);
    }

    private RestResponse<Sample> delete() throws ClientException {
        logger.debug("Deleting the selected sample");

        SampleCommandOptions.DeleteCommandOptions c = samplesCommandOptions.deleteCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), c.study);
        params.putIfNotNull(Constants.FORCE, c.force);
        params.putIfNotEmpty(ParamConstants.SAMPLE_EMPTY_FILES_ACTION_PARAM, c.emptyFilesAction);
        params.putIfNotNull(ParamConstants.SAMPLE_DELETE_EMPTY_COHORTS_PARAM, c.deleteEmptyCohorts);

        return openCGAClient.getSampleClient().delete(c.sample, params);
    }

    private RestResponse<ObjectMap> updateAcl() throws CatalogException, ClientException {
        SampleCommandOptions.SampleAclCommandOptions.AclsUpdateCommandOptions commandOptions =
                samplesCommandOptions.aclsUpdateCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotNull("study", commandOptions.study);
        queryParams.put("propagate", commandOptions.propagate);

        SampleAclUpdateParams updateParams = new SampleAclUpdateParams()
                .setSample(extractIdsFromListOrFile(commandOptions.id))
                .setIndividual(extractIdsFromListOrFile(commandOptions.individual))
                .setCohort(extractIdsFromListOrFile(commandOptions.cohort))
                .setFile(extractIdsFromListOrFile(commandOptions.file))
                .setPermissions(commandOptions.permissions);

        return openCGAClient.getSampleClient().updateAcl(commandOptions.memberId, commandOptions.action.name(), updateParams, queryParams);
    }

    private RestResponse<FacetField> stats() throws ClientException {
        logger.debug("Individual stats");

        SampleCommandOptions.StatsCommandOptions commandOptions = samplesCommandOptions.statsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(SampleDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty("creationYear", commandOptions.creationYear);
        params.putIfNotEmpty("creationMonth", commandOptions.creationMonth);
        params.putIfNotEmpty("creationDay", commandOptions.creationDay);
        params.putIfNotEmpty("creationDayOfWeek", commandOptions.creationDayOfWeek);
        params.putIfNotEmpty("status", commandOptions.status);
        params.putIfNotEmpty("source", commandOptions.source);
        params.putIfNotEmpty("type", commandOptions.type);
        params.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        params.putIfNotEmpty("release", commandOptions.release);
        params.putIfNotEmpty("version", commandOptions.version);
        params.putIfNotNull("somatic", commandOptions.somatic);
        params.putIfNotEmpty(Constants.ANNOTATION, commandOptions.annotation);

        params.put("default", commandOptions.defaultStats);
        params.putIfNotNull("field", commandOptions.field);

        return openCGAClient.getSampleClient().aggregationStats(params);
    }

    private RestResponse<Sample> updateAnnotations() throws ClientException, IOException {
        AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions commandOptions = samplesCommandOptions.annotationUpdateCommandOptions;

        ObjectMapper mapper = new ObjectMapper();
        ObjectMap annotations = mapper.readValue(new File(commandOptions.annotations), ObjectMap.class);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
//        queryParams.putIfNotNull("action", updateCommandOptions.action);

        return openCGAClient.getSampleClient().updateAnnotations(commandOptions.id, commandOptions.annotationSetId, annotations, params);
    }

    private RestResponse<ObjectMap> acl() throws ClientException {
        AclCommandOptions.AclsCommandOptions commandOptions = samplesCommandOptions.aclsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
        params.putIfNotEmpty("member", commandOptions.memberId);

        params.putAll(commandOptions.commonOptions.params);

        return openCGAClient.getSampleClient().acl(commandOptions.id, params);
    }

}
