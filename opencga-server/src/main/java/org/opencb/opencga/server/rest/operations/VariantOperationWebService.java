/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.server.rest.operations;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.operations.*;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.operations.variant.*;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.tools.ToolParams;
import org.opencb.opencga.server.rest.OpenCGAWSServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;

/**
 * Created by imedina on 17/08/16.
 */
@Path("/{apiVersion}/operation")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Operations - Variant Storage", description = "Internal operations for the variant storage engine")
public class VariantOperationWebService extends OpenCGAWSServer {

    public VariantOperationWebService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public VariantOperationWebService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }

    @POST
    @Path("/variant/configure")
    @ApiOperation(value = VariantSecondaryIndexOperationTool.DESCRIPTION, response = ObjectMap.class)
    public Response secondaryIndex(
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
//            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = "Configuration params to update") ObjectMap params) {
        return run(() -> {
            ObjectMap newConfiguration;
            StopWatch stopWatch = StopWatch.createStarted();
//            if (StringUtils.isNotEmpty(study)) {
//                variantManager.configureStudy(study, params, token);
//            } else {
            newConfiguration = variantManager.configureProject(project, params, token);
//            }
            return new DataResult<>()
                    .setResults(Collections.singletonList(newConfiguration))
                    .setNumResults(1)
                    .setTime(((int) stopWatch.getTime(TimeUnit.MILLISECONDS)));
        });
    }

    @POST
    @Path("/variant/secondaryIndex")
    @ApiOperation(value = VariantSecondaryIndexOperationTool.DESCRIPTION, response = Job.class)
    public Response secondaryIndex(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = VariantCatalogQueryUtils.PROJECT_DESC) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = VariantSecondaryIndexParams.DESCRIPTION) VariantSecondaryIndexParams params) {
        return submitOperation(VariantSecondaryIndexOperationTool.ID, project, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @DELETE
    @Path("/variant/secondaryIndex/delete")
    @ApiOperation(value = VariantSecondaryIndexSamplesDeleteOperationTool.DESCRIPTION, response = Job.class)
    public Response secondaryIndex(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = "Samples to remove. Needs to provide all the samples in the secondary index.") @QueryParam("samples") String samples) {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ParamConstants.STUDY_PARAM, study);
        params.put("samples", samples);
        return submitOperation(VariantSecondaryIndexSamplesDeleteOperationTool.ID, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/variant/annotation/index")
    @ApiOperation(value = VariantAnnotationIndexOperationTool.DESCRIPTION, response = Job.class)
    public Response annotation(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = VariantAnnotationIndexParams.DESCRIPTION) VariantAnnotationIndexParams params) {
        return submitOperation(VariantAnnotationIndexOperationTool.ID, project, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @DELETE
    @Path("/variant/annotation/delete")
    @ApiOperation(value = VariantAnnotationDeleteOperationTool.DESCRIPTION, response = Job.class)
    public Response annotationDelete(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = VariantCatalogQueryUtils.PROJECT_DESC) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            @ApiParam(value = "Annotation identifier") @QueryParam("annotationId") String annotationId
    ) {
        Map<String, Object> params = new HashMap<>();
        params.put(ParamConstants.PROJECT_PARAM, project);
        params.put("annotationId", annotationId);
        return submitOperationToProject(VariantAnnotationDeleteOperationTool.ID, project, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/variant/annotation/save")
    @ApiOperation(value = VariantAnnotationSaveOperationTool.DESCRIPTION, response = Job.class)
    public Response annotationSave(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = VariantCatalogQueryUtils.PROJECT_DESC) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            @ApiParam(value = VariantAnnotationSaveParams.DESCRIPTION) VariantAnnotationSaveParams params) {
        return submitOperationToProject(VariantAnnotationSaveOperationTool.ID, project, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/variant/score/index")
    @ApiOperation(value = VariantScoreIndexOperationTool.DESCRIPTION, response = Job.class)
    public Response scoreIndex(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = VariantScoreIndexParams.DESCRIPTION) VariantScoreIndexParams params) {
        return submitOperation(VariantScoreIndexOperationTool.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @DELETE
    @Path("/variant/score/delete")
    @ApiOperation(value = VariantScoreDeleteParams.DESCRIPTION, response = Job.class)
    public Response scoreDelete(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = "Unique name of the score within the study") @QueryParam("name") String name,
            @ApiParam(value = "Resume a previously failed remove") @QueryParam("resume") boolean resume,
            @ApiParam(value = "Force remove of partially indexed scores") @QueryParam("force") boolean force
    ) {
        Map<String, Object> params = new HashMap<>();
        params.put(ParamConstants.STUDY_PARAM, study);
        params.put("name", name);
        if (resume) params.put("resume", "");
        if (force) params.put("force", "");
        return submitOperation(VariantScoreDeleteParams.ID, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/variant/sample/genotype/index")
    @ApiOperation(value = VariantSampleIndexOperationTool.DESCRIPTION, response = Job.class)
    public Response sampleIndex(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = VariantSampleIndexParams.DESCRIPTION) VariantSampleIndexParams params) {
        return submitOperation(VariantSampleIndexOperationTool.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/variant/family/genotype/index")
    @ApiOperation(value = VariantFamilyIndexOperationTool.DESCRIPTION, response = Job.class)
    public Response familyIndex(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = VariantFamilyIndexParams.DESCRIPTION) VariantFamilyIndexParams params) {
        return submitOperation(VariantFamilyIndexOperationTool.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/variant/family/aggregate")
    @ApiOperation(value = VariantAggregateFamilyOperationTool.DESCRIPTION, response = Job.class)
    public Response aggregateFamily(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = VariantAggregateFamilyParams.DESCRIPTION) VariantAggregateFamilyParams params) {
        return submitOperation(VariantAggregateFamilyOperationTool.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/variant/aggregate")
    @ApiOperation(value = VariantAggregateOperationTool.DESCRIPTION, response = Job.class)
    public Response aggregate(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = VariantAggregateParams.DESCRIPTION) VariantAggregateParams params) {
        return submitOperation(VariantAggregateOperationTool.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    public Response submitOperation(String toolId, String study, ToolParams params,
                                    String jobName, String jobDescription, String jobDependsOn, String jobTags) {
        return submitOperation(toolId, null, study, params, jobName, jobDescription, jobDependsOn, jobTags);
    }

    public Response submitOperationToProject(String toolId, String project, ToolParams params, String jobName, String jobDescription,
                                             String jobDependsOn, String jobTags) {
        return submitOperation(toolId, project, null, params, jobName, jobDescription, jobDependsOn, jobTags);
    }

    public Response submitOperation(String toolId, String project, String study, ToolParams params, String jobName, String jobDescription,
                                    String jobDependsOn, String jobTags) {
        try {
            Map<String, Object> paramsMap = params.toParams();
            if (StringUtils.isNotEmpty(study)) {
                paramsMap.put(ParamConstants.STUDY_PARAM, study);
            }
            if (StringUtils.isNotEmpty(project)) {
                paramsMap.put(ParamConstants.PROJECT_PARAM, project);
            }
            return submitOperation(toolId, project, study, paramsMap, jobName, jobDescription, jobDependsOn, jobTags);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public Response submitOperationToProject(String toolId, String project, Map<String, Object> paramsMap, String jobName,
                                             String jobDescription, String jobDependsOn, String jobTags) {
        return submitOperation(toolId, project, null, paramsMap, jobName, jobDescription, jobDependsOn, jobTags);
    }

    public Response submitOperation(String toolId, Map<String, Object> paramsMap, String jobName, String jobDescription,
                                    String jobDependsOn, String jobTags) {
        String project = (String) paramsMap.get(ParamConstants.PROJECT_PARAM);
        String study = (String) paramsMap.get(ParamConstants.STUDY_PARAM);
        return submitOperation(toolId, project, study, paramsMap, jobName, jobDescription, jobDependsOn, jobTags);
    }

    public Response submitOperation(String toolId, String project, String study, Map<String, Object> paramsMap, String jobName,
                                    String jobDescription, String jobDependsOne, String jobTags) {
        Map<String, String> dynamicParamsMap = new HashMap<>();
        for (String key : this.params.keySet()) {
            String prefix = "dynamic_";
            if (key.startsWith(prefix)) {
                dynamicParamsMap.put(key.replace(prefix, ""), this.params.getString(key));
            }
        }
        if (dynamicParamsMap.size() > 0) {
            paramsMap.put("dynamicParams", dynamicParamsMap);
        }
        return run(() -> {
            if (StringUtils.isNotEmpty(project) && StringUtils.isEmpty(study)) {
                // Project job
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.FQN.key());
                // Peek any study. The ExecutionDaemon will take care of filling up the rest of studies.
                List<String> studies = catalogManager.getStudyManager()
                        .get(project, new Query(), options, token)
                        .getResults()
                        .stream()
                        .map(Study::getFqn)
                        .collect(Collectors.toList());
                if (studies.isEmpty()) {
                    throw new CatalogException("Project '" + project + "' not found!");
                }
                return submitJobRaw(toolId, studies.get(0), paramsMap, jobName, jobDescription, jobDependsOne, jobTags);
            } else {
                return submitJobRaw(toolId, study, paramsMap, jobName, jobDescription, jobDependsOne, jobTags);
            }
        });
    }
}