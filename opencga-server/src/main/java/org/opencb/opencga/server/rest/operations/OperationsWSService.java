package org.opencb.opencga.server.rest.operations;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import org.opencb.opencga.server.rest.analysis.RestBodyParams;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 17/08/16.
 */
@Path("/{apiVersion}/operation")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Operations - Variant Storage", description = "Internal operations for the variant storage engine")
public class OperationsWSService extends OpenCGAWSServer {

    public OperationsWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public OperationsWSService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }

    @DELETE
    @Path("/variant/file/delete")
    @ApiOperation(value = "Remove variant files from the variant storage", response = Job.class)
    public Response variantFileDelete(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = "Files to remove") @QueryParam("file") String file,
            @ApiParam(value = "Resume a previously failed indexation") @QueryParam("resume") boolean resume) {
        HashMap<String, Object> paramsMap = new HashMap<>();
        paramsMap.put(ParamConstants.STUDY_PARAM, study);
        paramsMap.put("file", file);
        if (resume) {
            paramsMap.put("resume", "");
        }
        return submitOperation("variant-delete", paramsMap, jobName, jobDescription, jobTags);
    }

    public static class VariantSecondaryIndexParams extends RestBodyParams {
        public VariantSecondaryIndexParams() {
        }
        public VariantSecondaryIndexParams(String region, String sample, boolean overwrite) {
            this.region = region;
            this.sample = sample;
            this.overwrite = overwrite;
        }
        public String region;
        public String sample;
        public boolean overwrite;
    }

    @POST
    @Path("/variant/secondaryIndex")
    @ApiOperation(value = "Creates a secondary index using a search engine. "
            + "If samples are provided, sample data will be added to the secondary index.")
    public Response secondaryIndex(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = VariantCatalogQueryUtils.PROJECT_DESC) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            VariantSecondaryIndexParams params) {
        return submitOperation("variant-secondary-index", project, study, params, jobName, jobDescription, jobTags);
    }

    @DELETE
    @Path("/variant/secondaryIndex/delete")
    @ApiOperation(value = "Remove a secondary index from the search engine for a specific set of samples.")
    public Response secondaryIndex(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = "Samples to remove. Needs to provide all the samples in the secondary index.") @QueryParam("samples") String samples) {
        HashMap<String, Object> params = new HashMap<>();
        params.put(ParamConstants.STUDY_PARAM, study);
        params.put("samples", samples);
        return submitOperation("variant-secondary-index-delete", params, jobName, jobDescription, jobTags);
    }

    public static class VariantAnnotationParams extends RestBodyParams {
        public VariantAnnotationParams() {
        }

        public VariantAnnotationParams(String outdir, VariantAnnotatorFactory.AnnotationEngine annotator,
                                       boolean overwriteAnnotations, String region, boolean create, String load, String customName) {
            this.outdir = outdir;
            this.annotator = annotator;
            this.overwriteAnnotations = overwriteAnnotations;
            this.region = region;
            this.create = create;
            this.load = load;
            this.customName = customName;
        }
        public String outdir;
        public VariantAnnotatorFactory.AnnotationEngine annotator;
        public boolean overwriteAnnotations;
        public String region;
        public boolean create;
        public String load;
        public String customName;
    }

    @POST
    @Path("/variant/annotation/index")
    @ApiOperation(value = "Create and load variant annotations into the database", response = Job.class)
    public Response annotation(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            VariantAnnotationParams params) {
        return submitOperation("variant-annotation-index", project, study, params, jobName, jobDescription, jobTags);
    }

    @DELETE
    @Path("/variant/annotation/delete")
    @ApiOperation(value = "Deletes a saved copy of variant annotation")
    public Response annotationDelete(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = VariantCatalogQueryUtils.PROJECT_DESC) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            @ApiParam(value = "Annotation identifier") @QueryParam("annotationId") String annotationId
    ) {
        Map<String, Object> params = new HashMap<>();
        params.put(ParamConstants.PROJECT_PARAM, project);
        params.put("annotationId", annotationId);
        return submitOperationToProject("variant-annotation-delete", project, params, jobName, jobDescription, jobTags);
    }

    public static class VariantAnnotationSaveParams extends RestBodyParams {
        public VariantAnnotationSaveParams() {
        }

        public VariantAnnotationSaveParams(String annotationId) {
            this.annotationId = annotationId;
        }

        public String annotationId;
    }

    @POST
    @Path("/variant/annotation/save")
    @ApiOperation(value = "Save a copy of the current variant annotation at the database")
    public Response annotationSave(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = VariantCatalogQueryUtils.PROJECT_DESC) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            VariantAnnotationSaveParams params) {
        return submitOperationToProject("variant-annotation-save", project, params, jobName, jobDescription, jobTags);
    }

    public static class VariantScoreIndexParams extends RestBodyParams {
        public VariantScoreIndexParams() {
        }
        public VariantScoreIndexParams(String cohort1, String cohort2, String input, String inputColumns,
                                       boolean resume) {
            this.cohort1 = cohort1;
            this.cohort2 = cohort2;
            this.input = input;
            this.inputColumns = inputColumns;
            this.resume = resume;
        }

        public String cohort1;
        public String cohort2;
        public String input;
        public String inputColumns;
        public boolean resume;
    }

    @POST
    @Path("/variant/score/index")
    @ApiOperation(value = "Index a variant score in the database.")
    public Response scoreIndex(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            VariantScoreIndexParams params) {
        return submitOperation("variant-score-index", study, params, jobName, jobDescription, jobTags);
    }

    @DELETE
    @Path("/variant/score/delete")
    @ApiOperation(value = "Remove a variant score in the database.")
    public Response scoreDelete(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
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
        return submitOperation("variant-score-delete", params, jobName, jobDescription, jobTags);
    }

    public static class VariantSampleIndexParams extends RestBodyParams {
        public VariantSampleIndexParams() {
        }
        public VariantSampleIndexParams(List<String> sample) {
            this.sample = sample;
        }
        public List<String> sample;
    }

    @POST
    @Path("/variant/sample/genotype/index")
    @ApiOperation(value = "Build and annotate the sample index.", response = Job.class)
    public Response sampleIndex(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            VariantSampleIndexParams params) {
        return submitOperation("variant-sample-index", study, params, jobName, jobDescription, jobTags);
    }

    public static class VariantFamilyIndexParams extends RestBodyParams {
        public VariantFamilyIndexParams() {
        }
        public VariantFamilyIndexParams(List<String> family, boolean overwrite) {
            this.family = family;
            this.overwrite = overwrite;
        }
        public List<String> family;
        public boolean overwrite;
    }

    @POST
    @Path("/variant/family/genotype/index")
    @ApiOperation(value = "Build the family index.", response = Job.class)
    public Response familyIndex(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            VariantFamilyIndexParams params) {
        return submitOperation("variant-family-index", study, params, jobName, jobDescription, jobTags);
    }

    public static class VariantAggregateFamilyParams extends RestBodyParams {
        public VariantAggregateFamilyParams() {
        }
        public VariantAggregateFamilyParams(boolean resume, List<String> samples) {
            this.resume = resume;
            this.samples = samples;
        }
        public boolean resume;
        public List<String> samples;
    }

    @POST
    @Path("/variant/family/aggregate")
    @ApiOperation(value = "Find variants where not all the samples are present, and fill the empty values.")
    public Response aggregateFamily(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            VariantAggregateFamilyParams params) {
        return submitOperation("variant-aggregate-family", study, params, jobName, jobDescription, jobTags);
    }

    public static class VariantAggregateParams extends RestBodyParams {
        public VariantAggregateParams() {
        }
        public VariantAggregateParams(String region, boolean overwrite, boolean resume) {
            this.region = region;
            this.overwrite = overwrite;
            this.resume = resume;
        }
        public String region;
        public boolean overwrite;
        public boolean resume;
    }

    @POST
    @Path("/variant/aggregate")
    @ApiOperation(value = "Find variants where not all the samples are present, and fill the empty values, excluding HOM-REF (0/0) values.")
    public Response aggregate(
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            VariantAggregateParams params) {
        return submitOperation("variant-aggregate", study, params, jobName, jobDescription, jobTags);
    }

    public Response submitOperation(String toolId, RestBodyParams params, String jobName, String jobDescription, String jobTags) {
        return submitOperation(toolId, null, params, jobName, jobDescription, jobTags);
    }

    public Response submitOperation(String toolId, String study, RestBodyParams params,
                               String jobName, String jobDescription, String jobTags) {
        return submitOperation(toolId, null, study, params, jobName, jobDescription, jobTags);
    }

    public Response submitOperationToProject(String toolId, String project, RestBodyParams params,
                               String jobName, String jobDescription, String jobTags) {
        return submitOperation(toolId, project, null, params, jobName, jobDescription, jobTags);
    }

    public Response submitOperation(String toolId, String project, String study, RestBodyParams params, String jobName, String jobDescription, String jobTags) {
        try {
            Map<String, Object> paramsMap = params.toParams();
            if (StringUtils.isNotEmpty(study)) {
                paramsMap.put(ParamConstants.STUDY_PARAM, study);
            }
            if (StringUtils.isNotEmpty(project)) {
                paramsMap.put(ParamConstants.PROJECT_PARAM, project);
            }
            return submitOperation(toolId, paramsMap, jobName, jobDescription, jobTags);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public Response submitOperationToProject(String toolId, String project, Map<String, Object> paramsMap,
                               String jobName, String jobDescription, String jobTags) {
        return submitOperation(toolId, project, null, paramsMap, jobName, jobDescription, jobTags);
    }

    public Response submitOperation(String toolId, Map<String, Object> paramsMap,
                               String jobName, String jobDescription, String jobTags) {
        String project = (String) paramsMap.get(ParamConstants.PROJECT_PARAM);
        String study = (String) paramsMap.get(ParamConstants.STUDY_PARAM);
        return submitOperation(toolId, project, study, paramsMap, jobName, jobDescription, jobTags);
    }

    public Response submitOperation(String toolId, String project, String study, Map<String, Object> paramsMap, String jobName, String jobDescription, String jobTags) {
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
        if (StringUtils.isEmpty(study) && StringUtils.isEmpty(project)) {
            // General job
            // FIXME
            return createPendingResponse();
        } else if (StringUtils.isNotEmpty(project)) {
            // Project job
            // FIXME
            return createPendingResponse();
        } else {
            return submitJob(toolId, study, paramsMap, null, jobName, jobDescription, jobTags);
        }
    }
}