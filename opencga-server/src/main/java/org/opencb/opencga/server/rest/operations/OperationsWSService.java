package org.opencb.opencga.server.rest.operations;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import org.opencb.opencga.server.rest.analysis.RestBodyParams;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;

import static org.opencb.opencga.server.rest.analysis.AnalysisWSService.*;

/**
 * Created by imedina on 17/08/16.
 */
@Path("/{apiVersion}/operation")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Operations", description = "General operations for catalog and storage")
public class OperationsWSService extends OpenCGAWSServer {

    public OperationsWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public OperationsWSService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }

    public static class SecondaryIndexParams extends RestBodyParams {
        public String study;
        public String project;
        public String region;
        public String sample;
        public String cohort;
        public boolean overwrite;
    }

    @POST
    @Path("/variant/secondaryIndex/run")
    @ApiOperation(value = "Creates a secondary index using a search engine. "
            + "If samples are provided, sample data will be added to the secondary index.")
    public Response secondaryIndex(
            @ApiParam(value = JOB_ID_DESCRIPTION) @QueryParam(JOB_ID) String jobId,
            @ApiParam(value = JOB_NAME_DESCRIPTION) @QueryParam(JOB_NAME) String jobName,
            @ApiParam(value = JOB_DESCRIPTION_DESCRIPTION) @QueryParam(JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = JOB_TAGS_DESCRIPTION) @QueryParam(JOB_TAGS) List<String> jobTags,
            SecondaryIndexParams params) {
        return submitJob(params.study, "variant", "secondary-index", params, jobId, jobName, jobDescription, jobTags);
    }

    public static class SecondaryIndexRemoveParams extends RestBodyParams {
        public String study;
        public String sample;
    }

    @POST
    @Path("/variant/secondaryIndex/remove")
    @ApiOperation(value = "Remove a secondary index from the search engine for a specific set of samples.")
    public Response secondaryIndex(
            @ApiParam(value = JOB_ID_DESCRIPTION) @QueryParam(JOB_ID) String jobId,
            @ApiParam(value = JOB_NAME_DESCRIPTION) @QueryParam(JOB_NAME) String jobName,
            @ApiParam(value = JOB_DESCRIPTION_DESCRIPTION) @QueryParam(JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = JOB_TAGS_DESCRIPTION) @QueryParam(JOB_TAGS) List<String> jobTags,
            SecondaryIndexRemoveParams params) {
        return submitJob(params.study, "variant", "secondary-index", params, jobId, jobName, jobDescription, jobTags);
    }

    public static class AnnotationRunParams extends RestBodyParams {
        public String study;
        public String outdir;
        public String annotator;
        public String overwriteAnnotations;
        public String region;
        public boolean create;
        public String load;
        public String customName;
    }

    @POST
    @Path("/variant/annotation/run")
    @ApiOperation(value = "Create and load variant annotations into the database", response = Job.class)
    public Response annotationRun(
            @ApiParam(value = JOB_ID_DESCRIPTION) @QueryParam(JOB_ID) String jobId,
            @ApiParam(value = JOB_NAME_DESCRIPTION) @QueryParam(JOB_NAME) String jobName,
            @ApiParam(value = JOB_DESCRIPTION_DESCRIPTION) @QueryParam(JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = JOB_TAGS_DESCRIPTION) @QueryParam(JOB_TAGS) List<String> jobTags,
            AnnotationRunParams params) {
        return submitJob(params.study, "variant", "annotate", params, jobId, jobName, jobDescription, jobTags);
    }

    public static class AnnotationDeleteParams extends RestBodyParams {
        public String study;
        //        @JsonProperty("annotation-id")
        public String annotationId;
    }

    @DELETE
    @Path("/variant/annotation/delete")
    @ApiOperation(value = "Deletes a saved copy of variant annotation")
    public Response annotationDelete(
            @ApiParam(value = JOB_ID_DESCRIPTION) @QueryParam(JOB_ID) String jobId,
            @ApiParam(value = JOB_NAME_DESCRIPTION) @QueryParam(JOB_NAME) String jobName,
            @ApiParam(value = JOB_DESCRIPTION_DESCRIPTION) @QueryParam(JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = JOB_TAGS_DESCRIPTION) @QueryParam(JOB_TAGS) List<String> jobTags,
            AnnotationDeleteParams params) {
        return submitJob(params.study, "variant", "annotation-delete", params, jobId, jobName, jobDescription, jobTags);
    }

    public static class AnnotationSaveParams extends RestBodyParams {
        public String study;
        //        @JsonProperty("annotation-id")
        public String annotationId;
    }

    @POST
    @Path("/variant/annotation/save")
    @ApiOperation(value = "Save a copy of the current variant annotation at the database")
    public Response annotationSave(
            @ApiParam(value = JOB_ID_DESCRIPTION) @QueryParam(JOB_ID) String jobId,
            @ApiParam(value = JOB_NAME_DESCRIPTION) @QueryParam(JOB_NAME) String jobName,
            @ApiParam(value = JOB_DESCRIPTION_DESCRIPTION) @QueryParam(JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = JOB_TAGS_DESCRIPTION) @QueryParam(JOB_TAGS) List<String> jobTags,
            AnnotationSaveParams params) {
        return submitJob(params.study, "variant", "annotation-save", params, jobId, jobName, jobDescription, jobTags);
    }

    public static class ScoreIndexParams extends RestBodyParams {
        public String study;
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
            @ApiParam(value = JOB_ID_DESCRIPTION) @QueryParam(JOB_ID) String jobId,
            @ApiParam(value = JOB_NAME_DESCRIPTION) @QueryParam(JOB_NAME) String jobName,
            @ApiParam(value = JOB_DESCRIPTION_DESCRIPTION) @QueryParam(JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = JOB_TAGS_DESCRIPTION) @QueryParam(JOB_TAGS) List<String> jobTags,
            ScoreIndexParams params) {
        return submitJob(params.study, "variant", "score-index", params, jobId, jobName, jobDescription, jobTags);
    }

    public static class ScoreDeleteParams extends RestBodyParams {
        public String study;
        public String name;
        public boolean resume;
        public boolean force;
    }

    @DELETE
    @Path("/variant/score/delete")
    @ApiOperation(value = "Remove a variant score in the database.")
    public Response scoreDelete(
            @ApiParam(value = JOB_ID_DESCRIPTION) @QueryParam(JOB_ID) String jobId,
            @ApiParam(value = JOB_NAME_DESCRIPTION) @QueryParam(JOB_NAME) String jobName,
            @ApiParam(value = JOB_DESCRIPTION_DESCRIPTION) @QueryParam(JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = JOB_TAGS_DESCRIPTION) @QueryParam(JOB_TAGS) List<String> jobTags,
            ScoreDeleteParams params) {
        return submitJob(params.study, "variant", "score-remove", params, jobId, jobName, jobDescription, jobTags);
    }


    public static class FamilyIndexParams extends RestBodyParams {
        public String study;
        public List<String> family;
        public boolean overwrite;
    }

    @POST
    @Path("/variant/family/indexGenotype")
    @ApiOperation(value = "Build the family index.", response = Job.class)
    public Response familyIndex(
            @ApiParam(value = JOB_ID_DESCRIPTION) @QueryParam(JOB_ID) String jobId,
            @ApiParam(value = JOB_NAME_DESCRIPTION) @QueryParam(JOB_NAME) String jobName,
            @ApiParam(value = JOB_DESCRIPTION_DESCRIPTION) @QueryParam(JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = JOB_TAGS_DESCRIPTION) @QueryParam(JOB_TAGS) List<String> jobTags,
            FamilyIndexParams params) {
        return submitJob(params.study, "variant", "family-index", params, jobId, jobName, jobDescription, jobTags);
    }
    public static class SampleIndexParams extends RestBodyParams {
        public String study;
        public List<String> sample;
    }

    @POST
    @Path("/variant/sample/indexGenotype")
    @ApiOperation(value = "Build and annotate the sample index.", response = Job.class)
    public Response sampleIndex(
            @ApiParam(value = JOB_ID_DESCRIPTION) @QueryParam(JOB_ID) String jobId,
            @ApiParam(value = JOB_NAME_DESCRIPTION) @QueryParam(JOB_NAME) String jobName,
            @ApiParam(value = JOB_DESCRIPTION_DESCRIPTION) @QueryParam(JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = JOB_TAGS_DESCRIPTION) @QueryParam(JOB_TAGS) List<String> jobTags,
            SampleIndexParams params) {
        return submitJob(params.study, "variant", "sample-index", params, jobId, jobName, jobDescription, jobTags);
    }

    public static class AggregateParams extends RestBodyParams {
        public String study;
        public String region;
        public boolean overwrite;
        public boolean resume;
    }

    @POST
    @Path("/variant/aggregate")
    @ApiOperation(value = "Find variants where not all the samples are present, and fill the empty values, excluding HOM-REF (0/0) values.")
    public Response aggregate(
            @ApiParam(value = JOB_ID_DESCRIPTION) @QueryParam(JOB_ID) String jobId,
            @ApiParam(value = JOB_NAME_DESCRIPTION) @QueryParam(JOB_NAME) String jobName,
            @ApiParam(value = JOB_DESCRIPTION_DESCRIPTION) @QueryParam(JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = JOB_TAGS_DESCRIPTION) @QueryParam(JOB_TAGS) List<String> jobTags,
            AggregateParams params) {
        return submitJob(params.study, "variant", "aggregate", params, jobId, jobName, jobDescription, jobTags);
    }

    public static class AggregateFamilyParams extends RestBodyParams {
        public String study;
        public boolean resume;
        public String samples;
    }

    @POST
    @Path("/variant/aggregateFamily")
    @ApiOperation(value = "Find variants where not all the samples are present, and fill the empty values.")
    public Response aggregateFamily(
            @ApiParam(value = JOB_ID_DESCRIPTION) @QueryParam(JOB_ID) String jobId,
            @ApiParam(value = JOB_NAME_DESCRIPTION) @QueryParam(JOB_NAME) String jobName,
            @ApiParam(value = JOB_DESCRIPTION_DESCRIPTION) @QueryParam(JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = JOB_TAGS_DESCRIPTION) @QueryParam(JOB_TAGS) List<String> jobTags,
            AggregateFamilyParams params) {
        return submitJob(params.study, "variant", "aggregate-family", params, jobId, jobName, jobDescription, jobTags);
    }

}