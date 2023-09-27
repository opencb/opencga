package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.cvdb.tasks.CvdbIndexTask;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.params.CvdbIndexTaskParams;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalAnalysisLoadTask;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisLoadParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.tools.annotations.*;
import org.opencb.opencga.server.rest.OpenCGAWSServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;

import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;

@Path("/{apiVersion}/cvdb")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Cvdb", description = "Methods for working with CVDB endpoints")
public class CvdbWSServer extends OpenCGAWSServer {

    public CvdbWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @GET
    @Path("/{caseId}/info")
    @ApiOperation(value = "Get sample information", response = Sample.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, format = "", example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType =
                    "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.SAMPLE_INCLUDE_INDIVIDUAL_PARAM, value =
                    ParamConstants.SAMPLE_INCLUDE_INDIVIDUAL_DESCRIPTION,
                    defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response infoSample(
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION, required = true) @PathParam("caseId") String samplesStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.SAMPLE_VERSION_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_VERSION_PARAM) String version,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("samples");

            return createOkResponse("Pajote!");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/case/index/run")
    @ApiOperation(value = CvdbIndexTask.DESCRIPTION, response = Job.class)
    public Response load(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobId,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = CvdbIndexTaskParams.DESCRIPTION, required = true) CvdbIndexTaskParams params) {
        try {
            // Execute CVDB index as a job
            return submitJob(CvdbIndexTask.ID, study, params, jobId, jobDescription, dependsOn, jobTags);
        } catch (Exception e) {
            return createErrorResponse(CvdbIndexTask.DESCRIPTION, e.getMessage());
        }
    }
}
