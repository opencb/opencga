package com.zettagenomics.opencga.enterprise.server.rest;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.tools.annotations.*;
import org.opencb.opencga.server.rest.OpenCGAWSServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;

@Path("/{apiVersion}/cva")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Cva", description = "Methods for working with 'cva' endpoint")
public class CvaWSServer extends OpenCGAWSServer {

    public CvaWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
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
}
