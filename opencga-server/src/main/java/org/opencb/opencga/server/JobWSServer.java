package org.opencb.opencga.server;


import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.IOUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Path("/job")
///opencga/rest/v1/jobs/create?analysisId=23&tool=samtools
@Api(value = "job", description = "job")
public class JobWSServer extends OpenCGAWSServer {

    public JobWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @Produces("application/json")
    @ApiOperation(value = "Create job")

    public Response createJob(
            @ApiParam(value = "analysisId", required = true) @QueryParam("analysisId") int analysisId,
            @ApiParam(value = "name", required = true) @QueryParam("name") String name,
            @ApiParam(value = "toolName", required = true) @QueryParam("toolName") String toolName,
            @ApiParam(value = "description", required = true) @QueryParam("description") String description,
            @ApiParam(value = "commandLine", required = true) @QueryParam("commandLine") String commandLine,
            @ApiParam(value = "outDir", required = true) @QueryParam("outDir") String outDir,
            @ApiParam(value = "inputFiles", required = true) @QueryParam("inputFiles") String inputFilesStr
    ) {
        QueryResult queryResult;
        try {
            List<String> inputFilesList = Splitter.on(",").splitToList(inputFilesStr);
            List<Integer> inputFiles = new ArrayList<>();

            for(String s : inputFilesList) inputFiles.add(Integer.valueOf(s));

            queryResult = catalogManager.createJob(analysisId, name, toolName, description, commandLine, outDir, inputFiles, sessionId);

            return createOkResponse(queryResult);

        } catch (CatalogManagerException | CatalogIOManagerException  e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }


}