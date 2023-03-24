package com.zettagenomics.opencga.enterprise.server.rest;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Api;
import org.opencb.opencga.core.tools.annotations.ApiOperation;
import org.opencb.opencga.core.tools.annotations.ApiParam;
import org.opencb.opencga.server.generator.RestApiParser;
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.*;

@Path("/{apiVersion}/meta")
@Produces("application/json")
@Api(value = "Meta", description = "Meta RESTful Web Services API")
public class EnterpriseMetaWSServer extends MetaWSServer {

    public EnterpriseMetaWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @Override
    @GET
    @Path("/about")
    @ApiOperation(httpMethod = "GET", value = "Returns info about current OpenCGA code.", response = Map.class)
    public Response getAbout() {
        Map<String, String> info = new HashMap<>(5);
        info.put("Program", "XetaBase!");
//        info.put("Version", GitRepositoryState.get("com/zettagenomics/opencga/enterprise/core/git.properties").getBuildVersion());
//        info.put("Git branch", GitRepositoryState.get("com/zettagenomics/opencga/enterprise/core/git.properties").getBranch());
//        info.put("Git commit", GitRepositoryState.get("com/zettagenomics/opencga/enterprise/core/git.properties").getCommitId());
        info.put("Description", "Big Data platform for processing and analysing NGS data");
        OpenCGAResult queryResult = new OpenCGAResult();
        queryResult.setTime(0);
        queryResult.setResults(Collections.singletonList(info));

        return createOkResponse(queryResult);
    }

    @Override
    @GET
    @Path("/api")
    @ApiOperation(value = "API", response = List.class)
    public Response api(@ApiParam(value = "List of categories to get API from") @QueryParam("category") String categoryStr, @QueryParam("summary") boolean summary) {
        Map<String, Class<?>> classMap = new LinkedHashMap<>();
        classMap.put("users", UserWSServer.class);
        classMap.put("projects", ProjectWSServer.class);
        classMap.put("studies", StudyWSServer.class);
        classMap.put("files", FileWSServer.class);
        classMap.put("jobs", JobWSServer.class);
        classMap.put("samples", SampleWSServer.class);
        classMap.put("individuals", IndividualWSServer.class);
        classMap.put("families", FamilyWSServer.class);
        classMap.put("cohorts", CohortWSServer.class);
        classMap.put("panels", PanelWSServer.class);
        classMap.put("alignment", AlignmentWebService.class);
        classMap.put("variant", VariantWebService.class);
        classMap.put("clinical", ClinicalWebService.class);
        classMap.put("variantOperations", VariantOperationWebService.class);
        classMap.put("meta", EnterpriseMetaWSServer.class);
        classMap.put("cva", CvaWSServer.class);
        classMap.put("admin", AdminWSServer.class);
//        classMap.put("ga4gh", Ga4ghWSServer.class);

        List<Class<?>> classes = new ArrayList<>();
        // Check if some categories have been selected
        if (StringUtils.isNotEmpty(categoryStr)) {
            for (String category : categoryStr.split(",")) {
                classes.add(classMap.get(category));
            }
        } else {
            // Get API for all categories
            for (String category : classMap.keySet()) {
                classes.add(classMap.get(category));
            }
        }
        RestApi restApi = new RestApiParser().parse(classes, summary);
        return createOkResponse(new OpenCGAResult<>(0, Collections.emptyList(), 1, Collections.singletonList(restApi.getCategories()), 1));
    }
    @GET
    @Path("/about2")
    @ApiOperation(httpMethod = "GET", value = "Returns info about current OpenCGA code.", response = Map.class)
    public Response getAbout2() {
        Map<String, String> info = new HashMap<>(5);
        info.put("Program", "XetaBase2!");
        info.put("Version", GitRepositoryState.get().getBuildVersion());
        info.put("Git branch", GitRepositoryState.get().getBranch());
        info.put("Git commit", GitRepositoryState.get().getCommitId());
        info.put("Description", "Big Data platform for processing and analysing NGS data");
        OpenCGAResult queryResult = new OpenCGAResult();
        queryResult.setTime(0);
        queryResult.setResults(Collections.singletonList(info));

        return createOkResponse(queryResult);
    }

}
