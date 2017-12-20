package org.opencb.opencga.server.rest.admin;

import io.swagger.annotations.*;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;

@Path("/{apiVersion}/admin/audit")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Admin - Audit", position = 4, description = "Methods to audit")
public class AuditWSServer extends OpenCGAWSServer {

    public AuditWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @GET
    @Path("/query")
    @ApiOperation(value = "Query the audit database")
    public Response query() {
        return createErrorResponse(new NotImplementedException());
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group by operation")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "count", value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(
            @ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
                @QueryParam("fields") String fields,
            @ApiParam(value = "Resource to be grouped by.", required = true) @QueryParam("resource") AuditRecord.Resource resource,
            @ApiParam(value = "Action performed") @DefaultValue("") @QueryParam("action") String action,
            @ApiParam(value = "Object before update") @DefaultValue("") @QueryParam("before") String before,
            @ApiParam(value = "Object after update") @DefaultValue("") @QueryParam("after") String after,
            @ApiParam(value = "Date <,<=,>,>=(Format: yyyyMMddHHmmss) and yyyyMMddHHmmss-yyyyMMddHHmmss") @DefaultValue("")
                @QueryParam("date") String date) {
        try {


            return createOkResponse(catalogManager.getAuditManager().groupBy(query, fields, queryOptions, sessionId));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }

    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Get some stats from the audit database")
    public Response stats() {
        return createErrorResponse(new NotImplementedException());
    }

}
