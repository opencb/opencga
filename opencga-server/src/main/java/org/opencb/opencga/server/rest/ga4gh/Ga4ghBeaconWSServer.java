package org.opencb.opencga.server.rest.ga4gh;

import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import org.opencb.opencga.server.rest.ga4gh.models.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

@Path("/{apiVersion}/beacon")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "GA4GH - BEACON", position = 13, description = "Global Alliance for Genomics & Health RESTful API")
public class Ga4ghBeaconWSServer extends OpenCGAWSServer {

    private final Ga4ghBeaconManager ga4ghBeaconManager;

    public Ga4ghBeaconWSServer(@Context UriInfo uriInfo,
                               @Context HttpServletRequest httpServletRequest,
                               @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        ga4ghBeaconManager = new Ga4ghBeaconManager(catalogManager, variantManager);
    }

    @GET
    @Path("/default")
    @Produces({"application/json"})
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get information about the beacon ", response = Ga4ghBeacon.class, tags = {"Endpoints",})
    @io.swagger.annotations.ApiResponses(value = {
            @io.swagger.annotations.ApiResponse(code = 200, message = "Successful operation ", response = Ga4ghBeacon.class)})
    public Response getBeacon(@Context SecurityContext securityContext)
            throws NotFoundException {

        Ga4ghBeacon beacon = ga4ghBeaconManager.getBeacon(token);
        return createJsonResponse(beacon, Response.Status.OK);
    }

    @POST
    @Path("/g_variants")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @ApiOperation(value = "", notes = "Any kind of genomic query that wants to query variants. ", response = Ga4ghGenomicVariantResponse.class, responseContainer = "List", tags={ "Endpoints", })
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful operation ", response = Ga4ghGenomicVariantResponse.class, responseContainer = "List"),
            @ApiResponse(code = 400, message = "Bad request (e.g. missing mandatory parameter) ", response = Ga4ghGenomicVariantResponse.class),
            @ApiResponse(code = 401, message = "Unauthorised (e.g. when an unauthenticated user tries to access a protected resource)", response = Ga4ghGenomicVariantResponse.class),
            @ApiResponse(code = 403, message = "Forbidden (e.g. the resource is protected for all users or the user is authenticated but s/he is not granted for this resource)", response = Ga4ghGenomicVariantResponse.class) })
    public Response postGenomicVariant(@ApiParam(value = "", required = true) @NotNull @Valid  Ga4ghGenomicVariantRequest request, @Context SecurityContext securityContext)
            throws NotFoundException {
        Ga4ghGenomicVariantResponse response = new Ga4ghGenomicVariantResponse();
        Ga4ghGenomicVariantResponseValue value = new Ga4ghGenomicVariantResponseValue();
        response.setValue(value);
        value.setBeaconId(Ga4ghBeaconManager.BEACON_ID);
        value.setApiVersion(Ga4ghBeaconManager.BEACON_API_VERSION);
        value.setRequest(request);

        return processRequest(response, value, ga4ghBeaconManager::variant);
    }


    @POST
    @Path("/individuals")
    @Consumes({"application/json"})
    @Produces({"application/json"})
    @ApiOperation(value = "", notes = "Gets response to a beacon query for individual information. ", response = Ga4ghIndividualResponse.class, responseContainer = "List", tags = {"Endpoints",})
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful operation ", response = Ga4ghIndividualResponse.class, responseContainer = "List"),
            @ApiResponse(code = 400, message = "Bad request (e.g. missing mandatory parameter) ", response = Ga4ghIndividualResponse.class),
            @ApiResponse(code = 401, message = "Unauthorised (e.g. when an unauthenticated user tries to access a protected resource)", response = Ga4ghIndividualResponse.class),
            @ApiResponse(code = 403, message = "Forbidden (e.g. the resource is protected for all users or the user is authenticated but s/he is not granted for this resource)", response = Ga4ghIndividualResponse.class)})
    public Response postIndividualResponse(@ApiParam(value = "", required = true) @NotNull @Valid Ga4ghIndividualRequest request, @Context SecurityContext securityContext)
            throws NotFoundException {
        Ga4ghIndividualResponse response = new Ga4ghIndividualResponse();
        response.setMeta(new Ga4ghRequestMeta().apiVersion(Ga4ghBeaconManager.BEACON_API_VERSION));
        Ga4ghIndividualResponseValue value = new Ga4ghIndividualResponseValue();
        response.setValue(value);
        value.setBeaconId(Ga4ghBeaconManager.BEACON_ID);
        value.setApiVersion(Ga4ghBeaconManager.BEACON_API_VERSION);
        value.setRequest(request);

        return processRequest(response, value, ga4ghBeaconManager::individual);
    }

    interface ProcessRequestCall<T> {
        void call(T t, String token) throws Exception;
    }

    private <T> Response processRequest(T response, Object value, ProcessRequestCall<T> process) {
        Response.Status status = Response.Status.OK;
        Ga4ghBeaconError error = null;
        try {
            process.call(response, token);
        } catch (CatalogAuthorizationException e) {
            // Forbidden  403
            status = Response.Status.FORBIDDEN;
            error = new Ga4ghBeaconError().errorCode(status.getStatusCode()).errorMessage(e.getMessage());
            logger.error("Catch exception", e);
        } catch (CatalogAuthenticationException e) {
            // Unauthorised  401
            status = Response.Status.UNAUTHORIZED;
            error = new Ga4ghBeaconError().errorCode(status.getStatusCode()).errorMessage(e.getMessage());
            logger.error("Catch exception", e);
        } catch (Exception e) {
            // Bad request  400
            status = Response.Status.BAD_REQUEST;
            error = new Ga4ghBeaconError().errorCode(status.getStatusCode())
                    .errorMessage(StringUtils.isEmpty(e.getMessage()) ? e.toString() : e.getMessage());
            logger.error("Catch exception", e);
        }
        if (error != null) {
            try {
                value.getClass().getMethod("setError", Ga4ghBeaconError.class).invoke(value, error);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                logger.error("Unexpected exception", e);
            }
        }

        return createJsonResponse(response, status);
    }
}
