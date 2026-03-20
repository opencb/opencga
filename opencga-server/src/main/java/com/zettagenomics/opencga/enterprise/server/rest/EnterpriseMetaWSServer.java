package com.zettagenomics.opencga.enterprise.server.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseFactory;
import org.opencb.opencga.core.config.Configuration;
import com.zettagenomics.opencga.enterprise.server.EnterpriseResourceConfig;
import com.zettagenomics.opencga.enterprise.server.commons.EnterpriseParamConstants;
import com.zettagenomics.opencga.enterprise.server.generator.EnterpriseApiCommonsImpl;
import org.apache.commons.lang3.StringUtils;
import org.jasig.cas.client.authentication.AttributePrincipal;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Api;
import org.opencb.opencga.core.tools.annotations.ApiOperation;
import org.opencb.opencga.core.tools.annotations.ApiParam;
import org.opencb.opencga.server.generator.RestApiParser;
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.generator.openapi.JsonOpenApiGenerator;
import org.opencb.opencga.server.generator.openapi.models.Swagger;
import org.opencb.opencga.server.rest.MetaWSServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.net.URI;
import java.util.*;

@Path("/{apiVersion}/meta")
@Produces("application/json")
@Api(value = "Meta", description = "Meta RESTful Web Services API")
public class EnterpriseMetaWSServer extends MetaWSServer {

    public EnterpriseMetaWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                  @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        EnterpriseFactory.init(catalogManager, opencgaHome);
    }

    @Override
    @GET
    @Path("/about")
    @ApiOperation(httpMethod = "GET", value = "Returns info about current OpenCGA code.", response = Map.class)
    public Response getAbout() {
        Map<String, String> info = new LinkedHashMap<>(6);
        info.put("Program", "XetaBase (Zetta Genomics)");
        info.put("Version", GitRepositoryState.getInstance().getBuildVersion());
        info.put("Git branch", GitRepositoryState.getInstance().getBranch());
        info.put("Git commit", GitRepositoryState.getInstance().getCommitId());
        info.put("Description", "Big Data platform for processing and analysing NGS data");
        info.put("OpenCGA Version", GitRepositoryState.getInstance().getBuildVersion());

        OpenCGAResult<Object> queryResult = new OpenCGAResult<>();
        queryResult.setTime(0);
        queryResult.setResults(Collections.singletonList(info));
        return createOkResponse(queryResult);
    }

    @Override
    @GET
    @Path("/api")
    @ApiOperation(value = "API", response = List.class)
    public Response api(@ApiParam(value = EnterpriseParamConstants.API_CATEGORY_DESCRIPTION) @QueryParam("category") String categoryStr,
                        @QueryParam("summary") boolean summary) {
        List<Class<?>> classes = new ArrayList<>();
        if (StringUtils.isNotEmpty(categoryStr)) {
            // Check if some categories have been selected
            for (String category : categoryStr.split(",")) {
                classes.add(EnterpriseResourceConfig.enterpriseApiClasses.get(category));
            }
        } else {
            // Get API for all categories
            classes = new ArrayList<>(EnterpriseResourceConfig.enterpriseApiClasses.values());
        }
        RestApi restApi = new RestApiParser().parse(classes, summary);
        return createOkResponse(new OpenCGAResult<>(0, Collections.emptyList(), 1, Collections.singletonList(restApi.getCategories()), 1));
    }

    @Deprecated
    @GET
    @Path("/openapi")
    @ApiOperation(value = "Opencga openapi json", response = String.class)
    public String openApi(@ApiParam(value = EnterpriseParamConstants.META_HOST_DESCRIPTION, required = true) @QueryParam("url") String url,
                          @ApiParam(value = "Opencga study to be default in queries.") @QueryParam("study") String study) {
        JsonOpenApiGenerator generator = new JsonOpenApiGenerator();
        Swagger swagger = generator.generateJsonOpenApi(new EnterpriseApiCommonsImpl(), token, url, apiVersion, study);
        String swaggerJson ="ERROR: openapi schema for swagger could not be generated";
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        try {
            swaggerJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(swagger);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return swaggerJson;
    }

    @GET
    @Path("/sso/login")
    @ApiOperation(httpMethod = "GET", value = "Single Sign On.", response = Map.class, hidden = true)
    public Response singleSignOn(@ApiParam(value = EnterpriseParamConstants.SSO_LOGIN_CALLBACK_DESCRIPTION) @QueryParam("url") String service) {
        if (StringUtils.isEmpty(service)) {
            return createErrorResponse(new CatalogParameterException("Missing mandatory field 'service'"));
        }

        URI targetURIForRedirection;
        try {
            StringBuilder queryParams = new StringBuilder();
            if (!service.endsWith("?")) {
                queryParams.append("?");
            }
            // Add session id
            queryParams.append("jsessionid").append("=").append(httpServletRequest.getSession().getId());

            AttributePrincipal principal = (AttributePrincipal) httpServletRequest.getUserPrincipal();
            String token = catalogManager.getUserManager().ssoLogin(principal.getName(), principal.getAttributes());
            // Add user and token
            queryParams.append("&").append("token").append("=").append(token);
            queryParams.append("&").append("user").append("=").append(principal.getName());

            targetURIForRedirection = new URI(service + queryParams);
            logger.debug("Redirecting /sso call to {}", targetURIForRedirection);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
        return Response.temporaryRedirect(targetURIForRedirection).build();
    }

    @Deprecated
    @GET
    @Path("/sso/logout")
    @ApiOperation(httpMethod = "GET", value = "Logout from Single Sign On.", response = Map.class, hidden = true)
    public Response singleSignOnLogout(
            @ApiParam(value = EnterpriseParamConstants.SSO_LOGOUT_CALLBACK_DESCRIPTION) @QueryParam("url") String service,
            @ApiParam(value = EnterpriseParamConstants.SSO_LOGOUT_SUCCESS_DESCRIPTION, hidden = true, defaultValue = "false") @QueryParam("logout") boolean logout
    ) {
        Configuration enterpriseConfiguration = EnterpriseFactory.getEnterpriseConfiguration();
        if (enterpriseConfiguration.getSso() == null || !enterpriseConfiguration.getSso().isActive()) {
            return createErrorResponse(new CatalogException("SSO is not enabled."));
        }
        if (StringUtils.isEmpty(enterpriseConfiguration.getSso().getCasServerPrefixUrl())) {
            return createErrorResponse(new CatalogException("Server error: SSO server prefix url is not properly set."));
        }

        // Logout is performed in 2 steps. Users must call to /logout without any query parameter. This will redirect to
        // CAS logout WS with a callback url to this same WS adding the query parameter "logout=true". When it reaches
        // the WS with the query parameter, it will return the html redirecting to the original callback url and remove
        // local cookies.
        if (!logout) {
            // Get the public address
            String serverName = enterpriseConfiguration.getSso().getServerName();
            // Get the internal address
            String internalUrlCalled = uriInfo.getAbsolutePath().toString();
            int i = internalUrlCalled.indexOf("/opencga");
            // Remove preceded section of internal url and replace it for serverName configuration url.
            // serverName should always contain the external public url users call, whereas internalUrlCalled will
            // normally be the internal url (when in use with a reverse proxy)
            String originalUrl = serverName + internalUrlCalled.substring(i);

            UriBuilder uriBuilder = UriBuilder.fromPath(enterpriseConfiguration.getSso().getCasServerPrefixUrl());
            uriBuilder.path("logout");
            UriBuilder callbackUri = UriBuilder.fromPath(originalUrl);

            callbackUri.queryParam("logout", true);
            callbackUri.queryParam("url", service);
            uriBuilder.queryParam("service", callbackUri.build());

            logger.debug("Callback uri: {}", callbackUri.build());

            URI uri = uriBuilder.build();
            logger.debug("Redirecting to {}", uri.getPath());
            return Response.temporaryRedirect(uri).build();
        } else {
            StringBuilder htmlBuilder = new StringBuilder()
                    .append("<html lang=\"en\">\n")
                    .append("  <head>\n")
                    .append("    <meta charset=\"UTF-8\" />\n")
                    .append("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />\n")
                    .append("    <link href=\"https://unpkg.com/lowcss/dist/low.css\" rel=\"stylesheet\" />\n")
                    .append("  </head>\n")
                    .append("  <body class=\"font-inter leading-normal m-0 p-0 h-screen\">\n")
                    .append("    <div class=\"flex items-center justify-center w-full h-full\">\n")
                    .append("      <div class=\"w-full maxw-lg rounded-lg text-center p-12 bg-green-100\">\n")
                    .append("        <div>Successfully logged out from CAS.</div>\n");
            if (StringUtils.isNotEmpty(service)) {
                htmlBuilder.append("        <div>Redirecting in <span id=\"time\">3</span> secs...</div>\n");
            }
            htmlBuilder
                    .append("      </div>\n")
                    .append("    </div>\n");
            if (StringUtils.isNotEmpty(service)) {
                htmlBuilder
                        .append("    <script type=\"text/javascript\">\n")
                        .append("      let time = 3;\n")
                        .append("      const timer = window.setInterval(() => {\n")
                        .append("        time = time - 1;\n")
                        .append("        document.getElementById(\"time\").textContent = time;\n")
                        .append("        if (time === 0) {\n")
                        .append("          window.clearInterval(timer);\n")
                        .append("          window.location = '").append(service).append("';\n")
                        .append("        }\n")
                        .append("      }, 1000);\n")
                        .append("    </script>\n");
            }
            htmlBuilder
                    .append("  </body>\n")
                    .append("</html>");

            logger.info("CAS logout requested. Invalidating session '{}'", httpServletRequest.getSession().getId());
            httpServletRequest.getSession().invalidate();

            return Response.ok(htmlBuilder.toString(), MediaType.TEXT_HTML_TYPE).build();
        }
    }
}
