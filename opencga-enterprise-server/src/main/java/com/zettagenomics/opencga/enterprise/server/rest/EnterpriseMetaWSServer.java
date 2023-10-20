package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseUserManager;
import com.zettagenomics.opencga.enterprise.core.GitUtils;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.server.EnterpriseResourceConfig;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.lang3.StringUtils;
import org.jasig.cas.client.authentication.AttributePrincipal;
import org.opencb.opencga.catalog.auth.authentication.JwtManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Api;
import org.opencb.opencga.core.tools.annotations.ApiOperation;
import org.opencb.opencga.core.tools.annotations.ApiParam;
import org.opencb.opencga.server.generator.RestApiParser;
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.rest.MetaWSServer;

import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Key;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Path("/{apiVersion}/meta")
@Produces("application/json")
@Api(value = "Meta", description = "Meta RESTful Web Services API")
public class EnterpriseMetaWSServer extends MetaWSServer {

    private final String opencgaToken;
    private final EnterpriseConfiguration enterpriseConfiguration;

    public static final AtomicReference<EnterpriseUserManager> enterpriseUserManagerAtomicRef = new AtomicReference<>();

    public EnterpriseMetaWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);

        Key key = new SecretKeySpec(catalogManager.getConfiguration().getAdmin().getSecretKey().getBytes(), SignatureAlgorithm.HS256.getJcaName());
        JwtManager jwtManager = new JwtManager(catalogManager.getConfiguration().getAdmin().getAlgorithm(), key);
        this.opencgaToken = jwtManager.createJWTToken(ParamConstants.OPENCGA_USER_ID, 0L);

        this.enterpriseConfiguration = EnterpriseConfiguration.load(opencgaHome);
    }

    private EnterpriseUserManager getEnterpriseUserManager() {
        EnterpriseUserManager enterpriseUserManager = enterpriseUserManagerAtomicRef.get();
        if (enterpriseUserManager == null) {
            synchronized (enterpriseUserManagerAtomicRef) {
                enterpriseUserManager = enterpriseUserManagerAtomicRef.get();
                if (enterpriseUserManager == null) {
                    enterpriseUserManager = new EnterpriseUserManager(catalogManager, enterpriseConfiguration, opencgaToken);
                    enterpriseUserManagerAtomicRef.set(enterpriseUserManager);
                }
            }
        }
        return enterpriseUserManager;
    }

    @Override
    @GET
    @Path("/about")
    @ApiOperation(httpMethod = "GET", value = "Returns info about current OpenCGA code.", response = Map.class)
    public Response getAbout() {
        Map<String, String> info = new LinkedHashMap<>(6);
        info.put("Program", "XetaBase (Zetta Genomics)");
        info.put("Version", GitUtils.getEnterprise().getBuildVersion());
        info.put("Git branch", GitUtils.getEnterprise().getBranch());
        info.put("Git commit", GitUtils.getEnterprise().getCommitId());
        info.put("Description", "Big Data platform for processing and analysing NGS data");
        info.put("OpenCGA Version", GitUtils.getOpenCGA().getBuildVersion());

        OpenCGAResult<Object> queryResult = new OpenCGAResult<>();
        queryResult.setTime(0);
        queryResult.setResults(Collections.singletonList(info));
        return createOkResponse(queryResult);
    }

    @Override
    @GET
    @Path("/api")
    @ApiOperation(value = "API", response = List.class)
    public Response api(@ApiParam(value = "List of categories to get API from") @QueryParam("category") String categoryStr,
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

    @GET
    @Path("/sso/login")
    @ApiOperation(httpMethod = "GET", value = "Single Sign On.", response = Map.class)
    public Response singleSignOn(@ApiParam(value = "Callback URL") @QueryParam("url") String service) {
        if (StringUtils.isEmpty(service)) {
            return createErrorResponse(new CatalogParameterException("Missing mandatory field 'service'"));
        }

        URI targetURIForRedirection;
        try {
            AttributePrincipal principal = (AttributePrincipal) httpServletRequest.getUserPrincipal();

            String token = getEnterpriseUserManager().ssoLogin(principal);

            Cookie[] cookies = httpServletRequest.getCookies();
            if (cookies == null) {
                throw new CatalogException("Unexpected event. Could not retrieve cookies");
            }
            logger.debug("SSO cookies: ");
            for (Cookie cookie : cookies) {
                logger.debug("{}: {}", cookie.getName(), cookie.getValue());
            }
            StringBuilder queryParams = new StringBuilder();
            if (!service.endsWith("?")) {
                queryParams.append("?");
            }

            queryParams.append("token").append("=").append(token);
            queryParams.append("&").append("user").append("=").append(principal.getName());
            for (Cookie cookie : cookies) {
                queryParams.append("&");
                queryParams.append(cookie.getName()).append("=").append(cookie.getValue());
            }

            targetURIForRedirection = new URI(service + queryParams);
            logger.debug("Redirecting /sso call to {}", targetURIForRedirection);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
        return Response.temporaryRedirect(targetURIForRedirection).build();
    }

    @GET
    @Path("/sso/logout")
    @ApiOperation(httpMethod = "GET", value = "Logout from Single Sign On.", response = Map.class)
    public Response singleSignOnLogout(
            @ApiParam(value = "Callback URL") @QueryParam("url") String service,
            @ApiParam(value = "Successfully logout from CAS service", hidden = true, defaultValue = "false") @QueryParam("logout") boolean logout
    ) {
        if (enterpriseConfiguration.getSso() == null || !enterpriseConfiguration.getSso().isActive()) {
            return createErrorResponse(new CatalogException("SSO is not enabled."));
        }
        if (StringUtils.isEmpty(enterpriseConfiguration.getSso().getCasServerPrefixUrl())) {
            return createErrorResponse(new CatalogException("Server error: SSO server prefix url is not properly set."));
        }

        // Logout is performed in 2 steps. Users must call to /logout without any query parameter. This will redirect to
        // CAS logout WS with a callback url to this same WS adding the query paramter "logout=true". When it reaches
        // the WS with the query parameter, it will return the html redirecting to the original callback url and remove
        // local cookies.
        if (!logout) {
            UriBuilder uriBuilder = UriBuilder.fromPath(enterpriseConfiguration.getSso().getCasServerPrefixUrl());
            uriBuilder.path("logout");
            UriBuilder callbackUri = uriInfo.getAbsolutePathBuilder();

            // Scheme may not be properly retrieved so we get it from the header (if present)
            String scheme = httpServletRequest.getHeader("X-Forwarded-Proto");
            if (StringUtils.isNotEmpty(scheme)) {
                callbackUri.scheme(scheme);
            }

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

            Response.ResponseBuilder responseBuilder = Response.ok(htmlBuilder.toString(), MediaType.TEXT_HTML_TYPE);

            logger.debug("Deleting session cookies");
            for (Cookie cookie : httpServletRequest.getCookies()) {
                NewCookie newCookie = new NewCookie(cookie.getName(), "", "/", cookie.getDomain(), cookie.getComment(),
                        0, cookie.getSecure());
                responseBuilder.cookie(newCookie);
                newCookie = new NewCookie(cookie.getName(), "", "/opencga", cookie.getDomain(), cookie.getComment(), 0,
                        cookie.getSecure());
                responseBuilder.cookie(newCookie);
            }
            return responseBuilder.build();
        }
    }
}
