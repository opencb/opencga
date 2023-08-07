package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseUserManager;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.server.EnterpriseResourceConfig;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.lang3.StringUtils;
import org.jasig.cas.client.authentication.AttributePrincipal;
import org.opencb.opencga.catalog.auth.authentication.JwtManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.GitRepositoryState;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
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
        Map<String, String> info = new HashMap<>(5);
        info.put("Program", "XetaBase (Zetta Genomics)");
        info.put("Version", GitRepositoryState.get().getBuildVersion());
        info.put("Git branch", GitRepositoryState.get().getBranch());
        info.put("Git commit", GitRepositoryState.get().getCommitId());
        info.put("Description", "Big Data platform for processing and analysing NGS data");

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
    @Path("/sso")
    @ApiOperation(httpMethod = "GET", value = "Single Sign On.", response = Map.class)
    public Response singleSignOn(@ApiParam(value = "Callback URL") @QueryParam("url") String service) {
        if (StringUtils.isEmpty(service)) {
            return createErrorResponse(new CatalogParameterException("Missing mandatory field 'service'"));
        }
        AttributePrincipal principal = (AttributePrincipal) httpServletRequest.getUserPrincipal();

        URI targetURIForRedirection;
        try {
            String token = getEnterpriseUserManager().ssoLogin(principal);

            Cookie[] cookies = httpServletRequest.getCookies();
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
        } catch (CatalogException | URISyntaxException e) {
            return createErrorResponse(e);
        }
        return Response.temporaryRedirect(targetURIForRedirection).build();
    }
}
