package org.opencb.opencga.server.sso;

import org.apache.commons.lang3.StringUtils;
import org.jasig.cas.client.Protocol;
import org.jasig.cas.client.authentication.*;
import org.jasig.cas.client.configuration.ConfigurationKeys;
import org.jasig.cas.client.util.AbstractCasFilter;
import org.jasig.cas.client.util.CommonUtils;
import org.jasig.cas.client.util.ReflectUtils;
import org.jasig.cas.client.validation.Assertion;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.JwtPayload;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class OpencgaAuthenticationFilter extends AbstractCasFilter {

    private String casServerLoginUrl;

    /**
     * Whether to send the renew request or not.
     */
    private boolean renew = false;

    /**
     * Whether to send the gateway request or not.
     */
    private boolean gateway = false;

    /**
     * The method used by the CAS server to send the user back to the application.
     */
    private String method;

    private GatewayResolver gatewayStorage = new DefaultGatewayResolverImpl();

    private AuthenticationRedirectStrategy authenticationRedirectStrategy = new DefaultAuthenticationRedirectStrategy();

    private static final List<Pattern> excludeUrlPatterns;

    static {
        excludeUrlPatterns = new ArrayList<>();
        excludeUrlPatterns.add(Pattern.compile(".*\\/webservices\\/rest\\/.*\\/meta\\/(?!sso\\/).*"));
        excludeUrlPatterns.add(Pattern.compile(".*\\/webservices\\/rest\\/.*\\/users\\/login.*"));
    }

    public OpencgaAuthenticationFilter() {
        this(Protocol.CAS2);
    }

    protected OpencgaAuthenticationFilter(final Protocol protocol) {
        super(protocol);
    }

    @Override
    protected void initInternal(final FilterConfig filterConfig) throws ServletException {
        if (!isIgnoreInitConfiguration()) {
            super.initInternal(filterConfig);

            final String loginUrl = getString(ConfigurationKeys.CAS_SERVER_LOGIN_URL);
            if (loginUrl != null) {
                setCasServerLoginUrl(loginUrl);
            } else {
                setCasServerUrlPrefix(getString(ConfigurationKeys.CAS_SERVER_URL_PREFIX));
            }

            setRenew(getBoolean(ConfigurationKeys.RENEW));
            setGateway(getBoolean(ConfigurationKeys.GATEWAY));
            setMethod(getString(ConfigurationKeys.METHOD));

            final Class<? extends GatewayResolver> gatewayStorageClass = getClass(ConfigurationKeys.GATEWAY_STORAGE_CLASS);

            if (gatewayStorageClass != null) {
                setGatewayStorage(ReflectUtils.newInstance(gatewayStorageClass));
            }

            final Class<? extends AuthenticationRedirectStrategy> authenticationRedirectStrategyClass =
                    getClass(ConfigurationKeys.AUTHENTICATION_REDIRECT_STRATEGY_CLASS);

            if (authenticationRedirectStrategyClass != null) {
                this.authenticationRedirectStrategy = ReflectUtils.newInstance(authenticationRedirectStrategyClass);
            }
        }
    }

    @Override
    public void init() {
        super.init();

        final String message = String.format(
                "one of %s and %s must not be null.",
                ConfigurationKeys.CAS_SERVER_LOGIN_URL.getName(),
                ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName());

        CommonUtils.assertNotNull(this.casServerLoginUrl, message);
    }

    @Override
    public final void doFilter(final ServletRequest servletRequest, final ServletResponse servletResponse,
                               final FilterChain filterChain) throws IOException, ServletException {

        final HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;

        if (isRequestUrlExcluded(request)) {
            logger.debug("Request is ignored.");
            filterChain.doFilter(request, response);
            return;
        }

        final HttpSession session = request.getSession(false);
        final Assertion assertion = session != null ? (Assertion) session.getAttribute(CONST_CAS_ASSERTION) : null;

        if (assertion != null) {
            filterChain.doFilter(request, response);
            return;
        }

        final String serviceUrl = constructServiceUrl(request, response);
        final String ticket = retrieveTicketFromRequest(request);
        final boolean wasGatewayed = this.gateway && this.gatewayStorage.hasGatewayedAlready(request, serviceUrl);

        if (CommonUtils.isNotBlank(ticket) || wasGatewayed) {
            filterChain.doFilter(request, response);
            return;
        }

        final String modifiedServiceUrl;

        logger.debug("no ticket and no assertion found");
        if (this.gateway) {
            logger.debug("setting gateway attribute in session");
            modifiedServiceUrl = this.gatewayStorage.storeGatewayInformation(request, serviceUrl);
        } else {
            modifiedServiceUrl = serviceUrl;
        }

        logger.debug("Constructed service url: {}", modifiedServiceUrl);

        final String urlToRedirectTo = CommonUtils.constructRedirectUrl(this.casServerLoginUrl,
                getProtocol().getServiceParameterName(), modifiedServiceUrl, this.renew, this.gateway, this.method);

        logger.debug("redirecting to \"{}\"", urlToRedirectTo);
        this.authenticationRedirectStrategy.redirect(request, response, urlToRedirectTo);
    }

    public final void setRenew(final boolean renew) {
        this.renew = renew;
    }

    public final void setGateway(final boolean gateway) {
        this.gateway = gateway;
    }

    public void setMethod(final String method) {
        this.method = method;
    }

    public final void setCasServerUrlPrefix(final String casServerUrlPrefix) {
        setCasServerLoginUrl(CommonUtils.addTrailingSlash(casServerUrlPrefix) + "login");
    }

    public final void setCasServerLoginUrl(final String casServerLoginUrl) {
        this.casServerLoginUrl = casServerLoginUrl;
    }

    public final void setGatewayStorage(final GatewayResolver gatewayStorage) {
        this.gatewayStorage = gatewayStorage;
    }

    /**
     * It will return true if we need to let the request pass through the filter.
     * We will let it through if:
     * - The url requested is any from /meta
     * - The url requested is the login url
     * - The user logged in does not have a SSO account.
     *
     * @param request Request url.
     * @return True if we need to let this request pass through the filter.
     */
    private boolean isRequestUrlExcluded(final HttpServletRequest request) {
        String authorization = request.getHeader("Authorization");
        if (StringUtils.isNotEmpty(authorization) && authorization.startsWith("Bearer")) {
            String token = authorization.substring("Bearer".length()).trim();
            if (StringUtils.isNotEmpty(token)) {
                try {
                    JwtPayload jwtPayload = new JwtPayload(token);
                    if (jwtPayload.getAuthOrigin() == null
                            || AuthenticationOrigin.AuthenticationType.SSO != jwtPayload.getAuthOrigin()) {
                        return true;
                    }
                } catch (IllegalArgumentException e) {
                    logger.error("Error parsing token '{}': {}", token, e.getMessage(), e);
                }
            }
        }

        final StringBuffer urlBuffer = request.getRequestURL();
        if (request.getQueryString() != null) {
            urlBuffer.append("?").append(request.getQueryString());
        }
        final String requestUri = urlBuffer.toString();
        for (Pattern excludeUrlPattern : excludeUrlPatterns) {
            if (excludeUrlPattern.matcher(requestUri).matches()) {
                return true;
            }
        }

        return false;
    }

}
