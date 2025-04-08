package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.catalog.utils.FederationUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ParamException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.JwtPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class FederationFilter implements Filter {

    private static Logger logger = LoggerFactory.getLogger(FederationFilter.class);
    private static final Pattern REST_PATTERN = Pattern.compile("^(/opencga/webservices/rest/[^/]+/)(.+)$");

    private static final Pattern[] LOCAL_REST_PATTERNS = new Pattern[]{
            Pattern.compile("/opencga/webservices/rest/[^/]+/federations/.+")
    };

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        // Check the conditions for redirection
        if (requestFederatedData(request)) {
            request = rewriteRequestUrl(request);
        }

        // Continue with the filter chain if no redirection is needed
        chain.doFilter(request, response);
    }

    private HttpServletRequest rewriteRequestUrl(HttpServletRequest request) {
        // request.getRequestURI(); // /opencga/webservices/rest/v2/sample/search
        // request.getRequestURL(); // http://localhost:8080/opencga/webservices/rest/v2/sample/search

        String queryString = StringUtils.isNotEmpty(request.getQueryString())
                ? request.getQueryString() + "&url=" + request.getRequestURI() + "&method=" + request.getMethod()
                : "url=" + request.getRequestURI() + "&method=" + request.getMethod();
        logger.info("Query string: {}\nRewritten query string: {}", request.getQueryString(), queryString);

        String rewrittenUri = REST_PATTERN.matcher(request.getRequestURI()).replaceAll("$1federations/redirect");
        logger.info("Requested URI: {}\nRewritten URI: {}", request.getRequestURI(), rewrittenUri);

        String rewrittenUrl = request.getRequestURL().toString().replace(request.getRequestURI(), rewrittenUri);
        logger.info("Requested URL: {}\nRewritten URL: {}", request.getRequestURL(), rewrittenUrl);

        return new HttpServletRequestWrapper(request) {
            @Override
            public String getRequestURI() {
                return rewrittenUri;
            }

            @Override
            public String getQueryString() {
                return queryString;
            }

            @Override
            public StringBuffer getRequestURL() {
                return new StringBuffer(rewrittenUrl);
            }
        };
    }

    private boolean requestFederatedData(HttpServletRequest request) {
        String token = getToken(request);
        if (StringUtils.isEmpty(token)) {
            return false;
        }
//        if (request.getRequestURI().endsWith("federations/redirect")) {
//            // When the federation is made to the same server, we need to avoid an infinite loop
//            return false;
//        }

        JwtPayload jwtPayload;
        try {
            // This may fail if the "token" doesn't have a token format
            jwtPayload = new JwtPayload(token);
        } catch (Exception e) {
            return false;
        }
        if (isRequestingFederatedData(request, jwtPayload)) {
            return true;
        }
        return false;
    }

    private boolean isRequestingFederatedData(HttpServletRequest request, JwtPayload jwtPayload) {
        Map<String, Object> params = new HashMap<>();
        Enumeration<String> parameterNames = request.getParameterNames();
        while (parameterNames.hasMoreElements()) {
            String key = parameterNames.nextElement();
            params.put(key, request.getParameter(key));
        }

        String url = request.getRequestURI();
        for (Pattern localRestPattern : LOCAL_REST_PATTERNS) {
            if (localRestPattern.matcher(url).matches()) {
                return false;
            }
        }

        String project = FederationUtils.extractProject(url, params);
        String study = FederationUtils.extractStudy(url, params);
        try {
            FederationUtils.findFederationServerIdInPayload(project, study, jwtPayload);
            return true;
        } catch (CatalogException e) {
            // The previous call fails if the project or study is not federated
            return false;
        }
    }

    private String getToken(HttpServletRequest request) {
        String authorization = request.getHeader("Authorization");
        if (authorization != null && authorization.length() > 7) {
            String token = authorization;
            if (!token.startsWith("Bearer ")) {
                throw new ParamException.HeaderParamException(new Throwable("Authorization header must start with Bearer JWToken"),
                        "Bearer", "");
            }
            token = token.substring("Bearer".length()).trim();
            if (StringUtils.isNotEmpty(token) && !token.equals("null")) {
                return token;
            }
        }

        return request.getParameter("sid");
    }

    @Override
    public void destroy() {
    }

}

