package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.catalog.utils.FederationUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ParamException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.JwtPayload;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class FederationFilter implements Filter {

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
            // Construct the new URL
            String newUrl = constructRedirectUrl(request);

            // Perform the redirection based on the request method
            String method = request.getMethod();
            if ("GET".equalsIgnoreCase(method)) {
                response.sendRedirect(newUrl);
            } else if ("POST".equalsIgnoreCase(method) || "DELETE".equalsIgnoreCase(method)) {
                response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
                response.setHeader("Location", newUrl);
                response.setHeader("Allow", method);
            }
            return;
        }

        // Continue with the filter chain if no redirection is needed
        chain.doFilter(request, response);
    }

    private String constructRedirectUrl(HttpServletRequest request) {
//        request.getRequestURI(); // /opencga/webservices/rest/v2/sample/search
//        request.getRequestURL(); // http://localhost:8080/opencga/webservices/rest/v2/sample/search

        String queryString = request.getQueryString();
        queryString = StringUtils.isNotEmpty(queryString)
                ? queryString + "&url=" + request.getRequestURI() + "&method=" + request.getMethod()
                : "url=" + request.getRequestURI() + "&method=" + request.getMethod();

        String url = request.getRequestURL().toString(); // http://localhost:8080/opencga/webservices/rest/v2/sample/search
        String regex = "^(https?://[^/]+/opencga/webservices/rest/[^/]+/)(.+)$";
        return url.replaceAll(regex, "$1federations/redirect") + "?" + queryString;
    }

    private boolean requestFederatedData(HttpServletRequest request) {
        String token = getToken(request);
        if (StringUtils.isEmpty(token)) {
            return false;
        }
        JwtPayload jwtPayload = new JwtPayload(token);
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

