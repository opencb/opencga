package org.opencb.opencga.server;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import java.nio.file.Paths;

public class FederationFilterTest {

    @Ignore
    @Test
    public void runServerTest() throws Exception {
        RestServer server = new RestServer(Paths.get("/opt/opencga"), 9090);
        server.start();
        server.blockUntilShutdown();
    }

    @Test
    public void rewriteRequestUrl_shouldRewriteUrlAndQueryStringTest1() {
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        Mockito.when(mockRequest.getRequestURI()).thenReturn("/opencga/webservices/rest/v2/sample/search");
        Mockito.when(mockRequest.getRequestURL()).thenReturn(new StringBuffer("http://localhost:8080/opencga/webservices/rest/v2/sample/search"));
        Mockito.when(mockRequest.getQueryString()).thenReturn("param1=value1");
        Mockito.when(mockRequest.getMethod()).thenReturn("GET");

        FederationFilter filter = new FederationFilter();
        HttpServletRequest rewrittenRequest = filter.rewriteRequestUrl(mockRequest);

        Assert.assertEquals("/opencga/webservices/rest/v2/federations/redirect", rewrittenRequest.getRequestURI());
        Assert.assertEquals("param1=value1&url=/opencga/webservices/rest/v2/sample/search&method=GET", rewrittenRequest.getQueryString());
        Assert.assertEquals("http://localhost:8080/opencga/webservices/rest/v2/federations/redirect", rewrittenRequest.getRequestURL().toString());
    }

    @Test
    public void rewriteRequestUrl_shouldRewriteUrlAndQueryStringTest2() {
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        Mockito.when(mockRequest.getRequestURI()).thenReturn("/opencga/webservices/rest/v2/other/path");
        Mockito.when(mockRequest.getRequestURL()).thenReturn(new StringBuffer("http://localhost:8080/opencga/webservices/rest/v2/other/path"));
        Mockito.when(mockRequest.getQueryString()).thenReturn("param=value");
        Mockito.when(mockRequest.getMethod()).thenReturn("GET");

        FederationFilter filter = new FederationFilter();
        HttpServletRequest rewrittenRequest = filter.rewriteRequestUrl(mockRequest);

        Assert.assertEquals("/opencga/webservices/rest/v2/federations/redirect", rewrittenRequest.getRequestURI());
        Assert.assertEquals("param=value&url=/opencga/webservices/rest/v2/other/path&method=GET", rewrittenRequest.getQueryString());
        Assert.assertEquals("http://localhost:8080/opencga/webservices/rest/v2/federations/redirect", rewrittenRequest.getRequestURL().toString());
    }

    @Test
    public void rewriteRequestUrl_shouldHandleEmptyQueryString() {
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        Mockito.when(mockRequest.getRequestURI()).thenReturn("/opencga/webservices/rest/v2/sample/search");
        Mockito.when(mockRequest.getRequestURL()).thenReturn(new StringBuffer("http://localhost:8080/opencga/webservices/rest/v2/sample/search"));
        Mockito.when(mockRequest.getQueryString()).thenReturn(null);
        Mockito.when(mockRequest.getMethod()).thenReturn("POST");

        FederationFilter filter = new FederationFilter();
        HttpServletRequest rewrittenRequest = filter.rewriteRequestUrl(mockRequest);

        Assert.assertEquals("/opencga/webservices/rest/v2/federations/redirect", rewrittenRequest.getRequestURI());
        Assert.assertEquals("url=/opencga/webservices/rest/v2/sample/search&method=POST", rewrittenRequest.getQueryString());
        Assert.assertEquals("http://localhost:8080/opencga/webservices/rest/v2/federations/redirect", rewrittenRequest.getRequestURL().toString());
    }

}
