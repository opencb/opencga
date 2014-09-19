package org.opencb.opencga.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;


import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.CatalogManager;
import org.opencb.opencga.catalog.core.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.ResponseBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

@Path("/")
public class OpenCGAWSServer {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected static Properties properties;
    protected static Config config;

    protected String version;
    protected UriInfo uriInfo;
    protected String userId;
    protected String sessionIp;

    // Common input arguments
    protected MultivaluedMap<String, String> params;
    protected QueryOptions queryOptions;
    protected QueryResponse queryResponse;

    // Common output members
    protected long startTime;
    protected long endTime;

    protected static ObjectWriter jsonObjectWriter;
    protected static ObjectMapper jsonObjectMapper;

    //Common query params
    @DefaultValue("")
    @QueryParam("sid")
    protected String sessionId;

    @DefaultValue("json")
    @QueryParam("of")
    protected String outputFormat;

    @DefaultValue("")
    @QueryParam("exclude")
    protected String exclude;

    @DefaultValue("")
    @QueryParam("include")
    protected String include;

    @DefaultValue("true")
    @QueryParam("metadata")
    protected Boolean metadata;

    protected static CatalogManager catalogManager;

    static {

        InputStream is = OpenCGAWSServer.class.getClassLoader().getResourceAsStream("catalog.properties");
        properties = new Properties();
        try {
            properties.load(is);
            System.out.println("catalog.properties");
            System.out.println(properties.getProperty("HOST"));
            System.out.println(properties.getProperty("PORT"));
            System.out.println(properties.getProperty("DATABASE"));
            System.out.println(properties.getProperty("USER"));
            System.out.println(properties.getProperty("PASSWORD"));
            System.out.println(properties.getProperty("ROOTDIR"));
        } catch (IOException e) {
            System.out.println("Error loading properties");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        try {
            catalogManager = new CatalogManager(properties);
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        } catch (CatalogIOManagerException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        jsonObjectMapper = new ObjectMapper();
        jsonObjectWriter = jsonObjectMapper.writer();

    }

    public OpenCGAWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        this.startTime = System.currentTimeMillis();
        this.version = version;

        this.uriInfo = uriInfo;
        logger.debug(uriInfo.getRequestUri().toString());

        this.queryOptions = new QueryOptions();
        queryOptions.put("exclude", exclude);
        queryOptions.put("include", include);
        queryOptions.put("metadata", metadata);

        this.sessionIp = httpServletRequest.getRemoteAddr();
    }


    protected Response createErrorResponse(Object o) {
        QueryResult<ObjectMap> result = new QueryResult();
        result.setErrorMsg(o.toString());
        return createOkResponse(result);
    }

    protected Response createOkResponse(Object obj) {
        queryResponse = new QueryResponse();
        endTime = System.currentTimeMillis() - startTime;
        queryResponse.setTime(new Long(endTime - startTime).intValue());
        queryResponse.setApiVersion(version);
        queryResponse.setQueryOptions(queryOptions);

        // Guarantee that the QueryResponse object contains a coll of results
        Collection coll;
        if (obj instanceof Collection) {
            coll = (Collection) obj;
        } else {
            coll = new ArrayList();
            coll.add(obj);
        }
        queryResponse.setResponse(coll);

        switch (outputFormat.toLowerCase()) {
            case "json":
                return createJsonResponse(queryResponse);
            case "xml":
//                return createXmlResponse(queryResponse);
            default:
                return buildResponse(Response.ok());
        }


    }

    protected Response createJsonResponse(Object object) {
        try {
            return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(object), MediaType.APPLICATION_JSON_TYPE));
        } catch (JsonProcessingException e) {
            return createErrorResponse("Error parsing QueryResponse object:\n" + Arrays.toString(e.getStackTrace()));
        }
    }

    //Response methods
    protected Response createOkResponse(Object o1, MediaType o2) {
        return buildResponse(Response.ok(o1, o2));
    }

    protected Response createOkResponse(Object o1, MediaType o2, String fileName) {
        return buildResponse(Response.ok(o1, o2).header("content-disposition", "attachment; filename =" + fileName));
    }

    protected Response buildResponse(ResponseBuilder responseBuilder) {
        return responseBuilder.header("Access-Control-Allow-Origin", "*").header("Access-Control-Allow-Headers", "x-requested-with, content-type").build();
    }
}
