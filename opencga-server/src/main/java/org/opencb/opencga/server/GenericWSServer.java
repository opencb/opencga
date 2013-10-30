package org.opencb.opencga.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.opencga.account.CloudSessionManager;
import org.opencb.opencga.account.io.IOManagementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.ResponseBuilder;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Path("/")
@Produces("text/plain")
public class GenericWSServer {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected static Properties properties;

    protected UriInfo uriInfo;
    protected String accountId;
    //    protected String sessionId;
    protected String sessionIp;

    protected MultivaluedMap<String, String> params;

    protected static ObjectWriter jsonObjectWriter;
    protected static ObjectMapper jsonObjectMapper;

    //General params
    @DefaultValue("")
    @QueryParam("sessionid")
    protected String sessionId;

    @DefaultValue("json")
    @QueryParam("of")
    protected String of;


    /**
     * Only one CloudSessionManager
     */
    protected static CloudSessionManager cloudSessionManager;

    static {

        try {
            cloudSessionManager = new CloudSessionManager();
        } catch (IOException | IOManagementException e) {
            e.printStackTrace();
        }

        jsonObjectMapper = new ObjectMapper();
        jsonObjectWriter = jsonObjectMapper.writer();

        InputStream is = CloudSessionManager.class.getClassLoader().getResourceAsStream("application.properties");
        properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

//    public GenericWSServer() {
////        packages("org.opencb.opencga.server");
//                super(
//                AccountWSServer.class,
//                AdminWSServer.class,
//                AnalysisWSServer.class,
//                BamWSServer.class,
//                GeocodingAddressService.class,
//                GffWSServer.class,
//                JobAnalysisWSServer.class,
//                StorageWSServer.class,
//                UtilsWSServer.class,
//                VcfWSServer.class,
//                WSResponse.class,
//                GenericWSServer.class,
//
//                MultiPartFeature.class
//        );
//    }

    public GenericWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
//        this();
//        packages("org.opencb.opencga.server");

        this.uriInfo = uriInfo;
        this.params = this.uriInfo.getQueryParameters();
//        this.sessionId = (this.params.get("sessionid") != null) ? this.params.get("sessionid").get(0) : "";
//        this.of = (this.params.get("of") != null) ? this.params.get("of").get(0) : "";
        this.sessionIp = httpServletRequest.getRemoteAddr();

//		UserAgent userAgent = UserAgent.parseUserAgentString(httpServletRequest.getHeader("User-Agent"));
//
//		Browser br = userAgent.getBrowser();
//
//		OperatingSystem op = userAgent.getOperatingSystem();

        logger.debug(uriInfo.getRequestUri().toString());
        // logger.info("------------------->" + br.getName());
        // logger.info("------------------->" + br.getBrowserType().getName());
        // logger.info("------------------->" + op.getName());
        // logger.info("------------------->" + op.getId());
        // logger.info("------------------->" + op.getDeviceType().getName());


        File dqsDir = new File(properties.getProperty("DQS.PATH"));
        if (dqsDir.exists()) {
            File accountsDir = new File(properties.getProperty("ACCOUNTS.PATH"));
            if (!accountsDir.exists()) {
                accountsDir.mkdir();
            }
        }
    }

    @GET
    @Path("/echo/{message}")
    public Response echoGet(@PathParam("message") String message) {
        logger.info(sessionId);
        logger.info(of);
        return createOkResponse(message);
    }

    protected Response createErrorResponse(Object o) {
        String objMsg = o.toString();
        if (objMsg.startsWith("ERROR:")) {
            return buildResponse(Response.ok("" + o));
        } else {
            return buildResponse(Response.ok("ERROR: " + o));
        }
    }

    protected Response createOkResponse(Object o) {
        return buildResponse(Response.ok(o));
    }

    protected Response createOkResponse(Object o1, MediaType o2) {
        return buildResponse(Response.ok(o1, o2));
    }

    protected Response createOkResponse(Object o1, MediaType o2, String fileName) {
        return buildResponse(Response.ok(o1, o2).header("content-disposition", "attachment; filename =" + fileName));
    }

    private Response buildResponse(ResponseBuilder responseBuilder) {
        return responseBuilder.header("Access-Control-Allow-Origin", "*").build();
    }
}
