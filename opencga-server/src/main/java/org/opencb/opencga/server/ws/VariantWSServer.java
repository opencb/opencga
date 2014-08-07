package org.opencb.opencga.server.ws;

import com.google.common.base.Joiner;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import org.apache.commons.lang.mutable.MutableInt;
import org.opencb.commons.bioformats.feature.Region;
import org.opencb.commons.bioformats.variant.json.VariantInfo;
import org.opencb.commons.containers.QueryResult;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.storage.variant.mongodb.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.variant.VariantDBAdaptor;

@Path("/account/{accountId}/file/{jobId}")
public class VariantWSServer extends GenericWSServer {
    
    private final String accountId;
    private final String projectId;
    private final String jobId;

    public VariantWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                               @DefaultValue("") @PathParam("accountId") String accountId,
                               @DefaultValue("default") @QueryParam("projectId") String projectId,
                               @DefaultValue("") @PathParam("jobId") String jobId) throws IOException {
        super(uriInfo, httpServletRequest);

        this.accountId = accountId;
        this.projectId = projectId;
        this.jobId = jobId;
    }

    @POST
    @Path("region/{regions}/variants")
    public Response getVariantsByRegion(@DefaultValue("") @PathParam("regions") String regions) {
        // TODO getAllVariantsByRegionList
        List<Region> regionList = Region.parseRegions(regions);
        return null;
    }
    
    @POST
    @Path("/list")
    public Response listVariants(@DefaultValue("1") @QueryParam("page") String page,
                                 @DefaultValue("0") @PathParam("start") String start,
                                 @DefaultValue("25") @PathParam("limit") String limit) {
        return null;
    }

/*   
    @GET
    @Path("/variantsMongo")
    public Response getVariantsMongo() {
        Map<String, String> map = new LinkedHashMap<>();

        UriInfo info = uriInfo;

        MultivaluedMap<String, String> queryParams = info.getQueryParameters();
        for (Map.Entry<String, List<String>> entry : queryParams.entrySet()) {
            map.put(entry.getKey(), Joiner.on(",").join(entry.getValue()));
        }


        int page = (info.getQueryParameters().containsKey("page")) ? Integer.parseInt(info.getQueryParameters().getFirst("page")) : 1;
        int start = (info.getQueryParameters().containsKey("start")) ? Integer.parseInt(info.getQueryParameters().getFirst("start")) : 0;
        int limit = (info.getQueryParameters().containsKey("limit")) ? Integer.parseInt(info.getQueryParameters().getFirst("limit")) : 25;
        String callback = (info.getQueryParameters().containsKey("callback")) ? info.getQueryParameters().getFirst("callback") : "null";


        //map.put("start", start);


        map.put("studyId", accountId + "_-_" + this.jobId);

        System.out.println(map);
        MutableInt count = new MutableInt(-1);


        Properties prop = new Properties();
        prop.put("mongo_host", "mem15");
        prop.put("mongo_port", 27017);
        prop.put("mongo_db_name", "cibererStudies");
        prop.put("mongo_user", "user");
        prop.put("mongo_password", "pass");

        MongoCredentials credentials = new MongoCredentials(prop);
        VariantDBAdaptor vqm;
        String res = null;
        QueryResult<VariantInfo> queryResult = null;
        try {
            vqm = new VariantMongoDBAdaptor(credentials);
            queryResult = ((VariantMongoDBAdaptor) vqm).getRecordsMongo(page, start, limit, count, map);

            queryResult.setNumResults(count.intValue());
//            res = callback + "(" + jsonObjectMapper.writeValueAsString(queryResult) + ");";

            System.out.println(res);

            vqm.close();


        } catch (MasterNotRunningException | ZooKeeperConnectionException | UnknownHostException e) {
            e.printStackTrace();
        }


        return createOkResponse(queryResult);
    }
    
    @POST
    @Path("/effects")
    @Consumes("application/x-www-form-urlencoded")
    public Response getVariantEffects(@DefaultValue("") @QueryParam("filename") String filename, MultivaluedMap<String, String> postParams) {
        Map<String, String> map = new LinkedHashMap<>();

        for (Map.Entry<String, List<String>> entry : postParams.entrySet()) {
            map.put(entry.getKey(), Joiner.on(",").join(entry.getValue()));
        }

        System.out.println(map);

        java.nio.file.Path dataPath = cloudSessionManager.getJobFolderPath(accountId, projectId, Paths.get(this.jobId)).resolve(filename);

        System.out.println("dataPath = " + dataPath.toString());

        map.put("db_name", dataPath.toString());
        VariantDBAdaptor vqm = new VariantSqliteQueryBuilder();
        List<VariantEffect> list = vqm.getEffect(map);


        String res = null;
        try {
            res = jsonObjectMapper.writeValueAsString(list);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return createOkResponse(res);
    }

    @GET
    @Path("/variantInfoMongo")
    public Response getAnalysisInfoMongo() {

        String studyId = (accountId + "_-_" + this.jobId);

        Properties prop = new Properties();
        prop.put("mongo_host", "mem15");
        prop.put("mongo_port", 27017);
        prop.put("mongo_db_name", "cibererStudies");
        prop.put("mongo_user", "user");
        prop.put("mongo_password", "pass");

        MongoCredentials credentials = new MongoCredentials(prop);
        VariantDBAdaptor vqm;
        String res = null;
        QueryResult<VariantAnalysisInfo> queryResult = null;
        try {
            vqm = new VariantMongoDBAdaptor(credentials);

            queryResult = ((VariantMongoDBAdaptor) vqm).getAnalysisInfo(studyId);

            res = jsonObjectMapper.writeValueAsString(queryResult);

            vqm.close();

        } catch (MasterNotRunningException | ZooKeeperConnectionException | UnknownHostException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }


        return createOkResponse(queryResult);


    }
*/
}
