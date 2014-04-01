package org.opencb.opencga.server.ws;

import org.opencb.commons.containers.QueryResult;
import org.opencb.opencga.account.beans.Job;
import org.opencb.opencga.analysis.AnalysisJobExecuter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

@Path("/account/{accountId}/analysis/job/{jobId}")
public class JobAnalysisWSServer extends GenericWSServer {
    private String accountId;
    private String projectId;
    private String jobId;

    public JobAnalysisWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                               @DefaultValue("") @PathParam("accountId") String accountId,
                               @DefaultValue("default") @QueryParam("projectId") String projectId,
                               @DefaultValue("") @PathParam("jobId") String jobId) throws IOException {
        super(uriInfo, httpServletRequest);

        this.accountId = accountId;
        this.projectId = projectId;
        this.jobId = jobId;
    }

    @GET
    @Path("/result.{format}")
    public Response getResultFile(@PathParam("format") String format) {
        try {
            String res = cloudSessionManager.getJobResult(accountId, jobId, sessionId);
            return createOkResponse(res);
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/table")
    public Response table(@DefaultValue("") @QueryParam("filename") String filename,
                          @DefaultValue("") @QueryParam("start") String start, @DefaultValue("") @QueryParam("limit") String limit,
                          @DefaultValue("") @QueryParam("colNames") String colNames,
                          @DefaultValue("") @QueryParam("colVisibility") String colVisibility,
                          @DefaultValue("") @QueryParam("callback") String callback,
                          @QueryParam("sort") @DefaultValue("false") String sort) {

        try {
            String res = cloudSessionManager.getFileTableFromJob(accountId, jobId, filename, start, limit, colNames,
                    colVisibility, callback, sort, sessionId);
            return createOkResponse(res, MediaType.valueOf("text/javascript"));
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/poll")
    public Response pollJobFile(@DefaultValue("") @QueryParam("filename") String filename,
                                @DefaultValue("true") @QueryParam("zip") String zip) {

        try {
            DataInputStream is = cloudSessionManager.getFileFromJob(accountId, jobId, filename, zip, sessionId);
            String name = null;
            if (zip.compareTo("true") != 0) {// PAKO zip != true
                name = filename;
            } else {
                name = filename + ".zip";
            }
            return createOkResponse(is, MediaType.APPLICATION_OCTET_STREAM_TYPE, name);
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/status")
    public Response getJobStatus() {
        try {
            String res = cloudSessionManager.checkJobStatus(accountId, jobId, sessionId);
            return createOkResponse(res);
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/delete")
    public Response deleteJob() {
        try {
            QueryResult result = cloudSessionManager.deleteJob(accountId, projectId, jobId, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/download")
    public Response downloadJob() {
        try {
            InputStream is = cloudSessionManager.getJobZipped(accountId, jobId, sessionId);
            return createOkResponse(is, MediaType.valueOf("application/zip"), jobId + ".zip");
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/result.js")
    public Response getResult() {
        try {

            // AnalysisJobExecuter aje = new AnalysisJobExecuter(analysis);
            // cloudSessionManager.get
            Job job = cloudSessionManager.getJob(accountId, jobId, sessionId);
            AnalysisJobExecuter aje = new AnalysisJobExecuter(job.getToolName());
            InputStream is = aje.getResultInputStream();
            cloudSessionManager.incJobVisites(accountId, jobId, sessionId);
            return createOkResponse(is, MediaType.valueOf("text/javascript"), "result.js");

            // String resultToUse = aje.getResult();
            // String jobObj = cloudSessionManager.getJobObject(accountId,
            // jobId);
            // StringBuilder sb = new StringBuilder();
            // String c = "\"";
            // sb.append("{");
            // sb.append(c + "result" + c + ":"+ c + resultJson + c + ",");
            // sb.append(c + "resultToUse" + c + ":"+ c + resultToUse + c +
            // ",");
            // sb.append(c + "job" + c + ":" + c + jobObj + c);
            // sb.append("}");
            // return createOkResponse(sb.toString());
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse("can not get result json.");
        }
    }
/*
    // TODO Find place for this webservices
    //VARIANT EXPLORER WS
    @POST
    @Path("/variants")
    @Consumes("application/x-www-form-urlencoded")
    public Response getVariantInfo(@DefaultValue("") @QueryParam("filename") String filename, MultivaluedMap<String, String> postParams) {
        Map<String, String> map = new LinkedHashMap<>();

        for (Map.Entry<String, List<String>> entry : postParams.entrySet()) {
            map.put(entry.getKey(), Joiner.on(",").join(entry.getValue()));
//            map.put(entry.getKey(), StringUtils.join(entry.getValue(), ","));
        }

        System.out.println(map);

        java.nio.file.Path dataPath = cloudSessionManager.getJobFolderPath(accountId, projectId, Paths.get(this.jobId)).resolve(filename);

        System.out.println("dataPath = " + dataPath.toString());

        map.put("db_name", dataPath.toString());
        VariantQueryBuilder vqm = new VariantSqliteQueryBuilder();
        List<VariantInfo> list = vqm.getRecords(map);


        String res = null;
        try {
            res = jsonObjectMapper.writeValueAsString(list);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return createOkResponse(res);
    }

    //    @Consumes("application/x-www-form-urlencoded")
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
//        String callback = (info.getQueryParameters().containsKey("callback")) ? info.getQueryParameters().getFirst("callback") : "null";

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
        VariantQueryBuilder vqm;
        String res = null;
        QueryResult<VariantInfo> queryResult = null;
        try {
            vqm = new VariantMongoQueryBuilder(credentials);
            queryResult = ((VariantMongoQueryBuilder) vqm).getRecordsMongo(page, start, limit, count, map);

            queryResult.setNumResults(count.intValue());
            vqm.close();

        } catch (MasterNotRunningException | ZooKeeperConnectionException | UnknownHostException e) {
            e.printStackTrace();
        }
        return createOkResponse(queryResult);
    }

    @POST
    @Path("/variant_effects")
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
        VariantQueryBuilder vqm = new VariantSqliteQueryBuilder();
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
    @Path("/variant_info")
    public Response getAnalysisInfo(@DefaultValue("") @QueryParam("filename") String filename) {

        Map<String, String> map = new LinkedHashMap<>();

        java.nio.file.Path dataPath = cloudSessionManager.getJobFolderPath(accountId, projectId, Paths.get(this.jobId)).resolve(filename);
        map.put("db_name", dataPath.toString());

        VariantQueryBuilder vqm = new VariantSqliteQueryBuilder();
        VariantAnalysisInfo vi = vqm.getAnalysisInfo(map);


        String res = null;
        try {
            res = jsonObjectMapper.writeValueAsString(vi);

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
        VariantQueryBuilder vqm;
        String res = null;
        QueryResult<VariantAnalysisInfo> queryResult = null;
        try {
            vqm = new VariantMongoQueryBuilder(credentials);

            queryResult = ((VariantMongoQueryBuilder) vqm).getAnalysisInfo(studyId);

            res = jsonObjectMapper.writeValueAsString(queryResult);

            vqm.close();

        } catch (MasterNotRunningException | ZooKeeperConnectionException | UnknownHostException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }


        return createOkResponse(queryResult);


    }

    @POST
    @Path("/variant_stats")
    @Consumes("application/x-www-form-urlencoded")
    public Response getVariantStats(@DefaultValue("") @QueryParam("filename") String filename, MultivaluedMap<String, String> postParams) {

        Map<String, String> map = new LinkedHashMap<>();

        for (Map.Entry<String, List<String>> entry : postParams.entrySet()) {
            map.put(entry.getKey(), Joiner.on(",").join(entry.getValue()));
        }

        System.out.println(map);

        java.nio.file.Path dataPath = cloudSessionManager.getJobFolderPath(accountId, projectId, Paths.get(this.jobId)).resolve(filename);

        System.out.println("dataPath = " + dataPath.toString());

        map.put("db_name", dataPath.toString());
        VariantQueryBuilder vqm = new VariantSqliteQueryBuilder();
        List<VariantStats> list = vqm.getRecordsStats(map);

        String res = null;
        try {
            res = jsonObjectMapper.writeValueAsString(list);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return createOkResponse(res);
    }
*/
}
