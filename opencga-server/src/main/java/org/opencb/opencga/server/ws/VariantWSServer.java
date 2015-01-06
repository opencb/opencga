package org.opencb.opencga.server.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.storage.variant.json.VariantSourceEntryJsonMixin;
import org.opencb.opencga.storage.variant.json.GenotypeJsonMixin;
import org.opencb.opencga.storage.variant.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.variant.json.VariantStatsJsonMixin;
import org.opencb.opencga.storage.variant.mongodb.VariantMongoDBAdaptor;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Path("/account/{accountId}/variant/{studyId}/{fileId}")
public class VariantWSServer extends GenericWSServer {

    private final String accountId;
    private final String studyId;
    private final String fileId;

    private static VariantMongoDBAdaptor variantMongoDbAdaptor;
    private static MongoCredentials credentials;
    private static final int MAX_REGION = 1000000;

    static {

        try {

            String host = properties.getProperty("VARIANT.STORAGE.HOST");
            int port = Integer.parseInt(properties.getProperty("VARIANT.STORAGE.PORT"));
            String db = properties.getProperty("VARIANT.STORAGE.DB");
            String user = properties.getProperty("VARIANT.STORAGE.USER");
            String pass = properties.getProperty("VARIANT.STORAGE.PASS");

            credentials = new MongoCredentials(host, port, db, user, pass);
            variantMongoDbAdaptor = new VariantMongoDBAdaptor(credentials, "variants", "files");


            jsonObjectMapper = new ObjectMapper();
            jsonObjectMapper.addMixInAnnotations(VariantSourceEntry.class, VariantSourceEntryJsonMixin.class);
            jsonObjectMapper.addMixInAnnotations(Genotype.class, GenotypeJsonMixin.class);
            jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);
            jsonObjectMapper.addMixInAnnotations(VariantSource.class, VariantSourceJsonMixin.class);
            jsonObjectWriter = jsonObjectMapper.writer();

        } catch (IllegalOpenCGACredentialsException | UnknownHostException e) {
            e.printStackTrace();
        }

    }


    public VariantWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                           @DefaultValue("") @PathParam("accountId") String accountId,
                           @DefaultValue("") @PathParam("studyId") String studyId,
                           @DefaultValue("") @PathParam("fileId") String fileId) throws IOException, IllegalOpenCGACredentialsException {
        super(uriInfo, httpServletRequest);

        this.accountId = accountId;
        this.studyId = studyId;
        this.fileId = fileId;


    }

    @OPTIONS
    @Path("/fetch")
    public Response getVariantsOpt() {
        return createOkResponse("");
    }

    @GET
    @Path("/fetch")
    public Response getVariants(@QueryParam("region") String region,
                                @QueryParam("ref") String reference,
                                @QueryParam("alt") String alternate,
                                @QueryParam("effects") String effects,
                                @QueryParam("studies") String studies,
                                @DefaultValue("-1f") @QueryParam("maf") float maf,
                                @DefaultValue("-1") @QueryParam("miss_alleles") int missingAlleles,
                                @DefaultValue("-1") @QueryParam("miss_gts") int missingGenotypes,
                                @QueryParam("maf_op") String mafOperator,
                                @QueryParam("miss_alleles_op") String missingAllelesOperator,
                                @QueryParam("miss_gts_op") String missingGenotypesOperator,
                                @QueryParam("type") String variantType,
                                @DefaultValue("false") @QueryParam("histogram") boolean histogram,
                                @DefaultValue("-1") @QueryParam("histogram_interval") int interval
    ) {


        if (reference != null && !reference.isEmpty()) {
            queryOptions.put("reference", reference);
        }
        if (alternate != null && !alternate.isEmpty()) {
            queryOptions.put("alternate", alternate);
        }
        if (effects != null && !effects.isEmpty()) {
            queryOptions.put("effect", Arrays.asList(effects.split(",")));
        }
        if (studies != null && !studies.isEmpty()) {
            queryOptions.put("studies", Arrays.asList(studies.split(",")));
        }
        if (variantType != null && !variantType.isEmpty()) {
            queryOptions.put("type", variantType);
        }
        if (maf >= 0) {
            queryOptions.put("maf", maf);
            if (mafOperator != null) {
                queryOptions.put("opMaf", mafOperator);
            }
        }
        if (missingAlleles >= 0) {
            queryOptions.put("missingAlleles", missingAlleles);
            if (missingAllelesOperator != null) {
                queryOptions.put("opMissingAlleles", missingAllelesOperator);
            }
        }
        if (missingGenotypes >= 0) {
            queryOptions.put("missingGenotypes", missingGenotypes);
            if (missingGenotypesOperator != null) {
                queryOptions.put("opMissingGenotypes", missingGenotypesOperator);
            }
        }

        // Parse the provided regions. The total size of all regions together
        // can't excede 1 million positions
        int regionsSize = 0;
        List<Region> regions = new ArrayList<>();
        for (String s : region.split(",")) {
            Region r = Region.parseRegion(s);
            regions.add(r);
            regionsSize += r.getEnd() - r.getStart();
        }

        if (histogram) {
            if (regions.size() > 1) {
                return createErrorResponse("Sorry, histogram functionality only works with a single region");
            } else {
                if (interval > 0) {
                    queryOptions.put("interval", interval);
                }
                return createOkResponse(variantMongoDbAdaptor.getVariantsHistogramByRegion(regions.get(0), queryOptions));
            }
        } else if (regionsSize <= MAX_REGION) {
            List<QueryResult> allVariantsByRegionList = variantMongoDbAdaptor.getAllVariantsByRegionList(regions, queryOptions);
            System.out.println("allVariantsByRegionList = " + allVariantsByRegionList);
            return createOkResponse(allVariantsByRegionList);
        } else {
            return createErrorResponse("The total size of all regions provided can't exceed " + MAX_REGION + " positions. "
                    + "If you want to browse a larger number of positions, please provide the parameter 'histogram=true'");
        }
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


        map.put("studyId", accountId + "_-_" + this.fileId);

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

        java.nio.file.Path dataPath = cloudSessionManager.getJobFolderPath(accountId, studyId, Paths.get(this.fileId)).resolve(filename);

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

        String studyId = (accountId + "_-_" + this.fileId);

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
