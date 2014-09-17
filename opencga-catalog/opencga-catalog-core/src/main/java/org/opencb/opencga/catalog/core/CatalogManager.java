package org.opencb.opencga.catalog.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.*;
import org.opencb.opencga.catalog.core.db.CatalogDBAdaptor;
import org.opencb.opencga.catalog.core.db.CatalogManagerException;
import org.opencb.opencga.catalog.core.db.CatalogMongoDBAdaptor;
import org.opencb.opencga.catalog.core.io.CatalogIOManagerException;
import org.opencb.opencga.catalog.core.io.PosixIOManager;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.lib.common.Config;

import org.opencb.opencga.lib.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

public class CatalogManager {

    private CatalogDBAdaptor catalogDBAdaptor;
//    private CatalogIOManager ioManager; //TODO: Generify API
    private PosixIOManager ioManager;

    protected static Logger logger = LoggerFactory.getLogger(CatalogManager.class);

    protected static ObjectMapper jsonObjectMapper;
    protected static ObjectWriter jsonObjectWriter;

    private Properties catalogProperties;

    public CatalogManager() throws IOException, CatalogIOManagerException{
        this(System.getenv("OPENCGA_HOME"));
    }

    public CatalogManager(String rootdir) throws IOException, CatalogIOManagerException {
//        catalogProperties = Config.getAccountProperties();

        Path path = Paths.get(rootdir, "conf", "catalog.properties");
        catalogProperties = new Properties();
        try {
            catalogProperties.load(Files.newInputStream(path));
        } catch (IOException e) {
            logger.error("Failed to load account.properties: " + e.getMessage());
        }

//        if (catalogProperties.getProperty("OPENCGA.ACCOUNT.MODE").equals("file")) {
//            catalogDBAdaptor = (CatalogDBAdaptor) new CatalogMongoDBAdaptor( --- );
//        } else {
//            catalogDBAdaptor = new CatalogMongoDBAdaptor(new MongoCredentials(catalogProperties));
//        }
        catalogDBAdaptor = new CatalogMongoDBAdaptor(new MongoCredentials(catalogProperties));
        ioManager = new PosixIOManager(rootdir);

        jsonObjectMapper = new ObjectMapper();
        jsonObjectWriter = jsonObjectMapper.writer();
    }

    /**
     * Getter path methods
     * ***************************
     */

    public Path getUserPath(String userId) throws CatalogIOManagerException {
        return ioManager.getUserPath(userId);
    }

    public Path getProjectPath(String userId, String projectId) throws CatalogIOManagerException {
        return ioManager.getProjectPath(userId, projectId);
    }

    public Path getFilePath(String userId, String projectId, String studyId, String relativeFilePath) throws CatalogIOManagerException {
        return ioManager.getFilePath(userId, projectId, studyId, relativeFilePath);
    }

//    public Path getJobFolderPath(String userId, String projectId, Path JobId) {
//        return ioManager.getJobFolderPath(userId, projectId, JobId);
//    }

    public Path getTmpPath() {
        return ioManager.getTmpPath();
    }

    public File getObjectFromBucket(String userId, String projectAlias, String studyAlias, Path filePath, String sessionId)
            throws CatalogManagerException, IOException {
        QueryResult queryResult = catalogDBAdaptor.getFile(userId, projectAlias, studyAlias, filePath, sessionId);
        if(queryResult.getNumResults() != 1){
            return null;
        } else {
            return (File) queryResult.getResult().get(0);
        }
    }

    /**
     * User methods
     * ***************************
     */

    public QueryResult createUser(User user)
            throws CatalogManagerException, CatalogIOManagerException, JsonProcessingException {
        checkParameter(user.getId(), "userId");
        checkParameter(user.getPassword(), "password");
        checkParameter(user.getName(), "name");
        checkEmail(user.getEmail());
//        checkParameter(sessionIp, "sessionIp");
//        Session session = new Session(sessionIp);

        try {
            ioManager.createUser(user.getId());
            return catalogDBAdaptor.createUser(user);
        } catch (CatalogIOManagerException | CatalogManagerException e) {
            ioManager.deleteUser(user.getId());
            throw e;
        }
    }

    public QueryResult<ObjectMap> loginAsAnonymous(String sessionIp) throws CatalogManagerException, CatalogIOManagerException, IOException {
        checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp);

        String userId = "anonymous_" + session.getId();


        ioManager.createAnonymousUser(userId);

        try {
            return catalogDBAdaptor.loginAsAnonymous(session);
        } catch (CatalogManagerException e) {
            ioManager.deleteUser(userId);
            throw e;
        }

    }

    public QueryResult<ObjectMap> login(String userId, String password, String sessionIp) throws CatalogManagerException, IOException {
        checkParameter(userId, "userId");
        checkParameter(password, "password");
        checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp);

        try {
            ioManager.createUser(userId);
        } catch (CatalogIOManagerException e) {

        }

        return catalogDBAdaptor.login(userId, password, session);
    }

    public QueryResult logout(String userId, String sessionId) throws CatalogManagerException, IOException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        return catalogDBAdaptor.logout(userId, sessionId);
    }

    public QueryResult logoutAnonymous(String sessionId) throws CatalogManagerException, CatalogIOManagerException {
        String userId = "anonymous_" + sessionId;
        System.out.println("-----> el userId del anonimo es: " + userId + " y la sesionId: " + sessionId);

        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");

        ioManager.deleteAnonymousUser(userId);
        return catalogDBAdaptor.logoutAnonymous(sessionId);
    }

    public QueryResult changePassword(String userId, String password, String nPassword1, String nPassword2, String sessionId)
            throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkParameter(password, "password");
        checkParameter(nPassword1, "nPassword1");
        checkParameter(nPassword2, "nPassword2");
        if (!nPassword1.equals(nPassword2)) {
            throw new CatalogManagerException("the new pass is not the same in both fields");
        }
        return catalogDBAdaptor.changePassword(userId, password, nPassword1, sessionId);
    }

    public QueryResult changeEmail(String userId, String nEmail, String sessionId) throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkEmail(nEmail);
        return catalogDBAdaptor.changeEmail(userId, sessionId, nEmail);
    }

    public QueryResult resetPassword(String userId, String email) throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkEmail(email);
        return catalogDBAdaptor.resetPassword(userId, email);
    }

    public QueryResult<User> getUserInfo(String userId, String lastActivity, String sessionId)
            throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        // lastActivity can be null
        return catalogDBAdaptor.getUser(userId, sessionId, lastActivity);
    }

//    public void deleteUser(String userId, String sessionId) throws CatalogManagerException,
//            CatalogIOManagerException {
//        // TODO
//    }

    /**
     * Project methods
     * ***************************
     */
    public QueryResult<Project> getAllProjects(String userId, String sessionId) throws CatalogManagerException, JsonProcessingException {
        return catalogDBAdaptor.getAllProjects(userId, sessionId);
    }

    public QueryResult<ObjectMap> createProject(String userId, Project project, String sessionId) throws CatalogManagerException,
            CatalogIOManagerException, JsonProcessingException {
        checkParameter(project.getName(), "projectName");
        checkParameter(project.getAlias(), "projectAlias");
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");

        ioManager.createProject(userId, project.getAlias());
        try {
            return catalogDBAdaptor.createProject(userId, project, sessionId);
        } catch (CatalogManagerException e) {
            ioManager.deleteProject(userId, project.getName());
            throw e;
        }
    }

    //TODO: Change name or alias? Create a generic modifier function?
    public QueryResult renameProject(String userId, String projectAlias, String newProjectAlias, String sessionId) throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(projectAlias, "projectAlias");
        checkParameter(newProjectAlias, "newProjectAlias");
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        ioManager.renameProject(userId, projectAlias, newProjectAlias);
        try {
            return catalogDBAdaptor.renameProject(userId, projectAlias, newProjectAlias, sessionId);
        } catch (CatalogManagerException e) {
            ioManager.renameProject(userId, newProjectAlias, projectAlias);
            throw e;
        }
    }

    /**
     * Study methods
     * ***************************
     */
    public QueryResult<Study> getAllStudies(String userId, String projectAlias, String sessionId) throws CatalogManagerException, JsonProcessingException {
        return catalogDBAdaptor.getAllStudies(userId, projectAlias, sessionId);
    }

    public QueryResult<ObjectMap> createStudy(String userId, String projectAlias, Study study, String sessionId) throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(study.getName(), "studyName");
        checkParameter(study.getAlias(), "studylias");
        checkParameter(projectAlias, "projectAlias");
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");

        ioManager.createStudy(userId, projectAlias, study.getAlias());
        try {
            return catalogDBAdaptor.createStudy(userId, projectAlias, study, sessionId);
        } catch (CatalogManagerException e) {
            ioManager.deleteStudy(userId, projectAlias, study.getAlias());
            throw e;
        }
    }

    //TODO: Change name or alias? Create a generic modifier function?
    public QueryResult renameStudy(String userId, String projectAlias, String studyAlias, String newStudAlias, String sessionId) throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(projectAlias, "projectAlias");
        checkParameter(studyAlias, "studyAlias");
        checkParameter(newStudAlias, "newStudAlias");
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        ioManager.renameProject(userId, studyAlias, newStudAlias);
        try {
            return catalogDBAdaptor.renameStudy(userId, projectAlias, studyAlias, newStudAlias, sessionId);
        } catch (CatalogManagerException e) {
            ioManager.renameProject(userId, newStudAlias, studyAlias);
            throw e;
        }
    }


//
//    public QueryResult<ObjectMap> createObjectToStudy(String userId, String projectAlias, String studyAlias, Path objectId, File file,
//                                            InputStream fileIs, boolean parents, String sessionId) throws CatalogManagerException,
//            CatalogIOManagerException, IOException, InterruptedException {
//        checkParameter(projectAlias, "projectAlias");
//        checkParameter(studyAlias, "studyAlias");
//        checkParameter(userId, "userId");
//        checkParameter(sessionId, "sessionId");
//        checkParameter(objectId.toString(), "objectId");
//        checkObj(file, "file");
//
//        file.setStatus("ready");
////        objectId = ioManager.createFile(userId, bucketId, objectId, file, fileIs, parents);
//
//        // set id and name to the itemObject
//        file.setUri(uri);
//
//        try {
//            return catalogDBAdaptor.createFileToStudy(userId, projectAlias, studyAlias, file, sessionId);
////            return objectId.toString();
//        } catch (CatalogManagerException e) {
//            ioManager.deleteFile(userId, projectAlias, studyAlias, objectId.toString());
//            throw e;
//        }
//    }

//    public QueryResult createFolderToStudy(String userId, String projectAlias, String studyAlias, Path objectId, File objectItem,
//                                            boolean parents, String sessionId) throws CatalogManagerException, CatalogIOManagerException, JsonProcessingException {
//        checkParameter(bucketId, "bucket");
//        checkParameter(userId, "userId");
//        checkParameter(sessionId, "sessionId");
//        checkParameter(objectId.toString(), "objectId");
//        checkObj(objectItem, "objectItem");
//
//        ioManager.createFolder(userId, projectAlias, studyAlias, objectId, parents);
//
//        // set id and name to the itemObject
//        objectItem.setId(objectId.toString());
//        objectItem.setFileName(objectId.getFileName().toString());
//
//        try {
//            return catalogDBAdaptor.createObjectToBucket(userId, bucketId, objectItem, sessionId);
////            return objectId.toString();
//        } catch (CatalogManagerException e) {
//            ioManager.deleteFile(bucketId, userId, objectId);
//            throw e;
//        }
//    }

//    public QueryResult refreshBucket(final String userId, final String bucketId, final String sessionId)
//            throws CatalogManagerException, IOException {
//
//        final Path bucketPath = ioManager.getBucketPath(userId, bucketId);
//        final List<ObjectItem> newObjects = new ArrayList<ObjectItem>();
//
//        Files.walkFileTree(bucketPath, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
//            @Override
//            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
//                String fileName = file.getFileName().toString();
//                if (!Files.isHidden(file) && !fileName.equals("sge_err.log") && !fileName.equals("sge_out.log") && !Files.isDirectory(file)) {
//                    Path ojectId = bucketPath.relativize(file);
////                    logger.info(ojectId);
//                    ObjectItem objectItem = null;
//                    try {//find the current object if already exists
//                        objectItem = catalogDBAdaptor.getObjectFromBucket(userId, bucketId, ojectId, sessionId);
//                    } catch (CatalogManagerException e) {
//                        objectItem = new ObjectItem(ojectId.toString(), ojectId.getFileName().toString(), "r");
//                        String fileExt = IOUtils.getExtension(ojectId.toString());
//                        if (fileExt != null) {
//                            objectItem.setFileFormat(fileExt.substring(1));
//                        }
//                        objectItem.setStatus("");
//                    }
//                    newObjects.add(objectItem);
//                    return FileVisitResult.CONTINUE;
//                } else {
//                    return FileVisitResult.CONTINUE;
//                }
//            }
//
//            @Override
//            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
//                // try to delete the file anyway, even if its attributes
//                // could not be read, since delete-only access is
//                // theoretically possible
//                return FileVisitResult.SKIP_SUBTREE;
//            }
//
//            @Override
//            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
//                if (Files.isHidden(dir) || !Files.isReadable(dir) || dir.getFileName().toString().equals("..")
//                        || dir.getFileName().toString().equals(".")) {
//                    return FileVisitResult.SKIP_SUBTREE;
//                }
//                if (!dir.equals(bucketPath)) {//dont add bucketId folder itself
//                    Path ojectId = bucketPath.relativize(dir);
////                    logger.info(bucketId);
////                    logger.info(ojectId);
////                    logger.info(dir.toString());
//
//                    ObjectItem objectItem = null;
//                    try {//find the current object if already exists
//                        objectItem = catalogDBAdaptor.getObjectFromBucket(userId, bucketId, ojectId, sessionId);
//                    } catch (CatalogManagerException e) {
//                        objectItem = new ObjectItem(ojectId.toString(), ojectId.getFileName().toString(), "dir");
//                        objectItem.setFileFormat("dir");
//                        objectItem.setStatus("");
//                    }
//                    newObjects.add(objectItem);
//                }
//                return FileVisitResult.CONTINUE;
//            }
//
//            @Override
//            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
//                if (Files.isHidden(dir) || !Files.isReadable(dir)) {
//                    return FileVisitResult.SKIP_SUBTREE;
//                }
//                // here
//                return FileVisitResult.CONTINUE;
//            }
//        });
//
//        catalogDBAdaptor.deleteObjectsFromBucket(userId, bucketId, sessionId);
//        for (ObjectItem objectItem : newObjects) {
//            catalogDBAdaptor.createObjectToBucket(userId, bucketId, objectItem, sessionId);
//        }
//
//        ObjectMap resultObjectMap = new ObjectMap();
//        QueryResult<ObjectMap> result = new QueryResult();
//        resultObjectMap.put("msg", "bucket refreshed");
//        result.setResult(Arrays.asList(resultObjectMap));
//        result.setNumResults(1);
//
//        return result;
//    }
//
    public QueryResult deleteDataFromStudy(String userId, String projectAlias, String studyAlias, Path objectId, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(projectAlias, "projectAlias");
        checkParameter(studyAlias, "studyAlias");
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkParameter(objectId.toString(), "objectId");

        ioManager.deleteFile(userId, projectAlias, studyAlias, objectId.toString());
        return catalogDBAdaptor.deleteFile(userId, projectAlias, studyAlias, objectId, sessionId);
    }
//
//    public DataInputStream getFileObjectFromBucket(String userId, String bucketId, Path objectId, String sessionId, String start, String limit)
//            throws CatalogIOManagerException, IOException, CatalogManagerException {
//        checkParameter(bucketId, "bucket");
//        checkParameter(userId, "userId");
//        checkParameter(sessionId, "sessionId");
//        checkParameter(objectId.toString(), "objectId");
//        checkParameter(start, "start");
//        checkParameter(limit, "limit");
//
//        return ioManager.getFileObject(userId, bucketId, objectId, start, limit);
//    }
//
//    public DataInputStream getGrepFileObjectFromBucket(String userId, String bucketId, Path objectId, String sessionId, String pattern, boolean ignoreCase, boolean multi)
//            throws CatalogIOManagerException, IOException, CatalogManagerException {
//        checkParameter(bucketId, "bucket");
//        checkParameter(userId, "userId");
//        checkParameter(sessionId, "sessionId");
//        checkParameter(objectId.toString(), "objectId");
//        checkParameter(pattern, "pattern");
//
//        return ioManager.getGrepFileObject(userId, bucketId, objectId, pattern, ignoreCase, multi);
//    }
//
//
//    //TODO
//    public void shareObject(String userId, String bucketId, Path objectId, String toAccountId, boolean read,
//                            boolean write, boolean execute, String sessionId) throws CatalogManagerException {
//        checkParameters(userId, "userId", bucketId, "bucketId", objectId.toString(), "objectId", toAccountId,
//                "toAccountId", sessionId, "sessionId");
//
//        Acl acl = new Acl(toAccountId, "", read, write, execute);
//        catalogDBAdaptor.shareObject(userId, bucketId, objectId, acl, sessionId);
//    }
//
//
////    public String fetchData(Path objectId, String fileFormat, String regionStr, Map<String, List<String>> params) throws Exception {
////        checkParameter(objectId.toString(), "objectId");
////        checkParameter(regionStr, "regionStr");
////
////        String result = "";
////        switch (fileFormat) {
////            case "bam":
////                result = fetchAlignmentData(objectId, regionStr, params);
////                break;
////            case "vcf":
////                result = fetchVariationData(objectId, regionStr, params);
////                break;
////            default:
////                throw new IllegalArgumentException("File format " + fileFormat + " not yet supported");
////        }
////
////        return result;
////    }
//
//    public QueryResult fetchAlignmentData(Path objectPath, String regionStr, Map<String, List<String>> params) throws Exception {
//        AlignmentQueryBuilder queryBuilder = new TabixAlignmentQueryBuilder(new SqliteCredentials(objectPath), null, null);
//        Region region = Region.parseRegion(regionStr);
//        QueryOptions options = new QueryOptions(params, true);
//        QueryResult queryResult = null;
//
//        boolean includeHistogram = params.containsKey("histogram") && Boolean.parseBoolean(params.get("histogram").get(0));
//        boolean includeAlignments = params.containsKey("alignments") && Boolean.parseBoolean(params.get("alignments").get(0));
//        boolean includeCoverage = params.containsKey("coverage") && Boolean.parseBoolean(params.get("coverage").get(0));
//
//        if (includeHistogram) { // Query the alignments' histogram: QueryResult<ObjectMap>
//            queryResult = queryBuilder.getAlignmentsHistogramByRegion(region,
//                    params.containsKey("histogramLogarithm") ? Boolean.parseBoolean(params.get("histogram").get(0)) : false,
//                    params.containsKey("histogramMax") ? Integer.parseInt(params.get("histogramMax").get(0)) : 500);
//
//        } else if ((includeAlignments && includeCoverage) ||
//                (!includeAlignments && !includeCoverage)) { // If both or none requested: QueryResult<AlignmentRegion>
//            queryResult = queryBuilder.getAlignmentRegionInfo(region, options);
//
//        } else if (includeAlignments) { // Query the alignments themselves: QueryResult<Alignment>
//            queryResult = queryBuilder.getAllAlignmentsByRegion(region, options);
//
//        } else if (includeCoverage) { // Query the alignments' coverage: QueryResult<RegionCoverage>
//            queryResult = queryBuilder.getCoverageByRegion(region, options);
//        }
//
//        return queryResult;
//    }
//
//
//    public String fetchVariationData(Path objectPath, String regionStr, Map<String, List<String>> params) throws Exception {
//        VcfManager vcfManager = new VcfManager();
//        return vcfManager.getByRegion(objectPath, regionStr, params);
//    }
//
///*
//    public QueryResult fetchVariationData(Path objectPath, String regionStr, Map<String, List<String>> params) throws Exception {
//        String species = params.containsKey("species") ? params.get("species").get(0) : "hsapiens";
//        VariantDBAdaptor queryBuilder = null;
//                //new VariantMonbaseQueryBuilder(species,
//                //new MonbaseCredentials("172.24.79.30", 60010, "172.24.79.30", 2181, "localhost", 9999, "variants_" + species, "cgonzalez", "cgonzalez"));
//                new VariantSqliteQueryBuilder(new SqliteCredentials(objectPath));
//        Region region = Region.parseRegion(regionStr);
//        QueryOptions options = new QueryOptions(params, true);
//        QueryResult queryResult = null;
//
//        boolean includeHistogram = params.containsKey("histogram") && Boolean.parseBoolean(params.get("histogram").get(0));
//        boolean includeVariants = params.containsKey("variants") && Boolean.parseBoolean(params.get("variants").get(0));
//        boolean includeStats = params.containsKey("stats") && Boolean.parseBoolean(params.get("stats").get(0));
//        boolean includeEffects = params.containsKey("effects") && Boolean.parseBoolean(params.get("effects").get(0));
//        String studyName = params.containsKey("study") ? params.get("study").toString() : "";
//        if (studyName.equals("")) { // TODO In the future, it will represent that we want to retrieve info from all studies
//            return new QueryResult(regionStr);
//        }
//
//        if (includeHistogram) { // Query the alignments' histogram: QueryResult<ObjectMap>
//            // TODO
//            queryResult = queryBuilder.getVariantsHistogramByRegion(region, studyName,
//                    params.containsKey("histogramLogarithm") ? Boolean.parseBoolean(params.get("histogram").get(0)) : false,
//                    params.containsKey("histogramMax") ? Integer.parseInt(params.get("histogramMax").get(0)) : 500);
//
//        } else if (includeVariants) {
//            // TODO in SQLite
//            queryResult = queryBuilder.getAllVariantsByRegion(region, studyName, options);
//        } else if (includeStats && !includeEffects) {
//
//        } else if (!includeStats && includeEffects) {
//
//        }
//
//        return queryResult;
//    }
//*/
//
//    public String indexFileObject(String userId, String bucketId, Path objectId, boolean force, String sessionId) throws Exception {
//        ObjectItem objectItem = catalogDBAdaptor.getObjectFromBucket(userId, bucketId, objectId, sessionId);
//        if (objectItem.getStatus().contains("indexer")) {
//            return "indexing...";
//        }
//        String sgeJobName = "ready";
//        boolean indexReady;
//        switch (objectItem.getFileFormat()) {
//            case "bam":
//                indexReady = BamManager.checkIndex(ioManager.getFilePath(userId, bucketId, objectId));
//                if (force || !indexReady) {
//                    sgeJobName = BamManager.createIndex(getFilePath(userId, bucketId, objectId));
//                    catalogDBAdaptor.setObjectStatus(userId, bucketId, objectId, sgeJobName, sessionId);
//                }
//                break;
//            case "vcf":
//                indexReady = VcfManager.checkIndex(ioManager.getFilePath(userId, bucketId, objectId));
//                if (force || !indexReady) {
//                    sgeJobName = VcfManager.createIndex(getFilePath(userId, bucketId, objectId));
//                    catalogDBAdaptor.setObjectStatus(userId, bucketId, objectId, sgeJobName, sessionId);
//                }
//                break;
//        }
//
//        return sgeJobName;
//    }
//
//    public String indexFileObjectStatus(String userId, String bucketId, Path objectId, String sessionId, String jobId) throws Exception {
//        checkParameter(jobId, "jobId");
//        logger.info(jobId);
//        String objectStatus = catalogDBAdaptor.getObjectFromBucket(userId, bucketId, objectId, sessionId).getStatus();
//        logger.info(objectStatus);
////        String jobStatus = SgeManager.status(jobId);
//        String jobStatus = "finished";
//        logger.info(jobStatus);
//        if (jobStatus.equalsIgnoreCase("finished")) {
//            objectStatus = objectStatus.replace("indexer_", "index_finished_");
//            logger.info(objectStatus);
//            catalogDBAdaptor.setObjectStatus(userId, bucketId, objectId, objectStatus, sessionId);
//        }
//        return jobStatus;
//    }
//
//    /**
//     * Project methods
//     * ***************************
//     */
//    public QueryResult getAllProjects(String userId, String sessionId) throws CatalogManagerException {
//        return catalogDBAdaptor.getAllProjects(userId, sessionId);
//    }
//
//    public QueryResult createProject(String userId, Project project, String sessionId) throws CatalogManagerException,
//            CatalogIOManagerException, JsonProcessingException {
//        checkParameter(project.getId(), "projectName");
//        checkParameter(userId, "userId");
//        checkParameter(sessionId, "sessionId");
//
//        ioManager.createProject(userId, project.getId());
//        try {
//            return catalogDBAdaptor.createProject(userId, project, sessionId);
//        } catch (CatalogManagerException e) {
//            ioManager.deleteProject(userId, project.getId());
//            throw e;
//        }
//    }
//
//    public String checkJobStatus(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException {
//        return catalogDBAdaptor.getJobStatus(userId, jobId, sessionId);
//    }
//
//    public void incJobVisites(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException {
//        catalogDBAdaptor.incJobVisites(userId, jobId, sessionId);
//    }
//
//    public QueryResult deleteJob(String userId, String projectId, String jobId, String sessionId)
//            throws CatalogManagerException, CatalogIOManagerException {
//        checkParameter(userId, "userId");
//        checkParameter(projectId, "projectId");
//        checkParameter(jobId, "jobId");
//        checkParameter(sessionId, "sessionId");
//
//        try {
//            ioManager.deleteJob(userId, projectId, jobId);
//        } catch (CatalogIOManagerException e) {
//            logger.info(e.toString());
//        }
//        return catalogDBAdaptor.deleteJobFromProject(userId, projectId, jobId, sessionId);
//    }
//
//    public String getJobResult(String userId, String jobId, String sessionId) throws IOException, CatalogIOManagerException, CatalogManagerException {
//        checkParameter(userId, "userId");
//        checkParameter(jobId, "jobId");
//
//        Path jobPath = getUserPath(userId).resolve(catalogDBAdaptor.getJobPath(userId, jobId, sessionId));
////        return ioManager.getJobResult(jobPath);
//        return "DEPRECATED";
//    }
//
//    public Job getJob(String userId, String jobId, String sessionId) throws IOException, CatalogIOManagerException, CatalogManagerException {
//        checkParameter(userId, "userId");
//        checkParameter(jobId, "jobId");
//
//        return catalogDBAdaptor.getJob(userId, jobId, sessionId);
//    }
//
//    public String getFileTableFromJob(String userId, String jobId, String filename, String start, String limit,
//                                      String colNames, String colVisibility, String sort, String sessionId)
//            throws CatalogIOManagerException, IOException, CatalogManagerException {
//        checkParameter(userId, "userId");
//        checkParameter(jobId, "jobId");
//        checkParameter(filename, "filename");
//
//        Path jobPath = getUserPath(userId).resolve(catalogDBAdaptor.getJobPath(userId, jobId, sessionId));
//
//        return ioManager.getFileTableFromJob(jobPath, filename, start, limit, colNames, colVisibility, sort);
//    }
//
//    public DataInputStream getFileFromJob(String userId, String jobId, String filename, String zip, String sessionId)
//            throws CatalogIOManagerException, IOException, CatalogManagerException {
//        checkParameter(userId, "userId");
//        checkParameter(jobId, "jobId");
//        checkParameter(filename, "filename");
//        checkParameter(zip, "zip");
//
//        Path jobPath = getUserPath(userId).resolve(catalogDBAdaptor.getJobPath(userId, jobId, sessionId));
//
//        return ioManager.getFileFromJob(jobPath, filename, zip);
//    }
//
//
//    public DataInputStream getGrepFileFromJob(String userId, String jobId, String filename, String pattern, boolean ignoreCase, boolean multi, String sessionId)
//            throws CatalogIOManagerException, IOException, CatalogManagerException {
//        checkParameter(userId, "userId");
//        checkParameter(jobId, "jobId");
//        checkParameter(filename, "filename");
//        checkParameter(pattern, "pattern");
//        checkParameter(sessionId, "sessionId");
//
//
//        Path jobPath = getUserPath(userId).resolve(catalogDBAdaptor.getJobPath(userId, jobId, sessionId));
//
//        return ioManager.getGrepFileFromJob(jobPath, filename, pattern, ignoreCase, multi);
//    }
//
//    public InputStream getJobZipped(String userId, String jobId, String sessionId) throws CatalogIOManagerException,
//            IOException, CatalogManagerException {
//        checkParameter(userId, "userId");
//        checkParameter(jobId, "jobId");
//        checkParameter(sessionId, "sessionId");
//
//        Path jobPath = getUserPath(userId).resolve(catalogDBAdaptor.getJobPath(userId, jobId, sessionId));
//        logger.info("getJobZipped");
//        logger.info(jobPath.toString());
//        logger.info(jobId);
//        return ioManager.getJobZipped(jobPath, jobId);
//    }
//
//    public QueryResult createJob(String jobName, String projectId, String jobFolder, String toolName, List<String> dataList,
//                                 String commandLine, String sessionId) throws CatalogManagerException, CatalogIOManagerException, JsonProcessingException {
//
//        checkParameter(jobName, "jobName");
//        checkParameter(projectId, "projectId");
//        checkParameter(toolName, "toolName");
//        checkParameter(sessionId, "sessionId");
//        String userId = catalogDBAdaptor.getAccountIdBySessionId(sessionId);
//
//        String jobId = StringUtils.randomString(15);
//        boolean jobFolderCreated = false;
//
//        if (jobFolder == null) {
//            ioManager.createJob(userId, projectId, jobId);
//            jobFolder = Paths.get("projects", projectId).resolve(jobId).toString();
//            jobFolderCreated = true;
//        }
//        checkParameter(jobFolder, "jobFolder");
//
//        Job job = new Job(jobId, jobName, jobFolder, toolName, Job.QUEUED, commandLine, "", dataList);
//
//        try {
//            return catalogDBAdaptor.createJob(userId, projectId, job, sessionId);
//        } catch (CatalogManagerException e) {
//            if (jobFolderCreated) {
//                ioManager.deleteJob(userId, projectId, jobId);
//            }
//            throw e;
//        }
//    }
//
//    public QueryResult getJobFolder(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException {
//        String projectId = catalogDBAdaptor.getJobProject(userId, jobId, sessionId).getId();
//        String jobFolder = ioManager.getJobPath(userId, projectId, null, jobId).toString();
//
//
//        QueryResult<String> result = new QueryResult();
//        result.setResult(Arrays.asList(jobFolder));
//        result.setNumResults(1);
//
//        return result;
//    }
//
//    public List<AnalysisPlugin> getUserAnalysis(String sessionId) throws CatalogManagerException, IOException {
//        return catalogDBAdaptor.getUserAnalysis(sessionId);
//    }
//
//    public void setJobCommandLine(String userId, String jobId, String commandLine, String sessionId)
//            throws CatalogManagerException, IOException {
//        catalogDBAdaptor.setJobCommandLine(userId, jobId, commandLine, sessionId);// this
//        // method
//        // increases
//        // visites
//        // by 1
//        // in
//        // mongodb
//    }
//
    /**
     * ****************
     */
    private void checkEmail(String email) throws CatalogManagerException {
        String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
                + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
        Pattern pattern = Pattern.compile(EMAIL_PATTERN);
        if (!pattern.matcher(email).matches()) {
            throw new CatalogManagerException("email not valid");
        }
    }

    private void checkParameter(String param, String name) throws CatalogManagerException {
        if (param == null || param.equals("") || param.equals("null")) {
            throw new CatalogManagerException("Error in parameter: parameter '" + name + "' is null or empty: "
                    + param + ".");
        }
    }

    private void checkParameters(String... args) throws CatalogManagerException {
        if (args.length % 2 == 0) {
            for (int i = 0; i < args.length; i += 2) {
                checkParameter(args[i], args[i + 1]);
            }
        } else {
            throw new CatalogManagerException("Error in parameter: parameter list is not multiple of 2");
        }
    }

    private void checkObj(Object obj, String name) throws CatalogManagerException {
        if (obj == null) {
            throw new CatalogManagerException("parameter '" + name + "' is null.");
        }
    }

    private void checkRegion(String regionStr, String name) throws CatalogManagerException {
        if (Pattern.matches("^([a-zA-Z0-9])+:([0-9])+-([0-9])+$", regionStr)) {//chr:start-end
            throw new CatalogManagerException("region '" + name + "' is not valid");
        }
    }

}
