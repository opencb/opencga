package org.opencb.opencga.account;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.opencga.account.beans.*;
import org.opencb.opencga.account.db.AccountFileManager;
import org.opencb.opencga.account.db.AccountManagementException;
import org.opencb.opencga.account.db.AccountManager;
import org.opencb.opencga.account.db.AccountMongoDBManager;
import org.opencb.opencga.account.io.FileIOManager;
import org.opencb.opencga.account.io.IOManagementException;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.IOUtils;
import org.opencb.opencga.lib.common.StringUtils;
import org.opencb.opencga.storage.datamanagers.VcfManager;
import org.opencb.opencga.storage.datamanagers.bam.BamManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.regex.Pattern;
import org.opencb.cellbase.core.common.Region;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.ObjectMap;
import org.opencb.opencga.lib.auth.SqliteCredentials;
import org.opencb.opencga.storage.alignment.AlignmentQueryBuilder;
import org.opencb.opencga.storage.alignment.TabixAlignmentQueryBuilder;

public class CloudSessionManager {

    private AccountManager accountManager;
    private FileIOManager ioManager;

    protected static Logger logger = LoggerFactory.getLogger(CloudSessionManager.class);

    protected static ObjectMapper jsonObjectMapper;
    protected static ObjectWriter jsonObjectWriter;

    private Properties accountProperties;
    
    public CloudSessionManager() throws IOException, IOManagementException {
        this(System.getenv("OPENCGA_HOME"));
    }

    public CloudSessionManager(String gcsaHome) throws IOException, IOManagementException {
        accountProperties = Config.getAccountProperties();

        if (accountProperties.getProperty("OPENCGA.ACCOUNT.MODE").equals("file")) {
            accountManager = (AccountManager) new AccountFileManager();
        } else {
            accountManager = new AccountMongoDBManager();
        }
        ioManager = new FileIOManager();
        
        jsonObjectMapper = new ObjectMapper();
        jsonObjectWriter = jsonObjectMapper.writer();
    }

    /**
     * Getter Path methods
     * ***************************
     */

    public Path getAccountPath(String accountId) {
        return ioManager.getAccountPath(accountId);
    }

    public Path getBucketPath(String accountId, String bucketId) {
        return ioManager.getBucketPath(accountId, bucketId);
    }

    public Path getObjectPath(String accountId, String bucketId, Path ObjectId) {
        return ioManager.getObjectPath(accountId, bucketId, ObjectId);
    }

    public Path getJobFolderPath(String accountId, String projectId, Path JobId) {
        return ioManager.getJobFolderPath(accountId, projectId, JobId);
    }

    public Path getTmpPath() {
        return ioManager.getTmpPath();
    }

    /**
     * Account methods
     * ***************************
     */
    public void createAccount(String accountId, String password, String name, String email, String sessionIp)
            throws AccountManagementException, IOManagementException, JsonProcessingException {
        checkParameter(accountId, "accountId");
        checkParameter(password, "password");
        checkParameter(name, "name");
        checkEmail(email);
        checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp);

        ioManager.createAccount(accountId);

        try {
            accountManager.createAccount(accountId, password, name, "user", email, session);
        } catch (AccountManagementException e) {
            ioManager.deleteAccount(accountId);
            throw e;
        }

    }

    public String createAnonymousAccount(String sessionIp) throws AccountManagementException, IOManagementException, IOException {
        checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp);

        String password = StringUtils.randomString(10);
        String accountId = "anonymous_" + password;

        ioManager.createAccount(accountId);
        try {
            return accountManager.createAnonymousAccount(accountId, password, session);
        } catch (AccountManagementException e) {
            ioManager.deleteAccount(accountId);
            throw e;
        }

    }

    public String login(String accountId, String password, String sessionIp) throws AccountManagementException, IOException {
        checkParameter(accountId, "accountId");
        checkParameter(password, "password");
        checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp);
        return accountManager.login(accountId, password, session);
    }

    public void logout(String accountId, String sessionId) throws AccountManagementException, IOException {
        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");
        accountManager.logout(accountId, sessionId);
    }

    public void logoutAnonymous(String sessionId) throws AccountManagementException, IOManagementException {
        String accountId = "anonymous_" + sessionId;
        System.out.println("-----> el accountId del anonimo es: " + accountId + " y la sesionId: " + sessionId);

        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");

        // TODO check inconsistency
        ioManager.deleteAccount(accountId);
        accountManager.logoutAnonymous(accountId, sessionId);
    }

    public void changePassword(String accountId, String password, String nPassword1, String nPassword2, String sessionId)
            throws AccountManagementException {
        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");
        checkParameter(password, "password");
        checkParameter(nPassword1, "nPassword1");
        checkParameter(nPassword2, "nPassword2");
        if (!nPassword1.equals(nPassword2)) {
            throw new AccountManagementException("the new pass is not the same in both fields");
        }
        accountManager.changePassword(accountId, sessionId, password, nPassword1, nPassword2);
    }

    public void changeEmail(String accountId, String nEmail, String sessionId) throws AccountManagementException {
        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");
        checkEmail(nEmail);
        accountManager.changeEmail(accountId, sessionId, nEmail);
    }

    public void resetPassword(String accountId, String email) throws AccountManagementException {
        checkParameter(accountId, "accountId");
        checkEmail(email);
        accountManager.resetPassword(accountId, email);
    }

    public String getAccountInfo(String accountId, String lastActivity, String sessionId)
            throws AccountManagementException {
        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");
        // lastActivity can be null
        return accountManager.getAccountInfo(accountId, sessionId, lastActivity);
    }

    public void deleteAccount(String accountId, String sessionId) throws AccountManagementException,
            IOManagementException {
        // TODO
    }

    /**
     * Bucket methods
     * ***************************
     */
    public String getBucketsList(String accountId, String sessionId) throws AccountManagementException, JsonProcessingException {
        return accountManager.getBucketsList(accountId, sessionId);
    }

    public void createBucket(String accountId, Bucket bucket, String sessionId) throws AccountManagementException,
            IOManagementException, JsonProcessingException {
        checkParameter(bucket.getName(), "bucketName");
        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");

        ioManager.createBucket(accountId, bucket.getName());
        try {
            accountManager.createBucket(accountId, bucket, sessionId);
        } catch (AccountManagementException e) {
            ioManager.deleteBucket(accountId, bucket.getName());
            throw e;
        }
    }

    public void renameBucket(String accountId, String bucketId, String newBucketId, String sessionId) throws AccountManagementException, IOManagementException {
        checkParameter(bucketId, "bucketName");
        checkParameter(newBucketId, "newBucketId");
        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");
        ioManager.renameBucket(accountId, bucketId, newBucketId);
        try {
            accountManager.renameBucket(accountId, bucketId, newBucketId, sessionId);
        } catch (AccountManagementException e) {
            ioManager.renameBucket(accountId, newBucketId, bucketId);
            throw e;
        }
    }


    public String createObjectToBucket(String accountId, String bucketId, Path objectId, ObjectItem objectItem,
                                       InputStream fileIs, boolean parents, String sessionId) throws AccountManagementException,
            IOManagementException, IOException, InterruptedException {
        checkParameter(bucketId, "bucket");
        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");
        checkParameter(objectId.toString(), "objectId");
        checkObj(objectItem, "objectItem");

        objectItem.setStatus("ready");
        objectId = ioManager.createObject(accountId, bucketId, objectId, objectItem, fileIs, parents);

        //Check files to create index if needed
        String sgeJobName;
//        switch (objectItem.getFileFormat()) {
//            case "bam":
//                sgeJobName = BamManager.createIndex(getObjectPath(accountId, bucketId, objectId));
//                objectItem.setStatus(sgeJobName);
//                break;
//            case "vcf":
//                sgeJobName = VcfManager.createIndex(getObjectPath(accountId, bucketId, objectId));
//                objectItem.setStatus(sgeJobName);
//                break;
//            default:
//                break;
//        }


        // set id and name to the itemObject
        objectItem.setId(objectId.toString());
        objectItem.setFileName(objectId.getFileName().toString());

        try {
            accountManager.createObjectToBucket(accountId, bucketId, objectItem, sessionId);
            return objectId.toString();
        } catch (AccountManagementException e) {
            ioManager.deleteObject(accountId, bucketId, objectId);
            throw e;
        }
    }

    public String createFolderToBucket(String accountId, String bucketId, Path objectId, ObjectItem objectItem,
                                       boolean parents, String sessionId) throws AccountManagementException, IOManagementException, JsonProcessingException {
        checkParameter(bucketId, "bucket");
        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");
        checkParameter(objectId.toString(), "objectId");
        checkObj(objectItem, "objectItem");

        ioManager.createFolder(accountId, bucketId, objectId, parents);

        // set id and name to the itemObject
        objectItem.setId(objectId.toString());
        objectItem.setFileName(objectId.getFileName().toString());

        try {
            accountManager.createObjectToBucket(accountId, bucketId, objectItem, sessionId);
            return objectId.toString();
        } catch (AccountManagementException e) {
            ioManager.deleteObject(bucketId, accountId, objectId);
            throw e;
        }
    }

    public void refreshBucket(final String accountId, final String bucketId, final String sessionId)
            throws AccountManagementException, IOException {

        final Path bucketPath = ioManager.getBucketPath(accountId, bucketId);
        final List<ObjectItem> newObjects = new ArrayList<ObjectItem>();

        Files.walkFileTree(bucketPath, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String fileName = file.getFileName().toString();
                if (!Files.isHidden(file) && !fileName.equals("sge_err.log") && !fileName.equals("sge_out.log") && !Files.isDirectory(file)) {
                    Path ojectId = bucketPath.relativize(file);
//                    logger.info(ojectId);
                    ObjectItem objectItem = null;
                    try {//find the current object if already exists
                        objectItem = accountManager.getObjectFromBucket(accountId, bucketId, ojectId, sessionId);
                    } catch (AccountManagementException e) {
                        objectItem = new ObjectItem(ojectId.toString(), ojectId.getFileName().toString(), "r");
                        String fileExt = IOUtils.getExtension(ojectId.toString());
                        if (fileExt != null) {
                            objectItem.setFileFormat(fileExt.substring(1));
                        }
                        objectItem.setStatus("");
                    }
                    newObjects.add(objectItem);
                    return FileVisitResult.CONTINUE;
                } else {
                    return FileVisitResult.CONTINUE;
                }
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                // try to delete the file anyway, even if its attributes
                // could not be read, since delete-only access is
                // theoretically possible
                return FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if (Files.isHidden(dir) || !Files.isReadable(dir) || dir.getFileName().toString().equals("..")
                        || dir.getFileName().toString().equals(".")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                if (!dir.equals(bucketPath)) {//dont add bucketId folder itself
                    Path ojectId = bucketPath.relativize(dir);
//                    logger.info(bucketId);
//                    logger.info(ojectId);
//                    logger.info(dir.toString());

                    ObjectItem objectItem = null;
                    try {//find the current object if already exists
                        objectItem = accountManager.getObjectFromBucket(accountId, bucketId, ojectId, sessionId);
                    } catch (AccountManagementException e) {
                        objectItem = new ObjectItem(ojectId.toString(), ojectId.getFileName().toString(), "dir");
                        objectItem.setFileFormat("dir");
                        objectItem.setStatus("");
                    }
                    newObjects.add(objectItem);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (Files.isHidden(dir) || !Files.isReadable(dir)) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                // here
                return FileVisitResult.CONTINUE;
            }
        });

        accountManager.deleteObjectsFromBucket(accountId, bucketId, sessionId);
        for (ObjectItem objectItem : newObjects) {
            accountManager.createObjectToBucket(accountId, bucketId, objectItem, sessionId);
        }
    }

    public void deleteDataFromBucket(String accountId, String bucketId, Path objectId, String sessionId)
            throws AccountManagementException, IOManagementException {
        checkParameter(bucketId, "bucket");
        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");
        checkParameter(objectId.toString(), "objectId");

        objectId = ioManager.deleteObject(accountId, bucketId, objectId);
        accountManager.deleteObjectFromBucket(accountId, bucketId, objectId, sessionId);

    }

    public void shareObject(String accountId, String bucketId, Path objectId, String toAccountId, boolean read,
                            boolean write, boolean execute, String sessionId) throws AccountManagementException {
        checkParameters(accountId, "accountId", bucketId, "bucketId", objectId.toString(), "objectId", toAccountId,
                "toAccountId", sessionId, "sessionId");

        Acl acl = new Acl(toAccountId, "", read, write, execute);
        accountManager.shareObject(accountId, bucketId, objectId, acl, sessionId);
    }

    public String region(String accountId, String bucketId, Path objectId, String regionStr,
                         Map<String, List<String>> params, String sessionId) throws Exception {
        System.out.println("(>·_·)>");
        checkParameter(bucketId, "bucket");
        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");
        checkParameter(objectId.toString(), "objectId");
        checkParameter(regionStr, "regionStr");

        Path fullFilePath = ioManager.getObjectPath(accountId, bucketId, objectId);
        ObjectItem objectItem = accountManager.getObjectFromBucket(accountId, bucketId, objectId, sessionId);

        logger.debug(fullFilePath.toString());
        logger.debug(regionStr);

        String result = "";
        switch (objectItem.getFileFormat()) {
            case "bam":
                if (!params.containsKey("histogram")) {
                    BamManager bamManager = new BamManager();
    //                result = bamManager.getByRegion(fullFilePath, regionStr, params);
                    result = bamManager.queryRegion(fullFilePath, regionStr, params);
                } else {
                    AlignmentQueryBuilder queryBuilder = new TabixAlignmentQueryBuilder(new SqliteCredentials(fullFilePath), null, null);
                    Region region = Region.parseRegion(regionStr);
                    QueryResult<List<ObjectMap>> queryResult = 
                            queryBuilder.getAlignmentsHistogramByRegion(region.getChromosome(), region.getStart(), region.getEnd(), 
                            params.containsKey("histogramLogarithm") ? Boolean.parseBoolean(params.get("histogram").get(0)) : false, 
                            params.containsKey("histogramMax") ? Integer.parseInt(params.get("histogramMax").get(0)) : 500);
                    result = jsonObjectWriter.writeValueAsString(queryResult);
                    System.out.println("result = " + result);
                }
                break;
            case "vcf":
                VcfManager vcfManager = new VcfManager();
//                result = vcfManager.getByRegion(fullFilePath, regionStr, params);
                result = vcfManager.queryRegion(fullFilePath, regionStr, params);
                break;
        }
        return result;
    }

    public String indexFileObject(String accountId, String bucketId, Path objectId, boolean force, String sessionId) throws Exception {
        ObjectItem objectItem = accountManager.getObjectFromBucket(accountId, bucketId, objectId, sessionId);
        if (objectItem.getStatus().contains("indexer")) {
            return "indexing...";
        }
        String sgeJobName = "ready";
        boolean indexReady;
        switch (objectItem.getFileFormat()) {
            case "bam":
                indexReady = BamManager.checkIndex(ioManager.getObjectPath(accountId, bucketId, objectId));
                if (force || !indexReady) {
                    sgeJobName = BamManager.createIndex(getObjectPath(accountId, bucketId, objectId));
                    accountManager.setObjectStatus(accountId, bucketId, objectId, sgeJobName, sessionId);
                }
                break;
            case "vcf":
                indexReady = VcfManager.checkIndex(ioManager.getObjectPath(accountId, bucketId, objectId));
                if (force || !indexReady) {
                    sgeJobName = VcfManager.createIndex(getObjectPath(accountId, bucketId, objectId));
                    accountManager.setObjectStatus(accountId, bucketId, objectId, sgeJobName, sessionId);
                }
                break;
        }

        return sgeJobName;
    }

    public String indexFileObjectStatus(String accountId, String bucketId, Path objectId, String sessionId, String jobId) throws Exception {
        checkParameter(jobId, "jobId");
        logger.info(jobId);
        String objectStatus = accountManager.getObjectFromBucket(accountId, bucketId, objectId, sessionId).getStatus();
        logger.info(objectStatus);
//        String jobStatus = SgeManager.status(jobId);
        String jobStatus = "finished";
        logger.info(jobStatus);
        if (jobStatus.equalsIgnoreCase("finished")) {
            objectStatus = objectStatus.replace("indexer_", "index_finished_");
            logger.info(objectStatus);
            accountManager.setObjectStatus(accountId, bucketId, objectId, objectStatus, sessionId);
        }
        return jobStatus;
    }

    /**
     * Project methods
     * ***************************
     */
    public String getProjectsList(String accountId, String sessionId) throws AccountManagementException {
        return accountManager.getProjectsList(accountId, sessionId);
    }

    public void createProject(String accountId, Project project, String sessionId) throws AccountManagementException,
            IOManagementException, JsonProcessingException {
        checkParameter(project.getId(), "projectName");
        checkParameter(accountId, "accountId");
        checkParameter(sessionId, "sessionId");

        ioManager.createProject(accountId, project.getId());
        try {
            accountManager.createProject(accountId, project, sessionId);
        } catch (AccountManagementException e) {
            ioManager.deleteProject(accountId, project.getId());
            throw e;
        }
    }

    public String checkJobStatus(String accountId, String jobId, String sessionId) throws AccountManagementException, IOException {
        return accountManager.getJobStatus(accountId, jobId, sessionId);
    }

    public void incJobVisites(String accountId, String jobId, String sessionId) throws AccountManagementException, IOException {
        accountManager.incJobVisites(accountId, jobId, sessionId);
    }

    public void deleteJob(String accountId, String projectId, String jobId, String sessionId)
            throws AccountManagementException, IOManagementException {
        checkParameter(accountId, "accountId");
        checkParameter(projectId, "projectId");
        checkParameter(jobId, "jobId");
        checkParameter(sessionId, "sessionId");

        ioManager.deleteJob(accountId, projectId, jobId);
        accountManager.deleteJobFromProject(accountId, projectId, jobId, sessionId);
    }

    public String getJobResult(String accountId, String jobId, String sessionId) throws IOException, IOManagementException, AccountManagementException {
        checkParameter(accountId, "accountId");
        checkParameter(jobId, "jobId");

        Path jobPath = getAccountPath(accountId).resolve(accountManager.getJobPath(accountId, jobId, sessionId));
//        return ioManager.getJobResult(jobPath);
        return "DEPRECATED";
    }

    public Job getJob(String accountId, String jobId, String sessionId) throws IOException, IOManagementException, AccountManagementException {
        checkParameter(accountId, "accountId");
        checkParameter(jobId, "jobId");

        return accountManager.getJob(accountId, jobId, sessionId);
    }

    public String getFileTableFromJob(String accountId, String jobId, String filename, String start, String limit,
                                      String colNames, String colVisibility, String callback, String sort, String sessionId)
            throws IOManagementException, IOException, AccountManagementException {
        checkParameter(accountId, "accountId");
        checkParameter(jobId, "jobId");
        checkParameter(filename, "filename");

        Path jobPath = getAccountPath(accountId).resolve(accountManager.getJobPath(accountId, jobId, sessionId));

        return ioManager.getFileTableFromJob(jobPath, filename, start, limit, colNames, colVisibility, callback, sort);
    }

    public DataInputStream getFileFromJob(String accountId, String jobId, String filename, String zip, String sessionId)
            throws IOManagementException, IOException, AccountManagementException {
        checkParameter(accountId, "accountId");
        checkParameter(jobId, "jobId");
        checkParameter(filename, "filename");
        checkParameter(zip, "zip");

        Path jobPath = getAccountPath(accountId).resolve(accountManager.getJobPath(accountId, jobId, sessionId));

        return ioManager.getFileFromJob(jobPath, filename, zip);
    }

    public InputStream getJobZipped(String accountId, String jobId, String sessionId) throws IOManagementException,
            IOException, AccountManagementException {
        checkParameter(accountId, "accountId");
        checkParameter(jobId, "jobId");
        checkParameter(sessionId, "sessionId");

        Path jobPath = getAccountPath(accountId).resolve(accountManager.getJobPath(accountId, jobId, sessionId));
        logger.info("getJobZipped");
        logger.info(jobPath.toString());
        logger.info(jobId);
        return ioManager.getJobZipped(jobPath, jobId);
    }

    public String createJob(String jobName, String projectId, String jobFolder, String toolName, List<String> dataList,
                            String commandLine, String sessionId) throws AccountManagementException, IOManagementException, JsonProcessingException {

        checkParameter(jobName, "jobName");
        checkParameter(projectId, "projectId");
        checkParameter(toolName, "toolName");
        checkParameter(sessionId, "sessionId");
        String accountId = accountManager.getAccountIdBySessionId(sessionId);

        String jobId = StringUtils.randomString(15);
        boolean jobFolderCreated = false;

        if (jobFolder == null) {
            ioManager.createJob(accountId, projectId, jobId);
            jobFolder = Paths.get("projects", projectId).resolve(jobId).toString();
            jobFolderCreated = true;
        }
        checkParameter(jobFolder, "jobFolder");

        Job job = new Job(jobId, jobName, jobFolder, toolName, Job.QUEUED, commandLine, "", dataList);

        try {
            accountManager.createJob(accountId, projectId, job, sessionId);
        } catch (AccountManagementException e) {
            if (jobFolderCreated) {
                ioManager.deleteJob(accountId, projectId, jobId);
            }
            throw e;
        }

        return jobId;
    }

    public String getJobFolder(String accountId, String jobId, String sessionId) throws AccountManagementException, IOException {
        String projectId = accountManager.getJobProject(accountId, jobId, sessionId).getId();
        return ioManager.getJobPath(accountId, projectId, null, jobId).toString();
    }

    public List<AnalysisPlugin> getUserAnalysis(String sessionId) throws AccountManagementException, IOException {
        return accountManager.getUserAnalysis(sessionId);
    }

    public void setJobCommandLine(String accountId, String jobId, String commandLine, String sessionId)
            throws AccountManagementException, IOException {
        accountManager.setJobCommandLine(accountId, jobId, commandLine, sessionId);// this
        // method
        // increases
        // visites
        // by 1
        // in
        // mongodb
    }

    /**
     * ****************
     */
    private void checkEmail(String email) throws AccountManagementException {
        String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
                + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
        Pattern pattern = Pattern.compile(EMAIL_PATTERN);
        if (!pattern.matcher(email).matches()) {
            throw new AccountManagementException("email not valid");
        }
    }

    private void checkParameter(String param, String name) throws AccountManagementException {
        if (param == null || param.equals("") || param.equals("null")) {
            throw new AccountManagementException("Error in parameter: parameter '" + name + "' is null or empty: "
                    + param + ".");
        }
    }

    private void checkParameters(String... args) throws AccountManagementException {
        if (args.length % 2 == 0) {
            for (int i = 0; i < args.length; i += 2) {
                checkParameter(args[i], args[i + 1]);
            }
        } else {
            throw new AccountManagementException("Error in parameter: parameter list is not multiple of 2");
        }
    }

    private void checkObj(Object obj, String name) throws AccountManagementException {
        if (obj == null) {
            throw new AccountManagementException("parameter '" + name + "' is null.");
        }
    }

    private void checkRegion(String regionStr, String name) throws AccountManagementException {
        if (Pattern.matches("^([a-zA-Z0-9])+:([0-9])+-([0-9])+$", regionStr)) {//chr:start-end
            throw new AccountManagementException("region '" + name + "' is not valid");
        }
    }


}
