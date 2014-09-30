package org.opencb.opencga.catalog.core.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.opencga.lib.common.IOUtils;
import org.opencb.opencga.lib.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class PosixIOManager implements CatalogIOManager {

    protected static Logger logger = LoggerFactory.getLogger(PosixIOManager.class);
    protected static ObjectMapper jsonObjectMapper;
    protected static ObjectWriter jsonObjectWriter;

//    private Properties accountProperties;

    //    private String appHomePath;
    private String opencgaRootDir;
    //    private Path opencgaRootDirPath;
    private String tmp;

//    private static String BUCKETS_FOLDER = "buckets";
    /**
     * OPENCGA_USER_FOLDER and OPENCGA_BIN_FOLDER are created in the ROOTDIR of OpenCGA.
     * OPENCGA_USER_FOLDER contains users workspaces organized by 'userId'
     * OPENCGA_BIN_FOLDER contains all packaged binaries delivered with OpenCGA
     */
    private static String OPENCGA_USER_FOLDER = "users";
    private static String OPENCGA_ANONYMOUS_USER_FOLDER = "anonymous";
    private static String OPENCGA_BIN_FOLDER = "bin";

    /**
     * USER_PROJECT_FOLDER and USER_BIN_FOLDER are created in the user workspace to store the projects with studies
     * and files, and the user binaries.
     */
    private static String USER_PROJECT_FOLDER = "projects";
    private static String USER_BIN_FOLDER = "bin";
    private static String SHARED_DATA_FOLDER = "shared_data";
//    private static String ANALYSIS_FOLDER = "analysis";


    public PosixIOManager(String rootdir) throws IOException, CatalogIOManagerException {
//        accountProperties = Config.getAccountProperties();
//        appHomePath = Config.getGcsaHome();
//		opencgaRootDir = appHomePath + accountProperties.getProperty("OPENCGA.ACCOUNT.PATH");
//        opencgaRootDir = accountProperties.getProperty("OPENCGA.ACCOUNT.PATH");
//        tmp = accountProperties.getProperty("OPENCGA.TMP.PATH");

        this(rootdir, true);
//        this.opencgaRootDir = rootdir;
//
//        if (!Files.exists(Paths.get(opencgaRootDir))) {
//            throw new IOManagementException("ERROR: The accounts folder not exists");
//        }
    }

    public PosixIOManager(String rootdir, boolean setup) throws IOException, CatalogIOManagerException {
        this.opencgaRootDir = rootdir;
//        this.opencgaRootDirPath = Paths.get(this.opencgaRootDir);

        if(setup) {
            setup();
        }
    }

    public void setup() throws IOException {
        Path opencgaRootDirPath = Paths.get(this.opencgaRootDir);
        if(!Files.exists(opencgaRootDirPath) && Files.isDirectory(opencgaRootDirPath) && Files.isWritable(opencgaRootDirPath)) {
            new IOException("OpenCGA ROOTDIR does not exist or it is not a writable directory");
        }

        if(!Files.exists(opencgaRootDirPath.resolve(OPENCGA_USER_FOLDER))) {
            Files.createDirectory(opencgaRootDirPath.resolve(OPENCGA_USER_FOLDER));
        }

        if(!Files.exists(opencgaRootDirPath.resolve(OPENCGA_ANONYMOUS_USER_FOLDER))) {
            Files.createDirectory(opencgaRootDirPath.resolve(OPENCGA_ANONYMOUS_USER_FOLDER));
        }

        if(!Files.exists(opencgaRootDirPath.resolve(OPENCGA_BIN_FOLDER))) {
            Files.createDirectory(opencgaRootDirPath.resolve(OPENCGA_BIN_FOLDER));
        }

    }

    private void checkParam(String param) throws CatalogIOManagerException {
        if(param == null || param.equals("")) {
            throw new CatalogIOManagerException("Parameter '" + param + "' not valid");
        }
    }

    private void checkPath(Path path) throws CatalogIOManagerException {
        if(path == null || !Files.exists(path)) {
            throw new CatalogIOManagerException("Path '" + path.toString() + "' is null or it does not exist");
        }
    }

    private void checkDirectoryPath(Path path, boolean writable) throws CatalogIOManagerException {
        if(path == null || !Files.exists(path) || !Files.isDirectory(path)) {
            throw new CatalogIOManagerException("Path '" + path.toString() + "' is null, it does not exist or it's not a directory");
        }

        if(writable && !Files.isWritable(path)) {
            throw new CatalogIOManagerException("Path '" + path.toString() + "' is not writable");
        }
    }

    /*****************************
     * Get Path methods
     * ***************************
     */
    public Path getUserPath(String userId) throws CatalogIOManagerException {
        checkParam(userId);

        Path path = Paths.get(opencgaRootDir, OPENCGA_USER_FOLDER, userId);
        checkPath(path);

        return path;
    }

    public Path getAnonmousUserPath(String userId) throws CatalogIOManagerException {
        checkParam(userId);

        Path path = Paths.get(opencgaRootDir, OPENCGA_ANONYMOUS_USER_FOLDER, userId);
        checkPath(path);

        return path;
    }

    public Path getProjectRootPath(String userId) throws CatalogIOManagerException {
        Path path = getUserPath(userId).resolve(PosixIOManager.USER_PROJECT_FOLDER);
        checkPath(path);

        return path;
    }

    public Path getProjectPath(String userId, String projectId) throws CatalogIOManagerException {
        checkParam(projectId);

        Path path = getUserPath(userId).resolve(PosixIOManager.USER_PROJECT_FOLDER).resolve(projectId);
        checkPath(path);

        return path;
    }

    public Path getStudyPath(String userId, String projectId, String studyId) throws CatalogIOManagerException {
        checkParam(projectId);
        checkParam(studyId);

        Path path = getUserPath(userId).resolve(PosixIOManager.USER_PROJECT_FOLDER).resolve(projectId).resolve(studyId);
        checkPath(path);

        return path;
    }
    public Path getFilePath(String userId, String projectId, String studyId, String relativeFilePath, boolean check) throws CatalogIOManagerException {
        checkParam(relativeFilePath);

        Path path = getStudyPath(userId, projectId, studyId).resolve(relativeFilePath);
        if(check) {
            checkPath(path);
        }

        return path;
    }

    public Path getFilePath(String userId, String projectId, String studyId, String relativeFilePath) throws CatalogIOManagerException {
        return getFilePath(userId, projectId, studyId, relativeFilePath, true);
    }

    // TODO Jobs are stored in the workspace right?
//    public Path getJobFolderPath(String accountId, String projectId, Path jobId) {
//        return getProjectPath(accountId, projectId).resolve(jobId);
//    }
//
//    // TODO tener en cuenta las demás implementaciones de la interfaz.
//    public Path getJobPath(String accountId, String projectId, String bucketId, String jobId) {
//        Path jobFolder;
//        // If a bucket is passed then outdir is set
//        if (bucketId != null && !bucketId.equals("")) {
//            jobFolder = Paths.get(opencgaRootDir, accountId, PosixIOManager.BUCKETS_FOLDER, jobId);
//        } else {
//            jobFolder = getProjectPath(accountId, projectId).resolve(jobId);
//        }
//        return jobFolder;
//    }


    public Path getTmpPath() {
        return Paths.get(tmp);
    }

    /**
     * User methods
     * ***************************
     */
    public Path createUser(String userId) throws CatalogIOManagerException {
        checkParam(userId);

        Path usersPath = Paths.get(opencgaRootDir, OPENCGA_USER_FOLDER);
        checkDirectoryPath(usersPath, true);

        Path userPath = usersPath.resolve(userId);
        try {
            if(!Files.exists(userPath)) {
                Files.createDirectory(userPath);
                Files.createDirectory(Paths.get(userPath.toString(), PosixIOManager.USER_PROJECT_FOLDER));
                Files.createDirectory(Paths.get(userPath.toString(), PosixIOManager.USER_BIN_FOLDER));

                return userPath;
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("IOException" + e.toString());
        }
        return null;
    }

    public Path deleteUser(String userId) throws CatalogIOManagerException {
        Path userPath = getUserPath(userId);
        checkPath(userPath);

        try {
            IOUtils.deleteDirectory(userPath);
        } catch (IOException e1) {
            throw new CatalogIOManagerException("IOException: " + e1.toString());
        }

        return userPath;
    }

    public Path createAnonymousUser(String anonymousUserId) throws CatalogIOManagerException {
        checkParam(anonymousUserId);

        Path usersPath = Paths.get(opencgaRootDir, OPENCGA_ANONYMOUS_USER_FOLDER);
        checkDirectoryPath(usersPath, true);

        Path userPath = usersPath.resolve(anonymousUserId);
        try {
            if(!Files.exists(userPath)) {
                Files.createDirectory(userPath);
                Files.createDirectory(Paths.get(userPath.toString(), PosixIOManager.USER_PROJECT_FOLDER));
                Files.createDirectory(Paths.get(userPath.toString(), PosixIOManager.USER_BIN_FOLDER));

                return userPath;
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("IOException" + e.toString());
        }
        return null;
    }

    public Path deleteAnonymousUser(String anonymousUserId) throws CatalogIOManagerException {
        Path anonymousUserPath = getAnonmousUserPath(anonymousUserId);
        checkPath(anonymousUserPath);

        try {
            IOUtils.deleteDirectory(anonymousUserPath);
        } catch (IOException e1) {
            throw new CatalogIOManagerException("IOException: " + e1.toString());
        }

        return anonymousUserPath;
    }

    /*****************************
     * Project methods ***********
     * ***************************
     */
    public Path createProject(String userId, String projectId) throws CatalogIOManagerException {
        checkParam(projectId);

        Path projectRootPath = getProjectRootPath(userId);
        checkDirectoryPath(projectRootPath, true);

        Path projectPath = projectRootPath.resolve(projectId);
        try {
            if(!Files.exists(projectPath)) {
                projectPath = Files.createDirectory(projectPath);
                Files.createDirectory(projectPath.resolve(SHARED_DATA_FOLDER));
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("createProject(): could not create the bucket folder: " + e.toString());
        }

        return projectPath;
    }

    public Path deleteProject(String userId, String projectId) throws CatalogIOManagerException {
        Path projectPath = getProjectPath(userId, projectId);
        checkPath(projectPath);

        try {
            IOUtils.deleteDirectory(projectPath);
        } catch (IOException e) {
            throw new CatalogIOManagerException("deleteProject(): could not delete the project folder: " + e.toString());
        }

        return projectPath;
    }

    public void renameProject(String userId, String oldProjectId, String newProjectId) throws CatalogIOManagerException {
        Path oldFolder =  getProjectPath(userId, oldProjectId);
        Path newFolder =  getProjectPath(userId, newProjectId);
        checkPath(oldFolder);
        checkDirectoryPath(oldFolder.getParent(), true);

        try {
            if(!Files.exists(newFolder)) {
                Files.move(oldFolder, newFolder);
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("renameProject(): could not rename the project folder: " + e.toString());
        }
    }

    public boolean existProject(String userId, String projectId) throws CatalogIOManagerException {
        return Files.exists(getProjectPath(userId, projectId));
    }

    /**
     * Project Study
     * ***************************
     */
    public Path createStudy(String userId, String projectId, String studyId) throws CatalogIOManagerException {
        checkParam(studyId);

        Path projectPath = getProjectPath(userId, projectId);
        checkDirectoryPath(projectPath, true);

        Path studyPath = projectPath.resolve(studyId);
        try {
            if(!Files.exists(studyPath)) {
                studyPath = Files.createDirectory(studyPath);
//                Files.createDirectory(studyPath.resolve("data"));
//                Files.createDirectory(studyPath.resolve("analysis"));
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("createStudy method: could not create the study folder: " + e.toString());
        }

        return studyPath;
    }

    public Path deleteStudy(String userId, String projectId, String studyId) throws CatalogIOManagerException {
        Path studyPath = getStudyPath(userId, projectId, studyId);
        checkPath(studyPath);

        try {
            IOUtils.deleteDirectory(studyPath);
        } catch (IOException e) {
            throw new CatalogIOManagerException("deleteProject(): could not delete the project folder: " + e.toString());
        }

        return studyPath;
    }

    public void renameStudy(String userId, String projectId, String oldStudyId, String newStudyId) throws CatalogIOManagerException {
        Path oldFolder =  getStudyPath(userId, projectId, oldStudyId);
        Path newFolder =  getStudyPath(userId, projectId, newStudyId);
        checkPath(oldFolder);
        checkDirectoryPath(oldFolder.getParent(), true);

        try {
            if(!Files.exists(newFolder)) {
                Files.move(oldFolder, newFolder);
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("renameProject(): could not rename the project folder: " + e.toString());
        }
    }


    /**
     * Folder and file methods
     * ***************************
     */
    public Path createFolder(String userid, String projectId, String studyId, String fileId, boolean parent)
            throws CatalogIOManagerException {
        checkParam(fileId);

        Path studyPath = getStudyPath(userid, projectId, studyId);
        checkDirectoryPath(studyPath, true);

//        Path fullFolderPath = getFilePath(userid, projectId, studyId, objectId);
        Path filePath = studyPath.resolve(fileId);
        try {
            if(!Files.exists(filePath)) {
                if(parent) {
                    Files.createDirectories(filePath);
                }else {
                    checkDirectoryPath(filePath.getParent(), true);
                    Files.createDirectory(filePath);
                }
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("createFolder(): could not create the directory " + e.toString());
        }

        return filePath;
    }

    public void createFile(String userId, String projectId, String studyId, String objectId, InputStream inputStream) throws CatalogIOManagerException {
        Path filePath = getFilePath(userId, projectId, studyId, objectId, false);

        try {
            Files.copy(inputStream, filePath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new CatalogIOManagerException("create file failed at copying file " + filePath);
        }
    }
    public void deleteFile(String userId, String projectId, String studyId, String objectId) throws CatalogIOManagerException {
        Path filePath = getFilePath(userId, projectId, studyId, objectId);
        checkPath(filePath);

        logger.debug("Deleting {}", filePath.toString());
        try {
            if(Files.isDirectory(filePath)) {
                IOUtils.deleteDirectory(filePath);
            }else {
                Files.delete(filePath);
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("deleteFile(): could not delete the object " + e.toString());
        }
    }

    /**
     * Job methods
     * ***************************
     */
//    public URI createJob(String accountId, String projectId, String jobId) throws CatalogIOManagerException {
//        // String path = getUserPath(accountId) + "/jobs";
//        Path jobFolder = getJobPath(accountId, projectId, null, jobId);
//        logger.debug("PAKO " + jobFolder);
//
//        if (Files.exists(jobFolder.getParent()) && Files.isDirectory(jobFolder.getParent())
//                && Files.isWritable(jobFolder.getParent())) {
//            try {
//                Files.createDirectory(jobFolder);
//            } catch (IOException e) {
//                throw new CatalogIOManagerException("createJob(): could not create the job folder: " + e.toString());
//            }
//        } else {
//            throw new CatalogIOManagerException("createJob(): 'jobs' folder not writable");
//        }
//        return jobFolder.toUri();
//    }
//
//    public void deleteJob(String accountId, String projectId, String jobId) throws CatalogIOManagerException {
//        Path jobFolder = getJobPath(accountId, projectId, null, jobId);
//        try {
//            IOUtils.deleteDirectory(jobFolder);
//        } catch (IOException e) {
//            throw new CatalogIOManagerException("deleteJob(): could not delete the job folder: " + e.toString());
//        }
//    }
//
//    public void deleteJobObjects(String accountId, String projectId, String bucketId, String jobId, List<String> objects)
//            throws CatalogIOManagerException {
//        Path jobFolder = getJobPath(accountId, projectId, bucketId, jobId);
//
//        try {
//            if (objects != null && objects.size() > 0) {
//                for (String object : objects) {
//                    Files.delete(Paths.get(jobFolder.toString(), object));
//                }
//            }
//        } catch (IOException e) {
//            throw new CatalogIOManagerException("deleteJobObjects(): could not delete the job objects: " + e.toString());
//        }
//    }
//
//    public void moveJob(String accountId, String projectId, String oldBucketId, String oldJobId, String newBucketId,
//                        String newJobId) throws CatalogIOManagerException {
//        Path oldBucketFolder = getJobPath(accountId, projectId, oldBucketId, oldJobId);
//        Path newBucketFolder = getJobPath(accountId, projectId, newBucketId, newJobId);
//
//        try {
//            Files.move(oldBucketFolder, newBucketFolder);
//        } catch (IOException e) {
//            throw new CatalogIOManagerException("deleteBucket(): could not rename the bucket folder: " + e.toString());
//        }
//    }


    /**
     * *****************
     * <p/>
     * OBJECT METHODS
     * <p/>
     * ******************
     */

//    public Path createObject(String userId, String projectId, String studyId, Path objectId, File file,
//                             InputStream fileIs, boolean parents) throws CatalogIOManagerException, IOException {
//
//        Path auxFullFilePath = getFilePath(userId, projectId, studyId, objectId);
//        Path fullFilePath = getFilePath(userId, projectId, objectId);
//
//        // if parents is
//        // true, folders
//        // will be
//        // autocreated
//        if (!parents && !Files.exists(fullFilePath.getParent())) {
//            throw new CatalogIOManagerException("createObject(): folder '" + fullFilePath.getParent().getFileName()
//                    + "' not exists");
//        }
//
//
//        // check if file exists and update fullFilePath and objectId
//        fullFilePath = renameExistingFileIfNeeded(fullFilePath);
//        objectId = getBucketPath(userId, projectId).relativize(fullFilePath);
//
//        // creating a random tmp folder
//        String rndStr = StringUtils.randomString(20);
//        Path randomFolder = Paths.get(tmp, rndStr);
//        Path tmpFile = randomFolder.resolve(fullFilePath.getFileName());
//
//        try {
//            Files.createDirectory(randomFolder);
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw new CatalogIOManagerException("createObject(): Could not create the upload temp directory");
//        }
//        try {
//            Files.copy(fileIs, tmpFile);
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new CatalogIOManagerException("createObject(): Could not write the file on disk");
//        }
//        try {
//            Files.copy(tmpFile, fullFilePath);
//            file.setDiskUsage(Files.size(fullFilePath));
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new CatalogIOManagerException("createObject(): Copying from tmp folder to bucket folder");
//        }
//        IOUtils.deleteDirectory(randomFolder);
//
//        return objectId;
//    }

//
//    public String getJobResult(Path jobPath) throws IOManagementException, IOException {
//        jsonObjectMapper = new ObjectMapper();
//        jsonObjectWriter = jsonObjectMapper.writer();
//
//        Path resultFile = jobPath.resolve("result.xml");
//
//        if (Files.exists(resultFile)) {
//            Result resultXml = new Result();
//            resultXml.loadXmlFile(resultFile.toAbsolutePath().toString());
////            Gson g = new Gson();
////            String resultJson = g.toJson(resultXml);
//            String resultJson = jsonObjectWriter.writeValueAsString(resultXml);
//            return resultJson;
//        } else {
//            throw new IOManagementException("getJobResultFromBucket(): the file '" + resultFile + "' not exists");
//        }
//    }

    public DataInputStream getFileObject(String userid, String projectId, String studyId, String objectId,
                                         int start, int limit)
            throws CatalogIOManagerException, IOException {

        Path objectPath = getFilePath(userid, projectId, studyId, objectId);
        if (Files.isRegularFile(objectPath)) {
            DataInputStream is;
            if (start == -1 && limit == -1) {
                is = new DataInputStream(Files.newInputStream(objectPath));
                return is;
            } else {
                is = new DataInputStream(IOUtils.headOffset(objectPath, start, limit));
            }
            return is;
        } else {
            throw new CatalogIOManagerException("Not a regular file: " + objectPath.toAbsolutePath().toString());
        }

    }

    public DataInputStream getGrepFileObject(String userId, String projectId, String studyId, String objectId,
                                             String pattern, boolean ignoreCase, boolean multi) throws CatalogIOManagerException, IOException {
        Path objectPath = getFilePath(userId, projectId, studyId, objectId);
        if (Files.isRegularFile(objectPath)) {
            return new DataInputStream(IOUtils.grepFile(objectPath, pattern, ignoreCase, multi));
        } else {
            throw new CatalogIOManagerException("Not a regular file: " + objectPath.toAbsolutePath().toString());
        }
    }


//    public String getFileTableFromJob(Path jobPath, String filename, String start, String limit, String colNames,
//                                      String colVisibility, String sort) throws CatalogIOManagerException, IOException {
//        Path jobFile = jobPath.resolve(filename);
//        return JobFileIOUtils.getSenchaTable(jobFile, filename, start, limit, colNames, colVisibility, sort);
//    }

    public DataInputStream getFileFromJob(Path jobPath, String filename, String zip) throws CatalogIOManagerException,
            FileNotFoundException {

        // String fileStr = getJobPath(accountId, bucketId, jobId).toString();
        Path filePath = jobPath.resolve(filename);
        File file = filePath.toFile();
        String name = filename.replace("..", "").replace("/", "");
        List<String> avoidingFiles = getAvoidingFiles();
        if (avoidingFiles.contains(name)) {
            throw new CatalogIOManagerException("No permission to use that file: " + file.getAbsolutePath());
        }

        if (!Files.exists(filePath)) {
            throw new CatalogIOManagerException("File not found: " + file.getAbsolutePath());
        }

        if (zip.compareTo("true") != 0) {// PAKO zip != true
            DataInputStream is = new DataInputStream(new FileInputStream(file));
            return is;
        } else {// PAKO zip=true, create the zip file
            String randomFolder = StringUtils.randomString(20);
            try {
                // FileUtils.createDirectory(tmp + "/" + randomFolder);
                Files.createDirectory(Paths.get(tmp, randomFolder));
            } catch (IOException e) {
                throw new CatalogIOManagerException("Could not create the random folder '" + randomFolder + "'");
            }
            File zipfile = new File(tmp + "/" + randomFolder + "/" + filename + ".zip");
            try {
                IOUtils.zipFile(file, zipfile);
            } catch (IOException e) {
                throw new CatalogIOManagerException("Could not zip the file '" + file.getName() + "'");
            }// PAKO comprimir
            logger.debug("checking file: " + zipfile.getName());

            if (!Files.exists(zipfile.toPath())) {
                throw new CatalogIOManagerException("Could not find zipped file '" + zipfile.getName() + "'");
            }

            logger.debug("file " + zipfile.getName() + " exists");
            DataInputStream is = new DataInputStream(new FileInputStream(zipfile));
            return is;
        }
    }

    public DataInputStream getGrepFileFromJob(Path jobPath, String filename, String pattern, boolean ignoreCase, boolean multi) throws CatalogIOManagerException,
            IOException {

        Path filePath = jobPath.resolve(filename);
        if (Files.isRegularFile(filePath)) {
            return new DataInputStream(IOUtils.grepFile(filePath, pattern, ignoreCase, multi));
        } else {
            throw new CatalogIOManagerException("Not a regular file: " + filePath.toAbsolutePath().toString());
        }
    }


    public InputStream getJobZipped(Path jobPath, String jobId) throws CatalogIOManagerException, IOException {
        String zipName = jobId + ".zip";
        Path zipPath = jobPath.resolve(zipName);
        File jobFolder = jobPath.toFile();
        File jobZip = zipPath.toFile();

        List<String> avoidingFiles = getAvoidingFiles();
        avoidingFiles.add(zipName);
        try {
            IOUtils.zipDirectory(jobFolder, jobZip, (ArrayList<String>) avoidingFiles);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Files.newInputStream(zipPath);
    }

    /**
     * **********
     */

    // public String getDataPath(String wsDataId){
    // wsDataId.replaceAll(":", "/")
    // }

    // public String getJobPath(String accountId, String bucketId, String jobId)
    // {
    // return getUserPath(accountId) + "/jobs/" + jobId;
    // }
    private Path renameExistingFileIfNeeded(Path fullFilePath) {
        if (Files.exists(fullFilePath)) {
            String file = fullFilePath.getFileName().toString();
            Path parent = fullFilePath.getParent();
            String fileName = IOUtils.removeExtension(file);
            String fileExt = IOUtils.getExtension(file);
            String newname = null;
            if (fileName != null && fileExt != null) {
                newname = fileName + "-copy" + fileExt;
            } else {
                newname = file + "-copy";
            }
            return renameExistingFileIfNeeded(parent.resolve(newname));
        } else {
            return fullFilePath;
        }
    }

    /****/
    private List<String> getAvoidingFiles() {
        List<String> avoidingFiles = new ArrayList<String>();
        avoidingFiles.add("cli.txt");
        avoidingFiles.add("form.txt");
        avoidingFiles.add("input_params.txt");
        avoidingFiles.add("job.log");
        avoidingFiles.add("jobzip.zip");
        return avoidingFiles;
    }

    /**
     * Bucket methods
     * ***************************
     */
//    public URI createBucket(String accountId, String bucketId) throws CatalogIOManagerException {
//        // String path = getBucketPath(accountId, bucketId);
//        Path folder = Paths.get(opencgaRootDir, accountId, PosixIOManager.BUCKETS_FOLDER, bucketId);
//        if (Files.exists(folder.getParent()) && Files.isDirectory(folder.getParent())
//                && Files.isWritable(folder.getParent())) {
//            try {
//                folder = Files.createDirectory(folder);
//            } catch (IOException e) {
//                // FileUtils.deleteDirectory(new File(path));
//                throw new CatalogIOManagerException("createBucket(): could not create the bucket folder: " + e.toString());
//            }
//        }
//        return folder.toUri();
//    }
//
//    public void deleteBucket(String accountId, String bucketId) throws CatalogIOManagerException {
//        // String path = getBucketPath(accountId, bucketId);
//        Path folder = Paths.get(opencgaRootDir, accountId, PosixIOManager.BUCKETS_FOLDER, bucketId);
//        try {
//            IOUtils.deleteDirectory(folder);
//        } catch (IOException e) {
//            throw new CatalogIOManagerException("deleteBucket(): could not delete the bucket folder: " + e.toString());
//        }
//    }
//
//    public void renameBucket(String accountId, String oldBucketId, String newBucketId) throws CatalogIOManagerException {
//        Path oldFolder = Paths.get(opencgaRootDir, accountId, PosixIOManager.BUCKETS_FOLDER, oldBucketId);
//        Path newFolder = Paths.get(opencgaRootDir, accountId, PosixIOManager.BUCKETS_FOLDER, newBucketId);
//        try {
//            Files.move(oldFolder, newFolder);
//        } catch (IOException e) {
//            throw new CatalogIOManagerException("renameBucket(): could not rename the bucket folder: " + e.toString());
//        }
//    }
//
//    public boolean existBucket(String accountId, String bucketId) throws CatalogIOManagerException {
//        return Files.exists(getBucketPath(accountId, bucketId));
//    }

}
