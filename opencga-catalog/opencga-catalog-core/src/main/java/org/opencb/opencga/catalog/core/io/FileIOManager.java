package org.opencb.opencga.catalog.core.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.opencga.account.beans.ObjectItem;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.IOUtils;
import org.opencb.opencga.lib.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//import org.opencb.opencga.account.io.result.Result;

public class FileIOManager implements CatalogIOManager {

    protected static Logger logger = LoggerFactory.getLogger(FileIOManager.class);
    protected static ObjectMapper jsonObjectMapper;
    protected static ObjectWriter jsonObjectWriter;

    private Properties accountProperties;

    private String appHomePath;
    private String accountHomePath;
    private String tmp;

    private static String BUCKETS_FOLDER = "buckets";
    private static String PROJECTS_FOLDER = "projects";
    private static String ANALYSIS_FOLDER = "analysis";
    private static String JOBS_FOLDER = "jobs";

    public FileIOManager() throws IOException, IOManagementException {
        accountProperties = Config.getAccountProperties();
        appHomePath = Config.getGcsaHome();
//		accountHomePath = appHomePath + accountProperties.getProperty("OPENCGA.ACCOUNT.PATH");
        accountHomePath = accountProperties.getProperty("OPENCGA.ACCOUNT.PATH");
        tmp = accountProperties.getProperty("OPENCGA.TMP.PATH");

        if (!Files.exists(Paths.get(accountHomePath))) {
            throw new IOManagementException("ERROR: The accounts folder not exists");
        }
    }

    /**
     * Getter Path methods
     * ***************************
     */
    public Path getAccountPath(String accountId) {
        return Paths.get(accountHomePath, accountId);
    }

    public Path getBucketsPath(String accountId) {
        return getAccountPath(accountId).resolve(Paths.get(FileIOManager.BUCKETS_FOLDER));
    }

    public Path getBucketPath(String accountId, String bucketId) {
        if (bucketId != null) {
            return getAccountPath(accountId).resolve(Paths.get(FileIOManager.BUCKETS_FOLDER, bucketId.toLowerCase()));
        }
        return getAccountPath(accountId).resolve(FileIOManager.BUCKETS_FOLDER);
    }

    public Path getProjectPath(String accountId, String projectId) {
        if (projectId != null) {
            return getAccountPath(accountId).resolve(Paths.get(FileIOManager.PROJECTS_FOLDER, projectId.toLowerCase()));
        }
        return getAccountPath(accountId).resolve(FileIOManager.PROJECTS_FOLDER);
    }

    public Path getObjectPath(String accountId, String bucketId, Path objectId) {
        return getBucketPath(accountId, bucketId).resolve(objectId);
    }

    public Path getJobFolderPath(String accountId, String projectId, Path jobId) {
        return getProjectPath(accountId, projectId).resolve(jobId);
    }

    // TODO tener en cuenta las demás implementaciones de la interfaz.
    public Path getJobPath(String accountId, String projectId, String bucketId, String jobId) {
        Path jobFolder;
        // If a bucket is passed then outdir is set
        if (bucketId != null && !bucketId.equals("")) {
            jobFolder = Paths.get(accountHomePath, accountId, FileIOManager.BUCKETS_FOLDER, jobId);
        } else {
            jobFolder = getProjectPath(accountId, projectId).resolve(jobId);
        }
        return jobFolder;
    }


    public Path getTmpPath() {
        return Paths.get(tmp);
    }

    /**
     * Account methods ···
     * ***************************
     */
    public void createAccount(String accountId) throws IOManagementException {
//        System.out.println("TEST ERROR "+ accountId);
        // get Java 7 Path object, concatenate home and account
        Path accountPath = Paths.get(accountHomePath, accountId);

        // If account folder not exist is created
        if (!Files.exists(accountPath)) {
            try {
                Path p = Files.createDirectory(accountPath);
//                System.out.println("TEST ERROR: " + p.toString());
            } catch (IOException e) {
//                System.out.println("TEST ERROR IOException:  "+ e.toString());
                throw new IOManagementException("IOException" + e.toString());
            }
        }

        logger.info(accountId + " createAccount(): Creating account folder: " + accountHomePath);
//        System.out.println(accountId + " createAccount(): Creating account folder: " + accountHomePath);
        if (Files.exists(accountPath) && Files.isDirectory(accountPath) && Files.isWritable(accountPath)) {

            try {
                Files.createDirectory(Paths.get(accountHomePath, accountId, FileIOManager.BUCKETS_FOLDER));
                createBucket(accountId, "default");
                Files.createDirectory(Paths.get(accountHomePath, accountId, FileIOManager.PROJECTS_FOLDER));
                createProject(accountId, "default");
                Files.createDirectory(Paths.get(accountHomePath, accountId, FileIOManager.ANALYSIS_FOLDER));
            } catch (IOException e) {
                throw new IOManagementException("IOException: " + e.toString());
            }

        } else {
//            System.out.println("TEST ERROR The account folder has not been created");
            throw new IOManagementException("ERROR: The account folder has not been created ");
        }

    }

    public void deleteAccount(String accountId) throws IOManagementException {
        Path accountPath = Paths.get(accountHomePath, accountId);
        try {
            if (Files.exists(accountPath)) {
                System.out.println("******************************************");
                System.out.println("******************************************");
                System.out.println("deleteAccount DELETING DIRECTORY");
                System.out.println("******************************************");
                System.out.println("******************************************");
                IOUtils.deleteDirectory(accountPath);
            }
        } catch (IOException e1) {
            throw new IOManagementException("IOException: " + e1.toString());
        }
    }

    /**
     * Bucket methods ···
     * ***************************
     */
    public URI createBucket(String accountId, String bucketId) throws IOManagementException {
        // String path = getBucketPath(accountId, bucketId);
        Path folder = Paths.get(accountHomePath, accountId, FileIOManager.BUCKETS_FOLDER, bucketId);
        if (Files.exists(folder.getParent()) && Files.isDirectory(folder.getParent())
                && Files.isWritable(folder.getParent())) {
            try {
                folder = Files.createDirectory(folder);
            } catch (IOException e) {
                // FileUtils.deleteDirectory(new File(path));
                throw new IOManagementException("createBucket(): could not create the bucket folder: " + e.toString());
            }
        }
        return folder.toUri();
    }

    public void deleteBucket(String accountId, String bucketId) throws IOManagementException {
        // String path = getBucketPath(accountId, bucketId);
        Path folder = Paths.get(accountHomePath, accountId, FileIOManager.BUCKETS_FOLDER, bucketId);
        try {
            IOUtils.deleteDirectory(folder);
        } catch (IOException e) {
            throw new IOManagementException("deleteBucket(): could not delete the bucket folder: " + e.toString());
        }
    }

    public void renameBucket(String accountId, String oldBucketId, String newBucketId) throws IOManagementException {
        Path oldFolder = Paths.get(accountHomePath, accountId, FileIOManager.BUCKETS_FOLDER, oldBucketId);
        Path newFolder = Paths.get(accountHomePath, accountId, FileIOManager.BUCKETS_FOLDER, newBucketId);
        try {
            Files.move(oldFolder, newFolder);
        } catch (IOException e) {
            throw new IOManagementException("renameBucket(): could not rename the bucket folder: " + e.toString());
        }
    }

    public boolean existBucket(String accountId, String bucketId) throws IOManagementException {
        return Files.exists(getBucketPath(accountId, bucketId));
    }

    /**
     * Project methods ···
     * ***************************
     */
    public URI createProject(String accountId, String projectId) throws IOManagementException {
        Path folder = Paths.get(accountHomePath, accountId, FileIOManager.PROJECTS_FOLDER, projectId);
        if (Files.exists(folder.getParent()) && Files.isDirectory(folder.getParent())
                && Files.isWritable(folder.getParent())) {
            try {
                folder = Files.createDirectory(folder);
            } catch (IOException e) {
                throw new IOManagementException("createProject(): could not create the bucket folder: " + e.toString());
            }
        }
        return folder.toUri();
    }

    public void deleteProject(String accountId, String projectId) throws IOManagementException {
        Path folder = Paths.get(accountHomePath, accountId, FileIOManager.PROJECTS_FOLDER, projectId);
        try {
            IOUtils.deleteDirectory(folder);
        } catch (IOException e) {
            throw new IOManagementException("deleteProject(): could not delete the project folder: " + e.toString());
        }
    }

    public void renameProject(String accountId, String oldProjectId, String newProjectId) throws IOManagementException {
        Path oldFolder = Paths.get(accountHomePath, accountId, FileIOManager.PROJECTS_FOLDER, oldProjectId);
        Path newFolder = Paths.get(accountHomePath, accountId, FileIOManager.PROJECTS_FOLDER, newProjectId);
        try {
            Files.move(oldFolder, newFolder);
        } catch (IOException e) {
            throw new IOManagementException("renameProject(): could not rename the project folder: " + e.toString());
        }
    }

    public boolean existProject(String accountId, String projectId) throws IOManagementException {
        return Files.exists(getProjectPath(accountId, projectId));
    }

    /**
     * Job methods ···
     * ***************************
     */
    public URI createJob(String accountId, String projectId, String jobId) throws IOManagementException {
        // String path = getAccountPath(accountId) + "/jobs";
        Path jobFolder = getJobPath(accountId, projectId, null, jobId);
        logger.debug("PAKO " + jobFolder);

        if (Files.exists(jobFolder.getParent()) && Files.isDirectory(jobFolder.getParent())
                && Files.isWritable(jobFolder.getParent())) {
            try {
                Files.createDirectory(jobFolder);
            } catch (IOException e) {
                throw new IOManagementException("createJob(): could not create the job folder: " + e.toString());
            }
        } else {
            throw new IOManagementException("createJob(): 'jobs' folder not writable");
        }
        return jobFolder.toUri();
    }

    public void deleteJob(String accountId, String projectId, String jobId) throws IOManagementException {
        Path jobFolder = getJobPath(accountId, projectId, null, jobId);
        try {
            IOUtils.deleteDirectory(jobFolder);
        } catch (IOException e) {
            throw new IOManagementException("deleteJob(): could not delete the job folder: " + e.toString());
        }
    }

    public void deleteJobObjects(String accountId, String projectId, String bucketId, String jobId, List<String> objects)
            throws IOManagementException {
        Path jobFolder = getJobPath(accountId, projectId, bucketId, jobId);

        try {
            if (objects != null && objects.size() > 0) {
                for (String object : objects) {
                    Files.delete(Paths.get(jobFolder.toString(), object));
                }
            }
        } catch (IOException e) {
            throw new IOManagementException("deleteJobObjects(): could not delete the job objects: " + e.toString());
        }
    }

    public void moveJob(String accountId, String projectId, String oldBucketId, String oldJobId, String newBucketId,
                        String newJobId) throws IOManagementException {
        Path oldBucketFolder = getJobPath(accountId, projectId, oldBucketId, oldJobId);
        Path newBucketFolder = getJobPath(accountId, projectId, newBucketId, newJobId);

        try {
            Files.move(oldBucketFolder, newBucketFolder);
        } catch (IOException e) {
            throw new IOManagementException("deleteBucket(): could not rename the bucket folder: " + e.toString());
        }
    }


    /**
     * *****************
     * <p/>
     * OBJECT METHODS
     * <p/>
     * ******************
     */
    public Path createFolder(String accountId, String bucketId, Path objectId, boolean parents)
            throws IOManagementException {

        Path fullFolderPath = getObjectPath(accountId, bucketId, objectId);

        try {
            if (existBucket(accountId, bucketId)) {
                if (Files.exists(fullFolderPath.getParent()) && Files.isDirectory(fullFolderPath.getParent())
                        && Files.isWritable(fullFolderPath.getParent())) {
                    Files.createDirectory(fullFolderPath);
                } else {
                    if (parents) {
                        Files.createDirectories(fullFolderPath);
                    } else {
                        throw new IOManagementException("createFolder(): path do no exist");
                    }
                }
            } else {
                throw new IOManagementException("createFolder(): bucket '" + bucketId + "' do no exist");
            }
        } catch (IOException e) {
            throw new IOManagementException("createFolder(): could not create the directory " + e.toString());
        }

        return objectId;
    }

    public Path createObject(String accountId, String bucketId, Path objectId, ObjectItem objectItem,
                             InputStream fileIs, boolean parents) throws IOManagementException, IOException {

        Path auxFullFilePath = getObjectPath(accountId, bucketId, objectId);
        Path fullFilePath = getObjectPath(accountId, bucketId, objectId);

        // if parents is
        // true, folders
        // will be
        // autocreated
        if (!parents && !Files.exists(fullFilePath.getParent())) {
            throw new IOManagementException("createObject(): folder '" + fullFilePath.getParent().getFileName()
                    + "' not exists");
        }


        // check if file exists and update fullFilePath and objectId
        fullFilePath = renameExistingFileIfNeeded(fullFilePath);
        objectId = getBucketPath(accountId, bucketId).relativize(fullFilePath);

        // creating a random tmp folder
        String rndStr = StringUtils.randomString(20);
        Path randomFolder = Paths.get(tmp, rndStr);
        Path tmpFile = randomFolder.resolve(fullFilePath.getFileName());

        try {
            Files.createDirectory(randomFolder);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOManagementException("createObject(): Could not create the upload temp directory");
        }
        try {
            Files.copy(fileIs, tmpFile);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOManagementException("createObject(): Could not write the file on disk");
        }
        try {
            Files.copy(tmpFile, fullFilePath);
            objectItem.setDiskUsage(Files.size(fullFilePath));
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOManagementException("createObject(): Copying from tmp folder to bucket folder");
        }
        IOUtils.deleteDirectory(randomFolder);

        return objectId;
    }

    public Path deleteObject(String accountId, String bucketId, Path objectId) throws IOManagementException {
        Path fullFilePath = getObjectPath(accountId, bucketId, objectId);
        logger.info(fullFilePath.toString());
        try {
            if (Files.deleteIfExists(fullFilePath)) {
                return objectId;
            } else {
                throw new IOManagementException("could not delete the object");
            }
        } catch (IOException e) {
            throw new IOManagementException("deleteObject(): could not delete the object " + e.toString());
        }
    }
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

    public DataInputStream getFileObject(String accountId, String bucketId, Path objectId, String startString, String limitString) throws IOManagementException,
            IOException {

        int limit;
        try {
            limit = Integer.parseInt(limitString);
        } catch (NumberFormatException e) {
            limit = -1;
        }
        int start;
        try {
            start = Integer.parseInt(startString);
        } catch (NumberFormatException e) {
            start = -1;
        }

        Path objectPath = getObjectPath(accountId, bucketId, objectId);
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
            throw new IOManagementException("Not a regular file: " + objectPath.toAbsolutePath().toString());
        }

    }

    public DataInputStream getGrepFileObject(String accountId, String bucketId, Path objectId, String pattern, boolean ignoreCase, boolean multi) throws IOManagementException,
            IOException {
        Path objectPath = getObjectPath(accountId, bucketId, objectId);
        if (Files.isRegularFile(objectPath)) {
            return new DataInputStream(IOUtils.grepFile(objectPath, pattern, ignoreCase, multi));
        } else {
            throw new IOManagementException("Not a regular file: " + objectPath.toAbsolutePath().toString());
        }
    }


    public String getFileTableFromJob(Path jobPath, String filename, String start, String limit, String colNames,
                                      String colVisibility, String sort) throws IOManagementException, IOException {
        Path jobFile = jobPath.resolve(filename);
        return JobFileIOUtils.getSenchaTable(jobFile, filename, start, limit, colNames, colVisibility, sort);
    }

    public DataInputStream getFileFromJob(Path jobPath, String filename, String zip) throws IOManagementException,
            FileNotFoundException {

        // String fileStr = getJobPath(accountId, bucketId, jobId).toString();
        Path filePath = jobPath.resolve(filename);
        File file = filePath.toFile();
        String name = filename.replace("..", "").replace("/", "");
        List<String> avoidingFiles = getAvoidingFiles();
        if (avoidingFiles.contains(name)) {
            throw new IOManagementException("No permission to use that file: " + file.getAbsolutePath());
        }

        if (!Files.exists(filePath)) {
            throw new IOManagementException("File not found: " + file.getAbsolutePath());
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
                throw new IOManagementException("Could not create the random folder '" + randomFolder + "'");
            }
            File zipfile = new File(tmp + "/" + randomFolder + "/" + filename + ".zip");
            try {
                IOUtils.zipFile(file, zipfile);
            } catch (IOException e) {
                throw new IOManagementException("Could not zip the file '" + file.getName() + "'");
            }// PAKO comprimir
            logger.debug("checking file: " + zipfile.getName());

            if (!Files.exists(zipfile.toPath())) {
                throw new IOManagementException("Could not find zipped file '" + zipfile.getName() + "'");
            }

            logger.debug("file " + zipfile.getName() + " exists");
            DataInputStream is = new DataInputStream(new FileInputStream(zipfile));
            return is;
        }
    }

    public DataInputStream getGrepFileFromJob(Path jobPath, String filename, String pattern, boolean ignoreCase, boolean multi) throws IOManagementException,
            IOException {

        Path filePath = jobPath.resolve(filename);
        if (Files.isRegularFile(filePath)) {
            return new DataInputStream(IOUtils.grepFile(filePath, pattern, ignoreCase, multi));
        } else {
            throw new IOManagementException("Not a regular file: " + filePath.toAbsolutePath().toString());
        }
    }


    public InputStream getJobZipped(Path jobPath, String jobId) throws IOManagementException, IOException {
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
    // return getAccountPath(accountId) + "/jobs/" + jobId;
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

}
