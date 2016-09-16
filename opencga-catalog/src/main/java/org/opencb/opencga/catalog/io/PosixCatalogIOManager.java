/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Stream;

public class PosixCatalogIOManager extends CatalogIOManager {

    protected static Logger logger = LoggerFactory.getLogger(PosixCatalogIOManager.class);
    protected static ObjectMapper jsonObjectMapper;
    protected static ObjectWriter jsonObjectWriter;


    public PosixCatalogIOManager(String propertiesFile) throws CatalogIOException {
        super(propertiesFile);
    }

    public PosixCatalogIOManager(Properties properties) throws CatalogIOException {
        super(properties);
    }

    public PosixCatalogIOManager(CatalogConfiguration catalogConfiguration) throws CatalogIOException {
        super(catalogConfiguration);
    }

    @Override
    protected void setConfiguration(CatalogConfiguration catalogConfiguration) throws CatalogIOException {
        try {
            rootDir = UriUtils.createDirectoryUri(catalogConfiguration.getDataDir());
        } catch (URISyntaxException e) {
            throw new CatalogIOException("Malformed URI 'OPENCGA.CATALOG.MAIN.ROOTDIR'", e);
        }
        if (!rootDir.getScheme().equals("file")) {
            throw new CatalogIOException("wrong posix file system in catalog.properties: " + rootDir);
        }
        if (catalogConfiguration.getTempJobsDir().isEmpty()) {
            jobsDir = rootDir.resolve(DEFAULT_OPENCGA_JOBS_FOLDER);
        } else {
            try {
                jobsDir = UriUtils.createDirectoryUri(catalogConfiguration.getTempJobsDir());
            } catch (URISyntaxException e) {
                throw new CatalogIOException("Malformed URI 'OPENCGA.CATALOG.MAIN.ROOTDIR'", e);
            }
        }
        if (!jobsDir.getScheme().equals("file")) {
            throw new CatalogIOException("wrong posix file system in catalog.properties: " + jobsDir);
        }
    }

    /*
     * FS Utils
     *
     */

    @Override
    protected void checkUriExists(URI uri) throws CatalogIOException {
        if (uri == null || !Files.exists(Paths.get(uri))) {
            throw new CatalogIOException("Path '" + String.valueOf(uri) + "' is null or it does not exist");
        }
    }

    @Override
    protected void checkUriScheme(URI uri) throws CatalogIOException {
        if (uri == null) {    //If scheme is missing, use "file" as scheme
            throw new CatalogIOException("URI is null");
        } else if (uri.getScheme() != null && !uri.getScheme().equals("file")) {    //If scheme is missing, use "file" as scheme
            throw new CatalogIOException("Unknown URI.scheme for URI '" + String.valueOf(uri) + "'");
        }
    }

    @Override
    protected void checkDirectoryUri(URI uri, boolean writable) throws CatalogIOException {
        if (uri == null) {
            throw new CatalogIOException("URI is null");
        } else {
            Path path = Paths.get(uri.getPath());
            if (!Files.exists(path) || !Files.isDirectory(path)) {
                throw new CatalogIOException("Path '" + uri.toString() + "' is null, it does not exist or it's not a directory");
            }
            if (writable && !Files.isWritable(path)) {
                throw new CatalogIOException("Path '" + uri.toString() + "' is not writable");
            }
        }
    }

    private void checkDirectoryPath(Path path, boolean writable) throws CatalogIOException {
        if (path == null || !Files.exists(path) || !Files.isDirectory(path)) {
            throw new CatalogIOException("Path '" + String.valueOf(path) + "' is null, it does not exist or it's not a directory");
        }

        if (writable && !Files.isWritable(path)) {
            throw new CatalogIOException("Path '" + path.toString() + "' is not writable");
        }
    }

    @Override
    public boolean exists(URI uri) {
        return Files.exists(Paths.get(uri));
    }

    @Override
    public URI createDirectory(URI uri, boolean parents) throws CatalogIOException {
        try {
            if (parents) {
                return Files.createDirectories(Paths.get(uri)).toUri();
            } else {
                return Files.createDirectory(Paths.get(uri)).toUri();
            }
        } catch (IOException e) {
            throw new CatalogIOException("Error creating directory " + uri + " with parents=" + parents, e);
        }
    }

    @Override
    public void deleteDirectory(URI uri) throws CatalogIOException {
        try {
            IOUtils.deleteDirectory(Paths.get(uri));
        } catch (IOException e) {
            throw new CatalogIOException("Could not delete directory " + uri, e);
        }
    }

    @Override
    public void deleteFile(URI fileUri) throws CatalogIOException {
        try {
            Files.delete(Paths.get(fileUri));
        } catch (IOException e) {
            throw new CatalogIOException("Could not delete file " + fileUri, e);
        }
    }

    @Override
    public void rename(URI oldName, URI newName) throws CatalogIOException {
        String parent;
        if (isDirectory(oldName)) { // if oldName is a file
            parent = "..";
        } else {
            parent = ".";
        }
        checkUriExists(oldName);
        checkDirectoryUri(oldName.resolve(parent), true);

        try {
            if (!Files.exists(Paths.get(newName))) {
                Files.move(Paths.get(oldName), Paths.get(newName));
            } else {
                throw new CatalogIOException("Unable to rename. File \"" + newName + "\" already exists");
            }
        } catch (IOException e) {
            throw new CatalogIOException("Unable to rename file", e);
        }
    }

    @Override
    public boolean isDirectory(URI uri) {
        return Paths.get(uri).toFile().isDirectory();
//        return uri.getRawPath().endsWith("/");
    }

    @Override
    public void copyFile(URI source, URI target) throws IOException, CatalogIOException {
        checkUriExists(source);
        if ("file".equals(source.getScheme()) && "file".equals(target.getScheme())) {
            Files.copy(Paths.get(source), Paths.get(target), StandardCopyOption.REPLACE_EXISTING);
        } else {
            throw new CatalogIOException("Expected posix file system URIs.");
        }
    }

    @Override
    public void moveFile(URI source, URI target) throws IOException, CatalogIOException {
        checkUriExists(source);
        if (source.getScheme().equals("file") && target.getScheme().equals("file")) {
            Files.move(Paths.get(source), Paths.get(target), StandardCopyOption.REPLACE_EXISTING);
        } else {
            throw new CatalogIOException("Can't move from " + source.getScheme() + " to " + target.getScheme());
        }
    }

    /*****************************
     * Get Path methods
     * ***************************
     */
/*
    public Path getStudyPath(String userId, String projectId, String studyId) throws CatalogIOManagerException {
        checkParam(projectId);
        checkParam(studyId);

        URI path = getUserUri(userId).resolve(USER_PROJECTS_FOLDER).resolve(projectId).resolve(studyId);
        checkUriExists(path);

        return null;
    }*/


//    public Path getFileUri(String userId, String projectId, String studyId, String relativeFilePath, boolean check) throws
// CatalogIOManagerException {
//        checkParam(relativeFilePath);
//
//        Path path = getStudyPath(userId, projectId, studyId).resolve(relativeFilePath);
//        if(check) {
//            checkPath(path);
//        }
//
//        return path;
//    }
/*
    @Override
    public URI getFileUri(String userId, String projectId, String studyId, String relativeFilePath) throws CatalogIOManagerException {
        checkParam(relativeFilePath);

        URI uri = getStudyPath(userId, projectId, studyId).resolve(relativeFilePath).toUri();

        return uri;
    }*/
    public URI getTmpUri() {
        return tmp;
    }

    /**
     * User methods
     * ***************************
     */
//    public Path insertUser(String userId) throws CatalogIOManagerException {
//        checkParam(userId);
//
//        Path usersPath = Paths.get(opencgaRootDir, OPENCGA_USERS_FOLDER);
//        checkDirectoryPath(usersPath, true);
//
//        Path userPath = usersPath.resolve(userId);
//        try {
//            if(!Files.exists(userPath)) {
//                Files.createDirectory(userPath);
//                Files.createDirectory(Paths.get(userPath.toString(), PosixCatalogIOManager.USER_PROJECTS_FOLDER));
//                Files.createDirectory(Paths.get(userPath.toString(), PosixCatalogIOManager.USER_BIN_FOLDER));
//
//                return userPath;
//            }
//        } catch (IOException e) {
//            throw new CatalogIOManagerException("IOException" + e.toString());
//        }
//        return null;
//    }
/*
    public void deleteUser(String userId) throws CatalogIOManagerException {
        URI userUri = getUserUri(userId);
        checkUriExists(userUri);
        try {
            IOUtils.deleteDirectory(Paths.get(userUri));
        } catch (IOException e) {
            throw new CatalogIOManagerException("IOException: " + e.toString());
        }
    }

    public URI createAnonymousUser(String anonymousUserId) throws CatalogIOManagerException {
        checkParam(anonymousUserId);

        URI usersUri = opencgaRootDir.resolve(OPENCGA_ANONYMOUS_USERS_FOLDER);
        checkDirectoryUri(usersUri, true);

        URI userUri = usersUri.resolve(anonymousUserId);
        try {
            if(!exists(userUri)) {
                createDirectory(userUri);
                createDirectory(userUri.resolve(USER_PROJECTS_FOLDER));
                createDirectory(userUri.resolve(USER_BIN_FOLDER));

                return userUri;
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("IOException" + e.toString());
        }
        return null;
    }

    public void deleteAnonymousUser(String anonymousUserId) throws CatalogIOManagerException {
        URI anonymousUserUri = getAnonymousUserUri(anonymousUserId);
        checkUriExists(anonymousUserUri);

        try {
            IOUtils.deleteDirectory(anonymousUserUri);
        } catch (IOException e1) {
            throw new CatalogIOManagerException("IOException: " + e1.toString());
        }
//        return anonymousUserPath;
    }
*/

    /*****************************
     * Project methods ***********
     * ***************************
     */
    /*
    public Path createProject(String userId, String projectId) throws CatalogIOManagerException {
        checkParam(projectId);

        Path projectRootPath = getProjectsUri(userId);
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
        Path projectPath = getProjectUri(userId, projectId);
        checkPath(projectPath);

        try {
            IOUtils.deleteDirectory(projectPath);
        } catch (IOException e) {
            throw new CatalogIOManagerException("deleteProject(): could not delete the project folder: " + e.toString());
        }

        return projectPath;
    }

    public void renameProject(String userId, String oldProjectId, String newProjectId) throws CatalogIOManagerException {
        Path oldFolder =  getProjectUri(userId, oldProjectId);
        Path newFolder =  getProjectUri(userId, newProjectId);
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
        return Files.exists(getProjectUri(userId, projectId));
    }

    /**
     * Project Study
     * ***************************
     */
    @Override
    public void createFile(URI fileUri, InputStream inputStream) throws CatalogIOException {
        try {
            Files.copy(inputStream, Paths.get(fileUri), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new CatalogIOException("create file failed at copying file " + fileUri);
        }
    }

    /**
     * Job methods
     * ***************************
     */
//    public URI createJob(String accountId, String projectId, String jobId) throws CatalogIOManagerException {
//        // String path = getUserUri(accountId) + "/jobs";
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


    /*
     * *****************
     * <p>
     * OBJECT METHODS
     * <p>
     * ******************
     */

//    public Path createObject(String userId, String projectId, String studyId, Path objectId, File file,
//                             InputStream fileIs, boolean parents) throws CatalogIOManagerException, IOException {
//
//        Path auxFullFilePath = getFileUri(userId, projectId, studyId, objectId);
//        Path fullFilePath = getFileUri(userId, projectId, objectId);
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
    @Override
    public DataInputStream getFileObject(URI fileUri, int start, int limit)
            throws CatalogIOException {

        Path objectPath = Paths.get(fileUri);

        if (Files.isRegularFile(objectPath)) {
            try {
                if (start == -1 && limit == -1) {
                    return new DataInputStream(Files.newInputStream(objectPath));
                } else {
                    return new DataInputStream(IOUtils.headOffset(objectPath, start, limit));
                }
            } catch (IOException e) {
                throw new CatalogIOException("Unable to read file", e);
            }
        } else {
            throw new CatalogIOException("Not a regular file: " + objectPath.toAbsolutePath().toString());
        }
    }


    @Override
    public DataInputStream getGrepFileObject(URI fileUri, String pattern,
                                             boolean ignoreCase, boolean multi)
            throws CatalogIOException {
        Path path = Paths.get(fileUri);
        if (Files.isRegularFile(path)) {
            try {
                return new DataInputStream(IOUtils.grepFile(path, pattern, ignoreCase, multi));
            } catch (IOException e) {
                throw new CatalogIOException("Error while grep file", e);
            }
        } else {
            throw new CatalogIOException("Not a regular file: " + path.toAbsolutePath().toString());
        }
    }

    @Override
    public DataOutputStream createOutputStream(URI fileUri, boolean overwrite) throws CatalogIOException {
        Path path = Paths.get(fileUri);
        logger.info("URI: {}", fileUri);
        if (overwrite || !Files.exists(path)) {
            try {
                return new DataOutputStream(new FileOutputStream(path.toFile()));
            } catch (IOException e) {
                throw new CatalogIOException("Unable to create file", e);
            }
        } else {
            throw new CatalogIOException("File already exists");
        }
    }

    @Override
    public String calculateChecksum(URI file) throws CatalogIOException {
        String checksum;
        try {
            String[] command = {"md5sum", file.getPath()};
            logger.debug("command = {} {}", command[0], command[1]);
            Process p = Runtime.getRuntime().exec(command);
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            checksum = br.readLine();

            if (p.waitFor() != 0) {
                //TODO: Handle error in checksum
                logger.info("checksum = " + checksum);
                br = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                throw new CatalogIOException("md5sum failed with exit value : " + p.exitValue() + ". ERROR: " + br.readLine());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            //TODO: Handle error in checksum
            throw new CatalogIOException("Checksum error in file " + file, e);
        }

        return checksum.split(" ")[0];
    }

    @Override
    public List<URI> listFiles(URI directory) throws CatalogIOException {
        checkUriExists(directory);
        class ListFiles extends SimpleFileVisitor<Path> {
            private List<String> filePaths = new LinkedList<>();

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                filePaths.add(file.toAbsolutePath().toString());
                return super.visitFile(file, attrs);
            }

            public List<String> getFilePaths() {
                return filePaths;
            }
        }
        ListFiles fileVisitor = new ListFiles();
        try {
            Files.walkFileTree(Paths.get(directory.getPath()), fileVisitor);
        } catch (IOException e) {
            throw new CatalogIOException("Unable to walkFileTree", e);
        }
        List<URI> fileUris = new LinkedList<>();

        for (String filePath : fileVisitor.getFilePaths()) {
            try {
                fileUris.add(new URI("file", filePath, null));
            } catch (URISyntaxException e) {
                throw CatalogIOException.uriSyntaxException(filePath, e);
            }
        }
        return fileUris;
    }

    @Override
    public Stream<URI> listFilesStream(URI directory) throws CatalogIOException {
        try {
            return Files.walk(Paths.get(directory.getPath()))
                    .map(Path::toUri)
                    .filter(uri -> !uri.equals(directory));
//                    .filter(uri -> !uri.getPath().endsWith("/"))
        } catch (IOException e) {
            throw new CatalogIOException("Unable to list files", e);
        }
    }

    @Override
    public long getFileSize(URI file) throws CatalogIOException {
        checkUriScheme(file);
        try {
            return Files.size(Paths.get(file));
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogIOException("Can't get file size", e);
        }
    }

    @Override
    public Date getCreationDate(URI file) throws CatalogIOException {
        checkUriScheme(file);
        try {
            return Date.from(Files.readAttributes(Paths.get(file), BasicFileAttributes.class).creationTime().toInstant());
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogIOException("Can't get file size", e);
        }
    }

    @Override
    public Date getModificationDate(URI file) throws CatalogIOException {
        checkUriScheme(file);
        try {
            return Date.from(Files.readAttributes(Paths.get(file), BasicFileAttributes.class).lastModifiedTime().toInstant());
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogIOException("Can't get file size", e);
        }
    }


//    public String getFileTableFromJob(Path jobPath, String filename, String start, String limit, String colNames,
//                                      String colVisibility, String sort) throws CatalogIOManagerException, IOException {
//        Path jobFile = jobPath.resolve(filename);
//        return JobFileIOUtils.getSenchaTable(jobFile, filename, start, limit, colNames, colVisibility, sort);
//    }

/*    @Override
    public DataInputStream getFileFromJob(Path jobPath, String filename, String zip) throws CatalogIOManagerException,
            FileNotFoundException {

        // String fileStr = getJobPath(accountId, bucketId, jobId).toString();
        Path filePath = jobPath.resolve(filename);
        File file = filePath.toFile();
        String name = filename.replace("src/main", "").replace("/", "");
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
*/
    /*
    public DataInputStream getGrepFileFromJob(Path jobPath, String filename, String pattern, boolean ignoreCase, boolean multi) throws
    CatalogIOManagerException,
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
    */


    /**
     * **********
     */

    // public String getDataPath(String wsDataId){
    // wsDataId.replaceAll(":", "/")
    // }

    // public String getJobPath(String accountId, String bucketId, String jobId)
    // {
    // return getUserUri(accountId) + "/jobs/" + jobId;
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
