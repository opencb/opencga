package org.opencb.opencga.catalog.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public abstract class CatalogIOManager {

    //    private Path opencgaRootDirPath;
    protected URI rootDir;
//    protected URI rootDir;
    protected URI tmp;


    /**
     * OpenCGA folders are created in the ROOTDIR:
     *  OPENCGA_USERS_FOLDER contains users workspaces organized by 'userId'
     *  OPENCGA_ANONYMOUS_USERS_FOLDER contains anonymous users workspaces organized by 'randomStringId'
     *  OPENCGA_BIN_FOLDER contains all packaged binaries delivered within OpenCGA
     */
    protected static final String OPENCGA_USERS_FOLDER = "users/";
    protected static final String OPENCGA_ANONYMOUS_USERS_FOLDER = "anonymous/";
    protected static final String OPENCGA_BIN_FOLDER = "bin/";

    /**
     * Users folders are created inside user workspace:
     *  USER_PROJECTS_FOLDER this folder stores all the projects with the studies and files
     *  USER_BIN_FOLDER contains user specific binaries
     */
    protected static final String USER_PROJECTS_FOLDER = "projects/";
    protected static final String USER_BIN_FOLDER = "bin/";
    protected static final String SHARED_DATA_FOLDER = "shared_data/";

    protected Properties properties;
    protected static Logger logger;

    private CatalogIOManager() {
        logger = LoggerFactory.getLogger(this.getClass());
    }

    public CatalogIOManager(String propertiesFile) throws IOException, CatalogIOManagerException {
        this();
        this.properties = new Properties();
        this.properties.load(new FileInputStream(propertiesFile));
        setup();
    }

    public CatalogIOManager(Properties properties) throws IOException, CatalogIOManagerException {
        this();
        this.properties = properties;
        setup();
    }

    protected abstract void setProperties(Properties properties) throws CatalogIOManagerException;

    /**
     * This method creates the folders and workspace structure for storing the OpenCGA data. I
     * @throws IOException
     */
    public void setup() throws CatalogIOManagerException, IOException {
        setProperties(properties);
        checkDirectoryUri(rootDir, true);

        if(!exists(rootDir.resolve(OPENCGA_USERS_FOLDER))) {
            createDirectory(rootDir.resolve(OPENCGA_USERS_FOLDER));
        }

        if(!exists(rootDir.resolve(OPENCGA_ANONYMOUS_USERS_FOLDER))) {
            createDirectory(rootDir.resolve(OPENCGA_ANONYMOUS_USERS_FOLDER));
        }

        if(!exists(rootDir.resolve(OPENCGA_BIN_FOLDER))) {
            createDirectory(rootDir.resolve(OPENCGA_BIN_FOLDER));
        }
    }

    protected void checkParam(String param) throws CatalogIOManagerException {
        if(param == null || param.equals("")) {
            throw new CatalogIOManagerException("Parameter '" + param + "' not valid");
        }
    }

    protected abstract void checkUri(URI param) throws CatalogIOManagerException;

    protected abstract void checkDirectoryUri(URI param, boolean writable) throws CatalogIOManagerException;

    public abstract boolean exists(URI uri);

    public abstract URI createDirectory(URI uri, boolean parents) throws IOException;

    public URI createDirectory(URI uri) throws IOException {
        return createDirectory(uri, false);
    }

    public abstract void deleteDirectory(URI uri) throws IOException;

    protected abstract void deleteFile(URI fileUri) throws IOException;

    public abstract void rename(URI oldName, URI newName) throws CatalogIOManagerException, IOException;

    public abstract boolean isDirectory(URI uri);



    public URI getUsersUri() throws CatalogIOManagerException {
        return rootDir.resolve(OPENCGA_USERS_FOLDER);
    }

    public URI getAnonymousUsersUri() throws CatalogIOManagerException {
        return rootDir.resolve(OPENCGA_ANONYMOUS_USERS_FOLDER);
    }

    public URI getUserUri(String userId) throws CatalogIOManagerException {
        checkParam(userId);
        return getUsersUri().resolve(userId + "/");
    }

    public URI getAnonymousUserUri(String userId) throws CatalogIOManagerException{ // FIXME: Should replicate to getAnonymousPojectUri, ...Study..., etc ?
        checkParam(userId);
        return getAnonymousUsersUri().resolve(userId + "/");
    }

    public URI getProjectsUri(String userId) throws CatalogIOManagerException {
        return getUserUri(userId).resolve(USER_PROJECTS_FOLDER);
    }

    public URI getProjectUri(String userId, String projectId) throws CatalogIOManagerException {
        return getProjectsUri(userId).resolve(projectId + "/");
    }

    public URI getStudyUri(String userId, String projectId, String studyId) throws CatalogIOManagerException {
        checkParam(studyId);
        return getProjectUri(userId, projectId).resolve(studyId + "/");
    }

    public URI getFileUri(String userId, String projectId, String studyId, String relativeFilePath)
            throws CatalogIOManagerException {
        checkParam(relativeFilePath);
        return getStudyUri(userId, projectId, studyId).resolve(relativeFilePath);
    }

    public abstract URI getTmpUri();    // FIXME Still used?



    public URI createUser(String userId) throws CatalogIOManagerException {
        checkParam(userId);

        //    //just to show the previous version
        //        Path usersPath = Paths.get(opencgaRootDir, CatalogIOManager.OPENCGA_USERS_FOLDER);
        //        checkDirectoryUri(usersPath, true);
        //
        //        Path userPath = usersPath.resolve(userId);

        //        try {
        //            if(!Files.exists(userPath)) {
        //                Files.createDirectory(userPath);
        //                Files.createDirectory(Paths.get(userPath.toString(), CatalogIOManager.USER_PROJECTS_FOLDER));
        //                Files.createDirectory(Paths.get(userPath.toString(), CatalogIOManager.USER_BIN_FOLDER));
        //
        //                return userPath;
        //            }
        //        } catch (IOException e) {
        //            throw new CatalogIOManagerException("IOException" + e.toString());
        //        }
        //        return null;

//        URI opencgaPath = new URI(opencgaRootDir);
        URI usersUri = getUsersUri();
        checkDirectoryUri(usersUri, true);

        URI userPath = getUserUri(userId);
        try {
            if(!exists(userPath)) {
                createDirectory(userPath);
                createDirectory(userPath.resolve(CatalogIOManager.USER_PROJECTS_FOLDER));
                createDirectory(userPath.resolve(CatalogIOManager.USER_BIN_FOLDER));

                return userPath;
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("IOException" + e.toString());
        }
        return null;
    }

    public void deleteUser(String userId) throws CatalogIOManagerException {
        URI userUri = getUserUri(userId);
        checkUri(userUri);
        try {
            deleteDirectory(userUri);
        } catch (IOException e) {
            throw new CatalogIOManagerException("IOException: " + e.toString());
        }
    }

    public  URI createAnonymousUser(String anonymousUserId) throws CatalogIOManagerException {
        checkParam(anonymousUserId);

        URI usersUri = getAnonymousUsersUri();
        checkDirectoryUri(usersUri, true);

        URI userUri = getAnonymousUserUri(anonymousUserId);
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

    public  void deleteAnonymousUser(String anonymousUserId) throws CatalogIOManagerException {
        URI anonymousUserUri = getAnonymousUserUri(anonymousUserId);
        checkUri(anonymousUserUri);

        try {
            deleteDirectory(anonymousUserUri);
        } catch (IOException e1) {
            throw new CatalogIOManagerException("IOException: " + e1.toString());
        }
//        return anonymousUserPath;
    }

    public URI createProject(String userId, String projectId) throws CatalogIOManagerException{
        checkParam(projectId);

        URI projectRootUri = getProjectsUri(userId);
//        checkDirectoryUri(projectRootUri, true);  //assuming catalogManager has checked it

        URI projectUri = projectRootUri.resolve(projectId);
        try {
            if(!exists(projectUri)) {
                projectUri = createDirectory(projectUri, true);
                //createDirectory(projectUri.resolve(SHARED_DATA_FOLDER));
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("createProject(): could not create the bucket folder: " + e.toString());
        }

        return projectUri;
    }

    public void deleteProject(String userId, String projectId) throws CatalogIOManagerException {
        URI projectUri = getProjectUri(userId, projectId);
        checkUri(projectUri);

        try {
            deleteDirectory(projectUri);
        } catch (IOException e) {
            throw new CatalogIOManagerException("deleteProject(): could not delete the project folder: " + e.toString());
        }
    }

    public void renameProject(String userId, String oldProjectId, String newProjectId)
            throws CatalogIOManagerException {
        URI oldFolder = getProjectUri(userId, oldProjectId);
        URI newFolder = getProjectUri(userId, newProjectId);

        try {
            rename(oldFolder, newFolder);
        } catch (IOException e) {
            throw new CatalogIOManagerException("renameProject(): could not rename the project folder: " + e.toString());
        }
    }

    public boolean existProject(String userId, String projectId) throws CatalogIOManagerException {
        return exists(getProjectUri(userId, projectId));
    }

    public URI createStudy(String userId, String projectId, String studyId) throws CatalogIOManagerException {
        checkParam(studyId);

        URI projectUri = getProjectUri(userId, projectId);
        checkDirectoryUri(projectUri, true);

        URI studyUri = projectUri.resolve(studyId);
        try {
            if(!exists(studyUri)) {
                studyUri = createDirectory(studyUri);
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("createStudy method: could not create the study folder: " + e.toString());
        }

        return studyUri;
    }

    public void deleteStudy(String userId, String projectId, String studyId) throws CatalogIOManagerException {
        URI studyUri = getStudyUri(userId, projectId, studyId);
        checkUri(studyUri);

        try {
            deleteDirectory(studyUri);
        } catch (IOException e) {
            throw new CatalogIOManagerException("deleteProject(): could not delete the project folder: " + e.toString());
        }
    }

    public void renameStudy(String userId, String projectId, String oldStudyId, String newStudyId)
            throws CatalogIOManagerException {
        URI oldFolder = getStudyUri(userId, projectId, oldStudyId);
        URI newFolder = getStudyUri(userId, projectId, newStudyId);

        try {
            rename(oldFolder, newFolder);
        } catch (IOException e) {
            throw new CatalogIOManagerException("renameStudy(): could not rename the study folder: " + e.toString());
        }
    }

    public URI createFolder(String userid, String projectId, String studyId, String folderName, boolean parent)
            throws CatalogIOManagerException {
        checkParam(folderName);
        if(!folderName.endsWith("/")) {
            folderName += "/";
        }
        URI studyUri = getStudyUri(userid, projectId, studyId);
        checkDirectoryUri(studyUri, true);

//        Path fullFolderPath = getFileUri(userid, projectId, studyId, objectId);
        URI folderUri = studyUri.resolve(folderName);
        try {
            if(!exists(folderUri)) {
                if(parent) {
                    createDirectory(folderUri, true);
                } else {
                    checkDirectoryUri(folderUri.resolve(".."), true);
                    createDirectory(folderUri);
                }
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("createFolder(): could not create the directory " + e.toString());
        }

        return folderUri;
    }

    public abstract void createFile(String userId, String projectId, String studyId, String filePath, InputStream inputStream)
            throws CatalogIOManagerException;

    public void deleteFile(String userId, String projectId, String studyId, String filePath)
            throws CatalogIOManagerException {
        URI fileUri = getFileUri(userId, projectId, studyId, filePath);
        checkUri(fileUri);

        logger.debug("Deleting {}", fileUri.toString());
        try {
            if(isDirectory(fileUri)) {
                deleteDirectory(fileUri);
            }else {
                deleteFile(fileUri);
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("deleteFile(): could not delete the object " + e.toString());
        }
    }


    public abstract DataInputStream getFileObject(String userid, String projectId, String studyId, String objectId,int start, int limit)
            throws CatalogIOManagerException, IOException;

    public abstract DataInputStream getGrepFileObject(String userId, String projectId, String studyId, String objectId,
                                                      String pattern, boolean ignoreCase, boolean multi)
            throws CatalogIOManagerException, IOException;
//
//    public abstract DataInputStream getFileFromJob(Path jobPath, String filename, String zip)
//            throws CatalogIOManagerException,FileNotFoundException;
//
//    public abstract DataInputStream getGrepFileFromJob(Path jobPath, String filename, String pattern, boolean ignoreCase,
//                                                       boolean multi) throws CatalogIOManagerException,IOException;
//
//    public abstract InputStream getJobZipped(Path jobPath, String jobId) throws CatalogIOManagerException, IOException;

}
