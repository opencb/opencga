package org.opencb.opencga.catalog.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public abstract class CatalogIOManager {

    //    private Path opencgaRootDirPath;
    protected URI opencgaRootDir;
    protected URI tmp;


    /**
     * OpenCGA folders are created in the ROOTDIR:
     *  OPENCGA_USERS_FOLDER contains users workspaces organized by 'userId'
     *  OPENCGA_ANONYMOUS_USERS_FOLDER contains anonymous users workspaces organized by 'randomStringId'
     *  OPENCGA_BIN_FOLDER contains all packaged binaries delivered within OpenCGA
     */
    protected static final String OPENCGA_USERS_FOLDER = "users";
    protected static final String OPENCGA_ANONYMOUS_USERS_FOLDER = "anonymous";
    protected static final String OPENCGA_BIN_FOLDER = "bin";

    /**
     * Users folders are created inside user workspace:
     *  USER_PROJECTS_FOLDER this folder stores all the projects with the studies and files
     *  USER_BIN_FOLDER contains user specific binaries
     */
    protected static final String USER_PROJECTS_FOLDER = "projects";
    protected static final String USER_BIN_FOLDER = "bin";
    protected static final String SHARED_DATA_FOLDER = "shared_data";

    protected Properties properties;
    protected static Logger logger;


    public CatalogIOManager(String propertiesFile){

    }

    public CatalogIOManager(Properties properties){

        logger = LoggerFactory.getLogger(this.getClass());
    }


    /**
     * This method creates the folders and workspace structure for storing the OpenCGA data. I
     * @throws IOException
     */
    public void setup() throws IOException {
        Path opencgaRootDirPath = Paths.get(this.opencgaRootDir);
        if(!Files.exists(opencgaRootDirPath) && Files.isDirectory(opencgaRootDirPath) && Files.isWritable(opencgaRootDirPath)) {
            new IOException("OpenCGA ROOTDIR does not exist or it is not a writable directory");
        }

        if(!Files.exists(opencgaRootDirPath.resolve(OPENCGA_USERS_FOLDER))) {
            Files.createDirectory(opencgaRootDirPath.resolve(OPENCGA_USERS_FOLDER));
        }

        if(!Files.exists(opencgaRootDirPath.resolve(OPENCGA_ANONYMOUS_USERS_FOLDER))) {
            Files.createDirectory(opencgaRootDirPath.resolve(OPENCGA_ANONYMOUS_USERS_FOLDER));
        }

        if(!Files.exists(opencgaRootDirPath.resolve(OPENCGA_BIN_FOLDER))) {
            Files.createDirectory(opencgaRootDirPath.resolve(OPENCGA_BIN_FOLDER));
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

    public abstract URI createDirectory(URI uri) throws IOException;

    public abstract void deleteDirectory(URI uri) throws IOException;



    public abstract URI getUserUri(String userId) throws CatalogIOManagerException;

    public abstract URI getAnonymousUserUri(String userId) throws CatalogIOManagerException;

    public abstract URI getProjectsUri(String userId) throws CatalogIOManagerException;

    public abstract URI getProjectUri(String userId, String projectId) throws CatalogIOManagerException;

    public abstract URI getStudyUri(String userId, String projectId, String studyId) throws CatalogIOManagerException;

    public abstract URI getFileUri(String userId, String projectId, String studyId, String relativeFilePath)
            throws CatalogIOManagerException;

    public abstract URI getTmpUri();



    public URI createUser(String userId) throws CatalogIOManagerException {
        checkParam(userId);

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
        URI usersPath = opencgaRootDir.resolve(CatalogIOManager.OPENCGA_USERS_FOLDER);
        checkDirectoryUri(usersPath, true);

        URI userPath = usersPath.resolve(userId);
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
    };

    public  URI createAnonymousUser(String anonymousUserId) throws CatalogIOManagerException {
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
    };

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
        checkDirectoryUri(projectRootUri, true);

        URI projectUri = projectRootUri.resolve(projectId);
        try {
            if(!exists(projectUri)) {
                projectUri = createDirectory(projectUri);
                createDirectory(projectUri.resolve(SHARED_DATA_FOLDER));
            }
        } catch (IOException e) {
            throw new CatalogIOManagerException("createProject(): could not create the bucket folder: " + e.toString());
        }

        return projectUri;
    }

    public void deleteProject(String userId, String projectId) throws CatalogIOManagerException;

    public abstract void renameProject(String userId, String oldProjectId, String newProjectId)
            throws CatalogIOManagerException;

    public abstract boolean existProject(String userId, String projectId) throws CatalogIOManagerException;

    public abstract URI createStudy(String userId, String projectId, String studyId) throws CatalogIOManagerException;

    public abstract void deleteStudy(String userId, String projectId, String studyId) throws CatalogIOManagerException;

    public abstract void renameStudy(String userId, String projectId, String oldStudyId, String newStudyId)
            throws CatalogIOManagerException;

    public abstract URI createFolder(String userid, String projectId, String studyId, String fileId, boolean parent)
            throws CatalogIOManagerException;

    public abstract void createFile(String userId, String projectId, String studyId, String objectId, InputStream inputStream)
            throws CatalogIOManagerException;

    public abstract void deleteFile(String userId, String projectId, String studyId, String objectId)
            throws CatalogIOManagerException;

    public abstract DataInputStream getFileObject(String userid, String projectId, String studyId, String objectId,int start, int limit)
            throws CatalogIOManagerException, IOException;

    public abstract DataInputStream getGrepFileObject(String userId, String projectId, String studyId, String objectId,
                                                      String pattern, boolean ignoreCase, boolean multi)
            throws CatalogIOManagerException, IOException;

    public abstract DataInputStream getFileFromJob(Path jobPath, String filename, String zip)
            throws CatalogIOManagerException,FileNotFoundException;

    public abstract DataInputStream getGrepFileFromJob(Path jobPath, String filename, String pattern, boolean ignoreCase,
                                                       boolean multi) throws CatalogIOManagerException,IOException;

    public abstract InputStream getJobZipped(Path jobPath, String jobId) throws CatalogIOManagerException, IOException;

}
