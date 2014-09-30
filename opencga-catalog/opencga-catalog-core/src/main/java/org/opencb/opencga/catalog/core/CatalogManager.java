package org.opencb.opencga.catalog.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.*;
import org.opencb.opencga.catalog.core.db.CatalogDBAdaptor;
import org.opencb.opencga.catalog.core.db.CatalogManagerException;
import org.opencb.opencga.catalog.core.db.CatalogMongoDBAdaptor;
import org.opencb.opencga.catalog.core.io.CatalogIOManagerException;
import org.opencb.opencga.catalog.core.io.PosixIOManager;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MongoCredentials;

import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
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

    private Properties properties;

    protected static Logger logger = LoggerFactory.getLogger(CatalogManager.class);
    public static final String USER_OTHERS_ID = "*";
    protected static final String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
    protected static final Pattern emailPattern = Pattern.compile(EMAIL_PATTERN);


    public CatalogManager() throws IOException, CatalogIOManagerException, CatalogManagerException {
        this(System.getenv("OPENCGA_HOME"));
    }

    public CatalogManager(CatalogDBAdaptor catalogDBAdaptor, PosixIOManager ioManager, Properties catalogProperties) {
        this.catalogDBAdaptor = catalogDBAdaptor;
        this.ioManager = ioManager;
        this.properties = catalogProperties;
    }

    public CatalogManager(String rootdir) throws IOException, CatalogIOManagerException, CatalogManagerException {
//        properties = Config.getAccountProperties();

        Path path = Paths.get(rootdir, "conf", "catalog.properties");
        properties = new Properties();
        try {
            properties.load(Files.newInputStream(path));
        } catch (IOException e) {
            logger.error("Failed to load account.properties: " + e.getMessage());
        }

//        if (properties.getProperty("OPENCGA.ACCOUNT.MODE").equals("file")) {
//            catalogDBAdaptor = (CatalogDBAdaptor) new CatalogMongoDBAdaptor( --- );
//        } else {
//            catalogDBAdaptor = new CatalogMongoDBAdaptor(new MongoCredentials(properties));
//        }
        catalogDBAdaptor = new CatalogMongoDBAdaptor(new MongoCredentials(properties));
        ioManager = new PosixIOManager(rootdir);
    }

    public CatalogManager(Properties properties) throws IOException, CatalogIOManagerException, CatalogManagerException {
        this.properties = properties;

        try {
            MongoCredentials mongoCredentials = new MongoCredentials(
                    properties.getProperty("HOST"),
                    Integer.parseInt(properties.getProperty("PORT")),
                    properties.getProperty("DATABASE"),
                    properties.getProperty("USER"),
                    properties.getProperty("PASSWORD"));
            catalogDBAdaptor = new CatalogMongoDBAdaptor(mongoCredentials);
            ioManager = new PosixIOManager(properties.getProperty("ROOTDIR"));
        } catch (IllegalOpenCGACredentialsException e) {
            e.printStackTrace();
        }
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

    public Path getFilePath(String userId, String projectId, String studyId, String relativeFilePath)
            throws CatalogIOManagerException {
        return ioManager.getFilePath(userId, projectId, studyId, relativeFilePath);
    }

//    public Path getJobFolderPath(String userId, String projectId, Path JobId) {
//        return ioManager.getJobFolderPath(userId, projectId, JobId);
//    }

    public Path getTmpPath() {
        return ioManager.getTmpPath();
    }

//    public File getFile(String userId, String projectAlias, String studyAlias, Path filePath,
//                                    String sessionId) throws CatalogManagerException, IOException {
//        QueryResult queryResult = catalogDBAdaptor.getFile(userId, projectAlias, studyAlias, filePath, sessionId);
//        if(queryResult.getNumResults() != 1){
//            return null;
//        } else {
//            return (File) queryResult.getResult().get(0);
//        }
//    }

    /**
     * User methods
     * ***************************
     */

    @Deprecated
    public QueryResult<User> createUser(User user)
            throws CatalogManagerException, CatalogIOManagerException, JsonProcessingException {
        checkObj(user, "user");
        checkParameter(user.getId(), "id");
        checkParameter(user.getPassword(), "password");
        checkParameter(user.getName(), "name");
        checkEmail(user.getEmail());

        try {
            ioManager.createUser(user.getId());
            return catalogDBAdaptor.createUser(user);
        } catch (CatalogIOManagerException | CatalogManagerException e) {
            ioManager.deleteUser(user.getId());
            throw e;
        }
    }

    public QueryResult createUser(String id, String name, String email, String password, String organization)
            throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(id, "id");
        checkParameter(password, "password");
        checkParameter(name, "name");
        checkParameter(email, "email");
        checkParameter(organization, "organization");
        User user = new User(id, name, email, password, organization, User.ROLE_USER, "");

        try {
            ioManager.createUser(user.getId());
            return catalogDBAdaptor.createUser(user);
        } catch (CatalogIOManagerException | CatalogManagerException e) {
            ioManager.deleteUser(user.getId());
            throw e;
        }
    }

    public QueryResult<ObjectMap> loginAsAnonymous(String sessionIp)
            throws CatalogManagerException, CatalogIOManagerException, IOException {
        checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp);

        String userId = "anonymous_" + session.getId();

        // TODO sessionID should be created here

        ioManager.createAnonymousUser(userId);

        try {
            return catalogDBAdaptor.loginAsAnonymous(session);
        } catch (CatalogManagerException e) {
            ioManager.deleteUser(userId);
            throw e;
        }

    }

    public QueryResult<ObjectMap> login(String userId, String password, String sessionIp)
            throws CatalogManagerException, IOException {
        checkParameter(userId, "userId");
        checkParameter(password, "password");
        checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp);

        return catalogDBAdaptor.login(userId, password, session);
    }

    public QueryResult logout(String userId, String sessionId) throws CatalogManagerException, IOException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        return catalogDBAdaptor.logout(userId, sessionId);
    }

    public QueryResult logoutAnonymous(String sessionId) throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = "anonymous_" + sessionId;
        checkParameter(userId, "userId");
        checkSessionId(userId, sessionId);

        logger.info("new anonymous user. userId: " + userId + " sesionId: " + sessionId);

        ioManager.deleteAnonymousUser(userId);
        return catalogDBAdaptor.logoutAnonymous(sessionId);
    }

    public QueryResult changePassword(String userId, String password, String nPassword1, String sessionId)
            throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkParameter(password, "password");
        checkParameter(nPassword1, "nPassword1");
        checkSessionId(userId, sessionId);  //Only the user can change his own password
        catalogDBAdaptor.updateUserLastActivity(userId);
        return catalogDBAdaptor.changePassword(userId, password, nPassword1);
    }

    public QueryResult changeEmail(String userId, String nEmail, String sessionId) throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        checkEmail(nEmail);
        catalogDBAdaptor.updateUserLastActivity(userId);
        return catalogDBAdaptor.changeEmail(userId, nEmail);
    }

    public QueryResult resetPassword(String userId, String email) throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkEmail(email);
        catalogDBAdaptor.updateUserLastActivity(userId);
        return catalogDBAdaptor.resetPassword(userId, email);
    }


    public QueryResult<User> getUser(String userId, String lastActivity, String sessionId)
            throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        //FIXME: Should other users get access to other user information? (If so, then filter projects)
        //FIXME: Should setPassword(null)??
        return catalogDBAdaptor.getUser(userId, lastActivity);
    }

    /**
     * Modify some params from the user profile:
     *
     *   name
     *   email
     *   organization
     *
     *   attributes
     *   configs
     *
     * @param userId userId
     * @param parameters Parameters to change.
     * @param sessionId sessionId must match with the userId
     * @return
     * @throws CatalogManagerException
     */
    public QueryResult modifyUser(String userId, ObjectMap parameters, String sessionId)
            throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkObj(parameters, "parameters");
        checkSessionId(userId, sessionId);
        for (String s : parameters.keySet()) {
            if (!s.matches("name|email|organization|attributes|configs")) {
                throw new CatalogManagerException("Parameter '" + s + "' can't be changed");
            }
        }
        if(parameters.containsKey("email")) {
            checkEmail(parameters.getString("email"));
        }
        catalogDBAdaptor.updateUserLastActivity(userId);
        return catalogDBAdaptor.modifyUser(userId, parameters);
    }

    public void deleteUser(String userId, String sessionId) throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        String userIdBySessionId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if(userIdBySessionId == userId
                //|| catalogDBAdaptor.getRole(userIdBySessionId) == User.ROLE_ADMIN
                ) {
            try {
                ioManager.deleteUser(userId);
            } catch (CatalogIOManagerException e) {
                e.printStackTrace();
            }
            catalogDBAdaptor.deleteUser(userId);
        }
    }

    /**
     * Project methods
     * ***************************
     */
    @Deprecated
    public QueryResult<Project> createProject(String ownerId, Project project, String sessionId)
            throws CatalogManagerException,
            CatalogIOManagerException, JsonProcessingException {
        checkObj(project, "project");
        checkParameter(project.getName(), "projectName");
        checkAlias(project.getAlias(), "projectAlias");
        checkParameter(ownerId, "ownerId");
        checkParameter(sessionId, "sessionId");
        checkSessionId(ownerId, sessionId);    //Only the user can create a project

        /* Add default ACL */
        //Add generic permissions to the project.
        project.getAcl().add(new Acl(USER_OTHERS_ID, false, false, false, false));


        QueryResult<Project> result = catalogDBAdaptor.createProject(ownerId, project);
        project = result.getResult().get(0);

        try {
            ioManager.createProject(ownerId, Integer.toString(project.getId()));
        } catch (CatalogIOManagerException e) {
            e.printStackTrace();
            catalogDBAdaptor.deleteProject(project.getId());
        }
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return result;
    }

    public QueryResult<Project> createProject(String ownerId, String name, String alias, String description,
                                              String organization, String sessionId)
            throws CatalogManagerException,
            CatalogIOManagerException, JsonProcessingException {
        checkParameter(ownerId, "ownerId");
        checkParameter(name, "name");
        checkAlias(alias, "alias");
        checkParameter(description, "description");
        checkParameter(organization, "organization");
        checkParameter(sessionId, "sessionId");
        checkSessionId(ownerId, sessionId);    //Only the user can create a project

        Project project = new Project(name, alias, description, "", organization);
        QueryResult<Project> result = catalogDBAdaptor.createProject(ownerId, project);
        project = result.getResult().get(0);

        try {
            ioManager.createProject(ownerId, Integer.toString(project.getId()));
        } catch (CatalogIOManagerException e) {
            e.printStackTrace();
            catalogDBAdaptor.deleteProject(project.getId());
        }
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return result;
    }

    public QueryResult<Project> getProject(int projectId, String sessionId)
            throws CatalogManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        Acl projectAcl = getProjectAcl(userId, projectId);
        if (projectAcl.isRead()) {
            QueryResult<Project> projectResult = catalogDBAdaptor.getProject(projectId);
            if(!projectResult.getResult().isEmpty()){
                filterStudies(userId, projectAcl, projectResult.getResult().get(0).getStudies());
            }
            return projectResult;
        } else {
            throw new CatalogManagerException("Permission denied. Can't read project.");
        }
    }

    public QueryResult<Project> getAllProjects(String ownerId, String sessionId)
            throws CatalogManagerException, JsonProcessingException {
        checkParameter(ownerId, "ownerId");
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        QueryResult<Project> allProjects = catalogDBAdaptor.getAllProjects(ownerId);

        List<Project> projects = allProjects.getResult();
        filterProjects(userId, projects);
        allProjects.setResult(projects);

        return allProjects;
    }

    public QueryResult renameProject(int projectId, String newProjectAlias, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkAlias(newProjectAlias, "newProjectAlias");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = catalogDBAdaptor.getProjectOwner(projectId);

        Acl projectAcl = getProjectAcl(userId, projectId);
        if(projectAcl.isWrite()) {
            catalogDBAdaptor.updateUserLastActivity(ownerId);
            return catalogDBAdaptor.renameProjectAlias(projectId, newProjectAlias);
        } else {
            throw new CatalogManagerException("Permission denied. Can't rename project");
        }
    }

    /**
     * Modify some params from the specified project:
     *
     *   name
     *   description
     *   organization
     *   status
     *   attributes
     *
     * @param projectId Project identifier
     * @param parameters Parameters to change.
     * @param sessionId sessionId to check permissions
     * @return
     * @throws CatalogManagerException
     */
    public QueryResult modifyProject(int projectId, ObjectMap parameters, String sessionId)
            throws CatalogManagerException {
        checkObj(parameters, "Parameters");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = catalogDBAdaptor.getProjectOwner(projectId);
        if (!getProjectAcl(userId, projectId).isWrite()) {
            throw new CatalogManagerException("Permission denied. Can't modify project");
        }
        for (String s : parameters.keySet()) {
            if (!s.matches("name|description|organization|status|attributes")) {
                throw new CatalogManagerException("Parameter '" + s + "' can't be changed");
            }
        }
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return catalogDBAdaptor.modifyProject(projectId, parameters);
    }

    public QueryResult shareProject(int projectId, Acl acl, String sessionId) throws CatalogManagerException {
        checkObj(acl, "acl");
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl projectAcl = getProjectAcl(userId, projectId);
        if (!projectAcl.isWrite()) {
            throw new CatalogManagerException("Permission denied. Can't modify project");
        }

        return catalogDBAdaptor.setProjectAcl(projectId, acl);
    }

    /**
     * Study methods
     * ***************************
     */

    public QueryResult<Study> createStudy(int projectId, String name, String alias, String type, String description,
                                          String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(name, "name");
        checkParameter(alias, "alias");
        checkParameter(type, "type");
        checkParameter(description, "description");
        checkAlias(alias, "alias");
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = catalogDBAdaptor.getProjectOwner(projectId);

        Study study = new Study(name, alias, type, description, "");
        study.setCreatorId(userId);


        /* Check project permissions */
        if (!getProjectAcl(userId, projectId).isWrite()) { //User can't write/modify the project
            throw new CatalogManagerException("Permission denied. Can't write in project");
        }


        /* Add default ACL */
        if(!userId.equals(ownerId)) {
            //Add full permissions for the creator if he is not the owner
            study.getAcl().add(new Acl(userId, true, true, true, true));
        }
        //Copy generic permissions from the project.
        QueryResult<Acl> aclQueryResult = catalogDBAdaptor.getProjectAcl(projectId, USER_OTHERS_ID);
        if (!aclQueryResult.getResult().isEmpty()) {
            study.getAcl().add(aclQueryResult.getResult().get(0));
        } else {
            throw new CatalogManagerException("Project " + projectId + " must have generic ACL");
        }

        /* CreateStudy */
        QueryResult<Study> result = catalogDBAdaptor.createStudy(projectId, study);
        study = result.getResult().get(0);

        try {
            ioManager.createStudy(userId, Integer.toString(projectId), Integer.toString(study.getId()));
        } catch (CatalogIOManagerException e) {
            e.printStackTrace();
            catalogDBAdaptor.deleteStudy(study.getId());
        }
        createFolder(result.getResult().get(0).getId(), Paths.get("data"), true, sessionId);
        createFolder(result.getResult().get(0).getId(), Paths.get("analysis"), true, sessionId);

        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return result;
    }

    public QueryResult<Study> getStudy(int studyId, String sessionId)
            throws CatalogManagerException{
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl studyAcl = getStudyAcl(userId, studyId);
        if (studyAcl.isRead()) {
            QueryResult<Study> studyResult = catalogDBAdaptor.getStudy(studyId);
            if(!studyResult.getResult().isEmpty()) {
                filterFiles(userId, studyAcl, studyResult.getResult().get(0).getFiles());
            }
            return studyResult;
        } else {
            throw new CatalogManagerException("Permission denied. Can't read this study");
        }
    }

    public QueryResult<Study> getAllStudies(int projectId, String sessionId)
            throws CatalogManagerException, JsonProcessingException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        Acl projectAcl = getProjectAcl(userId, projectId);
        if(!projectAcl.isRead()) {
            throw new CatalogManagerException("Permission denied. Can't read project");
        }

        QueryResult<Study> allStudies = catalogDBAdaptor.getAllStudies(projectId);
        List<Study> studies = allStudies.getResult();
        filterStudies(userId, projectAcl, studies);
        allStudies.setResult(studies);

        return allStudies;


    }

    public QueryResult renameStudy(int studyId, String newStudAlias, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkAlias(newStudAlias, "newStudAlias");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = catalogDBAdaptor.getStudyOwner(studyId);

        if (!getStudyAcl(userId, studyId).isWrite()) {  //User can't write/modify the study
            throw new CatalogManagerException("Permission denied. Can't write in project");
        }

        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return catalogDBAdaptor.renameStudy(studyId, newStudAlias);

    }
    /**
     * Modify some params from the specified study:
     *
     *   name
     *   description
     *   organization
     *   status
     *
     *   attributes
     *   stats
     *
     * @param studyId Study identifier
     * @param parameters Parameters to change.
     * @param sessionId sessionId to check permissions
     * @return
     * @throws CatalogManagerException
     */
    public QueryResult modifyStudy(int studyId, ObjectMap parameters, String sessionId)
            throws CatalogManagerException {
        checkObj(parameters, "Parameters");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogManagerException("User " + userId + " can't modify the study " + studyId);
        }
        for (String s : parameters.keySet()) {
            if (!s.matches("name|type|description|status|attributes|stats")) {
                throw new CatalogManagerException("Parameter '" + s + "' can't be changed");
            }
        }

        String ownerId = catalogDBAdaptor.getStudyOwner(studyId);
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return catalogDBAdaptor.modifyStudy(studyId, parameters);
    }

//    public QueryResult shareStudy(int studyId, Acl acl, String sessionId) throws CatalogManagerException {
//        checkObj(acl, "acl");
//        checkParameter(sessionId, "sessionId");
//
//        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        Acl studyAcl = getStudyAcl(userId, studyId);
//        if (!studyAcl.isWrite()) {
//            throw new CatalogManagerException("Permission denied. Can't modify project");
//        }
//
//        return catalogDBAdaptor.setSudyAcl(studyId, acl);
//    }

    /**
     * File methods
     * ***************************
     */


    public QueryResult<File> createFile(int studyId, String format, String bioformat, String path, String description,
                                        boolean parents, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException, IOException, InterruptedException {
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        checkParameter(format, "format");
        checkParameter(bioformat, "bioformat");
        checkParameter(description, "description");
        checkParameter(sessionId, "sessionId");
        checkPath(path, "filePath");
        File file = new File(Paths.get(path).getFileName().toString(), File.FILE, format, bioformat, "void://", path, userId, description, File.UPLOADING, 0);

        Path parent = Paths.get(path).getParent();

//        if null acl de study
        int fileId = catalogDBAdaptor.getFileId(studyId, parent.toString());
        if (fileId < 0) {
            if (parents) {
                createFolder(studyId, parent, true, sessionId);
                fileId = catalogDBAdaptor.getFileId(studyId, parent.toString());
            } else {
                throw new CatalogManagerException("Directory not found " + parent.toString());
            }
        }

        Acl parentAcl = getFileAcl(userId, fileId);

        if (parentAcl.isWrite()) {
            file.setStatus(File.UPLOADING);
            file.setCreatorId(userId);
            file.setCreationDate(TimeUtils.getTime());
            return catalogDBAdaptor.createFileToStudy(studyId, file);
        } else {
            throw new CatalogManagerException("Permission denied, " + userId + " can not write in " + parent.toString());
        }
    }

    public QueryResult<File> uploadFile(int studyId, String format, String bioformat, String path, String description,
                                  boolean parents, InputStream fileIs, String sessionId) throws CatalogManagerException,
            CatalogIOManagerException, IOException, InterruptedException {
        QueryResult<File> fileResult = createFile(studyId, format, bioformat, path, description, parents, sessionId);
        fileResult = uploadFile(fileResult.getResult().get(0).getId(), fileIs, sessionId);
        return fileResult;
    }

    public QueryResult<File> uploadFile(int fileId, InputStream fileIs, String sessionId) throws CatalogManagerException,
            CatalogIOManagerException, IOException, InterruptedException {

        checkObj(fileIs, "InputStream");
        checkParameter(sessionId, "SessionId");

        String userId = catalogDBAdaptor.getFileOwner(fileId);
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);

        List<File> result = catalogDBAdaptor.getFile(fileId).getResult();
        if(result.isEmpty()) {
            throw new CatalogManagerException("FileId '" + fileId + "' for found");
        }
        File file = result.get(0);
        if(!file.getStatus().equals(File.UPLOADING)) {
            throw new CatalogManagerException("File '" + fileId + "' already uploaded.");
        }
        if(!file.getCreatorId().equals(userId)){
            throw new CatalogManagerException("UserId mismatch with file creator");
        }
        ioManager.createFile(userId, Integer.toString(projectId), Integer.toString(studyId), file.getPath(), fileIs);

        catalogDBAdaptor.modifyFile(fileId, new ObjectMap("status", File.UPLOADED));
        return catalogDBAdaptor.getFile(fileId);
    }

    public QueryResult createFolder(int studyId, Path folderPath, boolean parents, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkPath(folderPath, "folderPath");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = catalogDBAdaptor.getStudyOwner(studyId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);

        LinkedList<File> folders = new LinkedList<>();
        Path parent = folderPath.getParent();
        int parentId = -1;
        if(parent != null) {
            parentId = catalogDBAdaptor.getFileId(studyId, parent.toString());
        }
        if(!parents && parentId < 0 && parent != null){  //If !parents and parent does not exist in the DB (but should exist)
            throw new CatalogManagerException("Path '" + parent + "' does not exist");
        }
        while(parentId < 0 && parent != null){  //Add all the parents that should be created
            folders.addFirst(new File(parent.getFileName().toString(), File.FOLDER, "", "", "void://", parent.toString(), userId
                    , "", File.READY, 0));
            parent = parent.getParent();
            if(parent != null) {
                parentId = catalogDBAdaptor.getFileId(studyId, parent.toString());
            }
        }

        Acl fileAcl;
        if(parentId < 0) { //If it hasn't got parent, take the StudyAcl
            fileAcl = getStudyAcl(userId, studyId);
        } else {
            fileAcl = getFileAcl(userId, parentId);
        }

        if (!fileAcl.isWrite()) {
            throw new CatalogManagerException("Permission denied. Can't create files or folders in this study");
        }


        ioManager.createFolder(ownerId, Integer.toString(projectId), Integer.toString(studyId), folderPath.toString(), parents);
        File lastFolder = new File(folderPath.getFileName().toString(), File.FOLDER, "", "", "void://", folderPath.toString(), userId
                , "", File.READY, 0);

        QueryResult<File> result;
        try {
            assert folders.size() == 0 && !parents || parents;
            for (File folder : folders) {
                catalogDBAdaptor.createFileToStudy(studyId, folder);
            }
            result = catalogDBAdaptor.createFileToStudy(studyId, lastFolder);
        } catch (CatalogManagerException e) {
            ioManager.deleteFile(ownerId, Integer.toString(projectId), Integer.toString(studyId), folderPath.toString());
            throw e;
        }
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return result;
    }


    public QueryResult deleteDataFromStudy(int fileId, String sessionId)
            throws CatalogManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = catalogDBAdaptor.getProjectOwner(projectId);

        if (!getFileAcl(userId, fileId).isDelete()) {
            throw new CatalogManagerException("Permission denied. User can't delete this file");
        }
        QueryResult<File> fileResult = catalogDBAdaptor.getFile(fileId);
        if(fileResult.getResult().isEmpty()){
            return new QueryResult("Delete file", 0, 0, 0, "File not found", null, null);
        }
        File file = fileResult.getResult().get(0);
        try {
            ioManager.deleteFile(ownerId, Integer.toString(projectId), Integer.toString(studyId), file.getPath());
        } catch (CatalogIOManagerException e) {
            throw new CatalogManagerException(e);
        }
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return catalogDBAdaptor.deleteFile(fileId);
    }



    /**
     * Modify some params from the specified file:
     *
     *   name
     *   type
     *   format
     *   bioformat
     *   description
     *   status
     *
     *   attributes
     *   stats
     *
     * @param fileId File identifier
     * @param parameters Parameters to change.
     * @param sessionId sessionId to check permissions
     * @return
     * @throws CatalogManagerException
     */
    public QueryResult modifyFile(int fileId, ObjectMap parameters, String sessionId)
            throws CatalogManagerException {
        checkObj(parameters, "Parameters");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!getFileAcl(userId, fileId).isWrite()) {
            throw new CatalogManagerException("User " + userId + " can't modify the file " + fileId);
        }
        for (String s : parameters.keySet()) {
            if (!s.matches("name|type|format|bioformat|description|status|attributes|stats")) {
                throw new CatalogManagerException("Parameter '" + s + "' can't be changed");
            }
        }
        String ownerId = catalogDBAdaptor.getFileOwner(fileId);
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return catalogDBAdaptor.modifyFile(fileId, parameters);
    }

    public QueryResult<File> getFile(int fileId, String sessionId)
            throws CatalogIOManagerException, IOException, CatalogManagerException {
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!getFileAcl(userId, fileId).isRead()) {
            throw new CatalogManagerException("Permission denied. User can't read file");
        }

        return catalogDBAdaptor.getFile(fileId);
    }

    public QueryResult<File> getAllFiles(int studyId, String sessionId) throws CatalogManagerException {
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl studyAcl = getStudyAcl(userId, studyId);
        if (!studyAcl.isRead()) {
            throw new CatalogManagerException("Permission denied. User can't read file");
        }
        QueryResult<File> allFilesResult = catalogDBAdaptor.getAllFiles(studyId);
        List<File> files = allFilesResult.getResult();
        filterFiles(userId, studyAcl, files);
        allFilesResult.setResult(files);
        return allFilesResult;
    }

    public DataInputStream downloadFile(int fileId, String sessionId)
            throws CatalogIOManagerException, IOException, CatalogManagerException {
        return downloadFile(fileId, -1, -1, sessionId);
    }
    private DataInputStream downloadFile(int fileId, int start, int limit, String sessionId)    //TODO: start & limit does not work
            throws CatalogIOManagerException, IOException, CatalogManagerException {
        checkParameter(sessionId, "sessionId");


        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!getFileAcl(userId, fileId).isRead()) {
            throw new CatalogManagerException("Permission denied. User can't download file");
        }
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
        QueryResult<File> fileResult = catalogDBAdaptor.getFile(fileId);
        if(fileResult.getResult().isEmpty()){
            throw new CatalogManagerException("File not found");
        }
        File file = fileResult.getResult().get(0);

        return ioManager.getFileObject(userId,
                Integer.toString(projectId),
                Integer.toString(studyId),
                file.getPath(), start, limit);
    }


    public QueryResult shareFile(int fileId, Acl acl, String sessionId) throws CatalogManagerException {
        checkObj(acl, "acl");
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl fileAcl = getFileAcl(userId, fileId);
        if (!fileAcl.isWrite()) {
            throw new CatalogManagerException("Permission denied. Can't modify file");
        }

        return catalogDBAdaptor.setFileAcl(fileId, acl);
    }

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

//    //TODO
//    public void shareObject(int fileId, String toAccountId, boolean read,
//                            boolean write, boolean execute, boolean delete, String sessionId) throws CatalogManagerException {
//        checkParameters(userId, "userId", bucketId, "bucketId", objectId.toString(), "objectId", toAccountId,
//                "toAccountId", sessionId, "sessionId");
//
//        Acl acl = new Acl(toAccountId, read, write, execute, delete);
//        catalogDBAdaptor.shareObject(fileId, acl, sessionId);
//    }

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


//    public String fetchVariationData(Path objectPath, String regionStr, Map<String, List<String>> params) throws Exception {
//        VcfManager vcfManager = new VcfManager();
//        return vcfManager.getByRegion(objectPath, regionStr, params);
//    }


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
    /**
     * Analysis methods
     * ***************************
     */

    public QueryResult<Analysis> createAnalysis(int studyId, Analysis analysis, String sessionId) throws CatalogManagerException {
        checkObj(analysis, "analysis");
        checkParameter(sessionId, "sessionId");
        checkAlias(analysis.getAlias(), "analysis.getAlias()");
        checkParameter(analysis.getName(), "analysis.getName()");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogManagerException("Permission denied. Can't write in this study");
        }

        if(analysis.getCreatorId() == null || analysis.getCreatorId().isEmpty()){
            analysis.setCreatorId(userId);
        }
        if(analysis.getCreationDate() == null || analysis.getCreationDate().isEmpty()){
            analysis.setCreationDate(TimeUtils.getTime());
        }

        String ownerId = catalogDBAdaptor.getStudyOwner(studyId);
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return catalogDBAdaptor.createAnalysis(studyId, analysis);
    }

    public QueryResult<Analysis> getAnalysis(int analysisId, String sessionId) throws CatalogManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);

        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogManagerException("Permission denied. User can't read from this study"); //TODO: Should Analysis have ACL?
        }

        return catalogDBAdaptor.getAnalysis(analysisId);

    }

    public QueryResult<Analysis> getAllAnalysis(int studyId, String sessionId) throws CatalogManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogManagerException("Permission denied. Can't read from this study"); //TODO: Should Analysis have ACL?
        }

        return catalogDBAdaptor.getAllAnalysis(studyId);
    }

    /**
     * Modify some params from the specified file:
     *
     *   name
     *   date
     *   description
     *
     *   attributes
     *
     * @param analysisId Analysis identifier
     * @param parameters Parameters to change.
     * @param sessionId sessionId to check permissions
     * @return
     * @throws CatalogManagerException
     */
    public QueryResult modifyAnalysis(int analysisId, ObjectMap parameters, String sessionId) throws CatalogManagerException {
        checkParameter(sessionId, "sessionId");
        checkObj(parameters, "Parameters");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);

        for (String s : parameters.keySet()) {
            if (!s.matches("name|date|description|attributes")) {
                throw new CatalogManagerException("Parameter '" + s + "' can't be changed");
            }
        }

        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogManagerException("Permission denied. Can't modify this analysis"); //TODO: Should Analysis have ACL?
        }

        String ownerId = catalogDBAdaptor.getAnalysisOwner(analysisId);
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return catalogDBAdaptor.modifyAnalysis(analysisId, parameters);
    }



    /**
     * Job methods
     * ***************************
     */

    public QueryResult<Job> createJob(int analysisId, Job job, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkObj(job, "Job");
        checkParameter(sessionId, "sessionId");
        checkParameter(job.getName(), "job.getName()");
        checkPath(job.getOutdir(), "job.getOutdir()");
//        checkParameter(job.getToolName(), "job.getToolName()");
        //TODO: Add check for required parameters

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);

        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogManagerException("Permission denied. Can't create job");
        }

        if (job.getUserId() == null || job.getUserId().isEmpty()) {
            job.setUserId(userId);
        }

//        if(!job.getOutdir().endsWith("/")){
//            job.setOutdir(job.getOutdir()+"/");
//        }
        int fileId = catalogDBAdaptor.getFileId(studyId, job.getOutdir());

        if(fileId < 0){
            createFolder(studyId, Paths.get(job.getOutdir()), true, sessionId);
        }

        return catalogDBAdaptor.createJob(analysisId, job);
    }
//
//    public String checkJobStatus(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException {
//        return catalogDBAdaptor.getJobStatus(userId, jobId, sessionId);
//    }

    public QueryResult<ObjectMap> incJobVisites(int jobId, String sessionId) throws CatalogManagerException{
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int analysisId = catalogDBAdaptor.getAnalysisIdByJobId(jobId);
        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogManagerException("Permission denied. Can't read job");
        }
        return catalogDBAdaptor.incJobVisits(jobId);
    }

    public QueryResult deleteJob(int jobId, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int analysisId = catalogDBAdaptor.getAnalysisIdByJobId(jobId);
        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
        if (!getStudyAcl(userId, studyId).isDelete()) {
            throw new CatalogManagerException("Permission denied. Can't delete job");
        }

        return catalogDBAdaptor.deleteJob(jobId);
    }


    public QueryResult<Job> getJob(int jobId, String sessionId) throws IOException, CatalogIOManagerException, CatalogManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int analysisId = catalogDBAdaptor.getAnalysisIdByJobId(jobId);
        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogManagerException("Permission denied. Can't read job");
        }

        return catalogDBAdaptor.getJob(jobId);
    }


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
        if (email == null || !emailPattern.matcher(email).matches()) {
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

    private void checkSessionId(String userId, String sessionId) throws CatalogManagerException {
        String userIdBySessionId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!userIdBySessionId.equals(userId)) {
            throw new CatalogManagerException("Invalid sessionId for user: " + userId);
        }
    }

    private void checkPath(String path, String name) throws CatalogManagerException {
        checkParameter(path, name);
        checkPath(Paths.get(path), name);
    }

    private void checkPath(Path path, String name) throws CatalogManagerException {
        checkObj(path, name);
        if (path.isAbsolute()) {
            throw new CatalogManagerException("Error in path: Path '"+name+"' can't be absolute");
        }
    }

    private void checkAlias(String alias, String name) throws CatalogManagerException {
        if(alias == null || alias.isEmpty() || !alias.matches("^[_A-Za-z0-9-\\+]+$") ){
            throw new CatalogManagerException("Error in alias: Invalid alias for '"+name+"'.");
        }
    }
    /*
     *  Permission methods. Internal use only.
     *  Builds the specific ACL for each pair sessionId,object
     *  ****************
     */

    /**
     * Removes from the list the projects that the user can not read.
     * From the remaining projects, filters the studies and files.
     *
     * @param userId    UserId
     * @param projects  Projects list
     * @throws CatalogManagerException
     */
    private void filterProjects(String userId, List<Project> projects) throws CatalogManagerException {
        Iterator<Project> projectIt = projects.iterator();
        while (projectIt.hasNext()) {
            Project p = projectIt.next();
            Acl projectAcl = getProjectAcl(userId, p.getId());
            if (!projectAcl.isRead()) {
                projectIt.remove();
            } else {
                List<Study> studies = p.getStudies();
                filterStudies(userId, projectAcl, studies);
            }
        }
    }

    /**
     * Removes from the list the studies that the user can not read.
     * From the remaining studies, filters the files.
     *
     * @param userId        UserId
     * @param projectAcl    Project ACL
     * @param studies       Studies list
     * @throws CatalogManagerException
     */
    private void filterStudies(String userId, Acl projectAcl, List<Study> studies) throws CatalogManagerException {
        Iterator<Study> studyIt = studies.iterator();
        while(studyIt.hasNext()){
            Study s = studyIt.next();
            Acl studyAcl = getStudyAcl(userId, s.getId(), projectAcl);
            if(!studyAcl.isRead()) {
                studyIt.remove();
            } else {
                List<File> files = s.getFiles();
                filterFiles(userId, studyAcl, files);
            }
        }
    }

    /**
     * Removes from the list the files that the user can not read.
     *
     * @param userId    UserId
     * @param studyAcl  Study ACL
     * @param files     Files list
     * @throws CatalogManagerException
     */
    private void filterFiles(String userId, Acl studyAcl, List<File> files) throws CatalogManagerException {
        Iterator<File> fileIt = files.iterator();
        while(fileIt.hasNext()){
            File f = fileIt.next();
            Acl fileAcl = getFileAcl(userId, f.getId(), studyAcl);
            if(!fileAcl.isRead()){
                fileIt.remove();
            }
        }
    }

    private Acl mergeAcl(String userId, Acl acl1, Acl acl2) {
        return new Acl(
                userId,
                acl1.isRead() && acl2.isRead(),
                acl1.isWrite() && acl2.isWrite(),
                acl1.isExecute() && acl2.isExecute(),
                acl1.isDelete() && acl2.isDelete()
        );
    }

    private Acl getProjectAcl(String userId, int projectId) throws CatalogManagerException {
        Acl projectAcl;
        boolean sameOwner = catalogDBAdaptor.getProjectOwner(projectId).equals(userId);

        if(sameOwner){
            projectAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = catalogDBAdaptor.getProjectAcl(projectId, userId);
            if (!result.getResult().isEmpty()) {
                projectAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = catalogDBAdaptor.getProjectAcl(projectId, USER_OTHERS_ID);
                if (!resultAll.getResult().isEmpty()) {
                    projectAcl = resultAll.getResult().get(0);
                } else {
                    projectAcl = new Acl(userId, false, false, false, false);
                }
            }
        }
        return projectAcl;
    }

    private Acl getStudyAcl(String userId, int studyId) throws CatalogManagerException {
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
        return getStudyAcl(userId, studyId, getProjectAcl(userId, projectId));
    }

    private Acl getStudyAcl(String userId, int studyId, Acl projectAcl) throws CatalogManagerException {
        Acl studyAcl;
        boolean sameOwner = catalogDBAdaptor.getStudyOwner(studyId).equals(userId);

        if(sameOwner){
            studyAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = catalogDBAdaptor.getStudyAcl(studyId, userId);
            if (!result.getResult().isEmpty()) {
                studyAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = catalogDBAdaptor.getStudyAcl(studyId, USER_OTHERS_ID);
                if (!resultAll.getResult().isEmpty()) {
                    studyAcl = resultAll.getResult().get(0);
                } else {
                    //studyAcl = new Acl(userId, false, false, false, false);
                    studyAcl = projectAcl;
                }
            }
        }
        return mergeAcl(userId, projectAcl, studyAcl);
    }

    private Acl getFileAcl(String userId, int fileId) throws CatalogManagerException {
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        return getStudyAcl(userId, studyId);
    }

    //TODO: Check folder ACLs
    private Acl getFileAcl(String userId, int fileId, Acl studyAcl) throws CatalogManagerException {
        Acl fileAcl;
        boolean sameOwner = catalogDBAdaptor.getFileOwner(fileId).equals(userId);

        if(sameOwner){
            fileAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = catalogDBAdaptor.getFileAcl(fileId, userId);
            if (!result.getResult().isEmpty()) {
                fileAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = catalogDBAdaptor.getFileAcl(fileId, USER_OTHERS_ID);
                if (!resultAll.getResult().isEmpty()) {
                    fileAcl = resultAll.getResult().get(0);
                } else {
                    //fileAcl = new Acl(userId, false, false, false, false);
                    fileAcl = studyAcl;
                }
            }
        }
        return mergeAcl(userId, fileAcl, studyAcl);
    }

//    private Acl getFileAcl(String userId, int studyId, Path filePath, Acl studyAcl) throws CatalogManagerException {
//        int fileId = catalogDBAdaptor.getFileId(studyId, filePath.toString());
//        QueryResult<Acl> fileAcl = catalogDBAdaptor.getFileAcl(fileId, userId);
//        if(fileAcl.getResult().isEmpty()) {
//            return getFileAcl(userId, studyId, filePath.getParent(), studyAcl);
//        }
//
//    }

}
