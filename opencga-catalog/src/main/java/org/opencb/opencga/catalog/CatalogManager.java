package org.opencb.opencga.catalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.MongoCredential;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.db.CatalogDBAdaptor;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBAdaptor;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;

import org.opencb.opencga.lib.common.MailUtils;
import org.opencb.opencga.lib.common.StringUtils;
import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;

public class CatalogManager {

    private CatalogDBAdaptor catalogDBAdaptor;
    private CatalogIOManager ioManager;
    private CatalogIOManagerFactory catalogIOManagerFactory;

//    private PosixCatalogIOManager ioManager;



    private Properties properties;

    protected static Logger logger = LoggerFactory.getLogger(CatalogManager.class);
    protected static final String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
    protected static final Pattern emailPattern = Pattern.compile(EMAIL_PATTERN);


    /*public CatalogManager() throws IOException, CatalogIOManagerException, CatalogManagerException {
        this(System.getenv("OPENCGA_HOME"));
    }*/

    public CatalogManager(CatalogDBAdaptor catalogDBAdaptor, Properties catalogProperties) throws IOException, CatalogIOManagerException {
        this.catalogDBAdaptor = catalogDBAdaptor;
        this.properties = catalogProperties;

        configureIOManager(properties);
    }

    @Deprecated
    public CatalogManager(String rootdir)
            throws IOException, CatalogIOManagerException, CatalogManagerException {
//        properties = Config.getAccountProperties();

        Path path = Paths.get(rootdir, "conf", "catalog.properties");
        properties = new Properties();
        try {
            properties.load(Files.newInputStream(path));
        } catch (IOException e) {
            logger.error("Failed to load account.properties: " + e.getMessage());
            throw e;
        }

        configureDBAdaptor(properties);
        configureIOManager(properties);
    }

    public CatalogManager(Properties properties)
            throws IOException, CatalogIOManagerException, CatalogManagerException {
        this.properties = properties;

        configureDBAdaptor(properties);
        configureIOManager(properties);
    }

    public CatalogIOManagerFactory getCatalogIOManagerFactory() {
        return catalogIOManagerFactory;
    }

    private void configureIOManager(Properties properties)
            throws IOException, CatalogIOManagerException {
        catalogIOManagerFactory = new CatalogIOManagerFactory(properties);
//        ioManager = this.catalogIOManagerFactory.get(properties.getProperty("CATALOG.MODE", DEFAULT_CATALOG_SCHEME));
        String scheme = URI.create(properties.getProperty("CATALOG.MAIN.ROOTDIR")).getScheme();
        if(scheme == null) {
            scheme = "file";
        }
        ioManager = this.catalogIOManagerFactory.get(scheme);
    }

    private void configureDBAdaptor(Properties properties)
            throws CatalogManagerException {

        MongoCredential mongoCredential = MongoCredential.createMongoCRCredential(
                properties.getProperty("CATALOG.USER", ""),
                properties.getProperty("CATALOG.DATABASE", ""),
                properties.getProperty("CATALOG.PASSWORD", "").toCharArray());

        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                properties.getProperty("CATALOG.HOST", ""),
                Integer.parseInt(properties.getProperty("CATALOG.PORT", "0")));

        catalogDBAdaptor = new CatalogMongoDBAdaptor(dataStoreServerAddress, mongoCredential);

    }

    /**
     * Getter path methods
     * ***************************
     */

    public URI getUserUri(String userId) throws CatalogIOManagerException {
        return ioManager.getUserUri(userId);
    }

    public URI getProjectUri(String userId, String projectId) throws CatalogIOManagerException {
        return ioManager.getProjectUri(userId, projectId);
    }

    @Deprecated
    public URI getStudyUri(String scheme, String userId, String projectId, String studyId)
            throws CatalogIOManagerException, IOException {
        return catalogIOManagerFactory.get(scheme).getStudyUri(userId, projectId, studyId);
    }

    @Deprecated
    public URI getStudyUri(String userId, String projectId, String studyId)
            throws CatalogIOManagerException, IOException {
        return ioManager.getStudyUri(userId, projectId, studyId);
    }
    public URI getStudyUri(int studyId, String sessionId)
            throws CatalogIOManagerException, IOException, CatalogManagerException {
        return getStudy(studyId, sessionId, new QueryOptions("include", Arrays.asList("id", "uri"))).getResult().get(0).getUri();
    }

    public URI getFileUri(String userId, String projectId, String studyId, String relativeFilePath)
            throws CatalogIOManagerException {
        return ioManager.getFileUri(userId, projectId, studyId, relativeFilePath);
    }

    public URI getFileUri(URI studyUri, String relativeFilePath)
            throws CatalogIOManagerException, IOException {
        return catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, relativeFilePath);
    }

    public URI getFileUri(File file) throws CatalogManagerException, CatalogIOManagerException {
        int studyId = catalogDBAdaptor.getStudyIdByFileId(file.getId());
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
        String userId = catalogDBAdaptor.getProjectOwnerId(projectId);
        return getFileUri(userId, Integer.toString(projectId), Integer.toString(studyId), file.getPath());
    }
    /* jmmut uncomment
//    public Uri getJobFolderUri(String userId, String projectId, Uri JobId) {
//        return ioManager.getJobFolderUri(userId, projectId, JobId);
//    }

//    public URI getTmpUri() {
//        return ioManager.getTmpUri();
//    }

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
     * Id methods
     * <user>@project:study:directories:filePath
     * ***************************
     */

    public int getProjectId(String id) throws CatalogManagerException {
        try{
            return Integer.parseInt(id);
        } catch(NumberFormatException ignore){}

        String[] split = id.split("@");
        if(split.length != 2){
            return -1;
        }
        return catalogDBAdaptor.getProjectId(split[0], split[1]);
    }

    public int getStudyId(String id) throws CatalogManagerException {
        try{
            return Integer.parseInt(id);
        } catch(NumberFormatException ignore){}

        String[] split = id.split("@");
        if(split.length != 2){
            return -1;
        }
        String[] projectStudy = split[1].replace(':', '/').split("/", 2);
        if(projectStudy.length != 2){
            return -2;
        }
        return catalogDBAdaptor.getStudyId(split[0], projectStudy[0], projectStudy[1]);
    }

    public int getFileId(String id) throws CatalogManagerException {
        try{
            return Integer.parseInt(id);
        } catch(NumberFormatException ignore){}

        String[] split = id.split("@", 2);
        if(split.length != 2){
            return -1;
        }
        String[] projectStudyPath = split[1].replace(':', '/').split("/", 3);
        if(projectStudyPath.length <= 2){
            return -2;
        }
        return catalogDBAdaptor.getFileId(split[0], projectStudyPath[0], projectStudyPath[1], projectStudyPath[2]);
    }

    public int getToolId(String id) throws CatalogManagerException {
        try{
            return Integer.parseInt(id);
        } catch(NumberFormatException ignore){}

        String[] split = id.split("@");
        if(split.length != 2){
            return -1;
        }
        return catalogDBAdaptor.getToolId(split[0], split[1]);
    }

    /**
     * User methods
     * ***************************
     */

    public QueryResult<User> createUser(String id, String name, String email, String password, String organization)
            throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(id, "id");
        checkParameter(password, "password");
        checkParameter(name, "name");
        checkParameter(email, "email");
        organization = organization != null ? organization : "";

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

        String newPassword = StringUtils.randomString(6);
        String newCryptPass;
        try {
            newCryptPass = StringUtils.sha1(newPassword);
        } catch (NoSuchAlgorithmException e) {
            throw new CatalogManagerException("could not encode password");
        }

        QueryResult qr = catalogDBAdaptor.resetPassword(userId, email, newCryptPass);

        String mailUser = properties.getProperty("CATALOG.MAIL.USER");
        String mailPassword = properties.getProperty("CATALOG.MAIL.PASSWORD");
        String mailHost = properties.getProperty("CATALOG.MAIL.HOST");
        String mailPort = properties.getProperty("CATALOG.MAIL.PORT");

        MailUtils.sendResetPasswordMail(email, newPassword, mailUser, mailPassword, mailHost, mailPort);

        return qr;
    }


    public QueryResult<User> getUser(String userId, String lastActivity, String sessionId) throws CatalogManagerException {
        return getUser(userId, lastActivity, sessionId, new QueryOptions());
    }
    public QueryResult<User> getUser(String userId, String lastActivity, String sessionId, QueryOptions options)
            throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        if(options == null || !options.containsKey("include") || !options.containsKey("exclude")) {
            options.put("exclude", Arrays.asList("password", "sessions"));
        }
//        if(options.containsKey("exclude")) {
//            options.getListAs("exclude", String.class).add("sessions");
//        }
        //FIXME: Should other users get access to other user information? (If so, then filter projects)
        //FIXME: Should setPassword(null)??
        QueryResult<User> user = catalogDBAdaptor.getUser(userId, options, lastActivity);
        return user;
    }

    public String getUserIdBySessionId(String sessionId){
        return catalogDBAdaptor.getUserIdBySessionId(sessionId);
    }

    /**
     * Modify some params from the user profile:
     * <p/>
     * name
     * email
     * organization
     * <p/>
     * attributes
     * configs
     *
     * @param userId     userId
     * @param parameters Parameters to change.
     * @param sessionId  sessionId must match with the userId
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
        if (parameters.containsKey("email")) {
            checkEmail(parameters.getString("email"));
        }
        catalogDBAdaptor.updateUserLastActivity(userId);
        return catalogDBAdaptor.modifyUser(userId, parameters);
    }

    public void deleteUser(String userId, String sessionId) throws CatalogManagerException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        String userIdBySessionId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if(userIdBySessionId.equals(userId) || getUserRole(userIdBySessionId) == User.ROLE_ADMIN ) {
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

    public QueryResult<Project> createProject(String ownerId, String name, String alias, String description,
                                              String organization, String sessionId)
            throws CatalogManagerException,
            CatalogIOManagerException, JsonProcessingException {
        checkParameter(ownerId, "ownerId");
        checkParameter(name, "name");
        checkAlias(alias, "alias");
        checkParameter(sessionId, "sessionId");

        checkSessionId(ownerId, sessionId);    //Only the user can create a project

        description = description != null ? description : "";
        organization = organization != null ? organization : "";

        Project project = new Project(name, alias, description, "", organization);

        /* Add default ACL */
        //Add generic permissions to the project.
        project.getAcl().add(new Acl(Acl.USER_OTHERS_ID, false, false, false, false));

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
            QueryResult<Project> projectResult = catalogDBAdaptor.getProject(projectId, null);
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
        String ownerId = catalogDBAdaptor.getProjectOwnerId(projectId);

        Acl projectAcl = getProjectAcl(userId, projectId);
        if (projectAcl.isWrite()) {
            catalogDBAdaptor.updateUserLastActivity(ownerId);
            return catalogDBAdaptor.renameProjectAlias(projectId, newProjectAlias);
        } else {
            throw new CatalogManagerException("Permission denied. Can't rename project");
        }
    }

    /**
     * Modify some params from the specified project:
     * <p/>
     * name
     * description
     * organization
     * status
     * attributes
     *
     * @param projectId  Project identifier
     * @param parameters Parameters to change.
     * @param sessionId  sessionId to check permissions
     * @return
     * @throws CatalogManagerException
     */
    public QueryResult modifyProject(int projectId, ObjectMap parameters, String sessionId)
            throws CatalogManagerException {
        checkObj(parameters, "Parameters");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = catalogDBAdaptor.getProjectOwnerId(projectId);
        if (!getProjectAcl(userId, projectId).isWrite()) {
            throw new CatalogManagerException("User '" + userId + "' can't modify the project " + projectId);
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
    public QueryResult<Study> createStudy(int projectId, String name, String alias, Study.StudyType type, String description,
                                          String sessionId)
            throws CatalogManagerException, CatalogIOManagerException, IOException {
        return createStudy(projectId, name, alias, type, description, CatalogIOManagerFactory.DEFAULT_CATALOG_SCHEME, sessionId);
    }

    private QueryResult<Study> createStudy(int projectId, String name, String alias, Study.StudyType type, String description,
                                          String uriScheme, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException, IOException {
        checkParameter(name, "name");
        checkParameter(alias, "alias");
        checkObj(type, "type");
        checkParameter(description, "description");
        checkAlias(alias, "alias");
        checkParameter(uriScheme, "uriScheme");   // TODO
        checkParameter(sessionId, "sessionId");

        CatalogIOManager catalogIOManager = catalogIOManagerFactory.get(uriScheme);

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String projectOwnerId = catalogDBAdaptor.getProjectOwnerId(projectId);

//        URI projectUri = catalogIOManager.getProjectUri(projectOwnerId, Integer.toString(projectId));
        Study study = new Study(name, alias, type, description, "", null);

        study.getFiles().add(new File(".", File.TYPE_FOLDER, "", "", "", userId, "", "", 0));//TODO: Take scheme from study
        study.setCreatorId(userId);


        /* Check project permissions */
        if (!getProjectAcl(userId, projectId).isWrite()) { //User can't write/modify the project
            throw new CatalogManagerException("Permission denied. Can't write in project");
        }


        /* Add default ACL */
        if (!userId.equals(projectOwnerId)) {
            //Add full permissions for the creator if he is not the owner
            study.getAcl().add(new Acl(userId, true, true, true, true));
        }
        //Copy generic permissions from the project.
        QueryResult<Acl> aclQueryResult = catalogDBAdaptor.getProjectAcl(projectId, Acl.USER_OTHERS_ID);
        if (!aclQueryResult.getResult().isEmpty()) {
            //study.getAcl().add(aclQueryResult.getResult().get(0));
        } else {
            throw new CatalogManagerException("Project " + projectId + " must have generic ACL");
        }

        /* CreateStudy */
        QueryResult<Study> result = catalogDBAdaptor.createStudy(projectId, study);
        study = result.getResult().get(0);

        URI studyUri;
        try {
            studyUri = catalogIOManager.createStudy(projectOwnerId, Integer.toString(projectId), Integer.toString(study.getId()));
        } catch (CatalogIOManagerException e) {
            e.printStackTrace();
            catalogDBAdaptor.deleteStudy(study.getId());
            throw e;
        }

        catalogDBAdaptor.modifyStudy(study.getId(), new ObjectMap("uri", studyUri));

        createFolder(result.getResult().get(0).getId(), Paths.get("data"), true, sessionId);
        createFolder(result.getResult().get(0).getId(), Paths.get("analysis"), true, sessionId);

        catalogDBAdaptor.updateUserLastActivity(projectOwnerId);
        return result;
    }

    public QueryResult<Study> getStudy(int studyId, String sessionId)
            throws CatalogManagerException {
        return getStudy(studyId, sessionId, null);
    }
    public QueryResult<Study> getStudy(int studyId, String sessionId, QueryOptions options)
            throws CatalogManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl studyAcl = getStudyAcl(userId, studyId);
        if (studyAcl.isRead()) {
            QueryResult<Study> studyResult = catalogDBAdaptor.getStudy(studyId, options);
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

    public QueryResult renameStudy(int studyId, String newStudyAlias, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkAlias(newStudyAlias, "newStudyAlias");
        checkParameter(sessionId, "sessionId");
        String sessionUserId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String studyOwnerId = catalogDBAdaptor.getStudyOwnerId(studyId);

        if (!getStudyAcl(sessionUserId, studyId).isWrite()) {  //User can't write/modify the study
            throw new CatalogManagerException("Permission denied. Can't write in project");
        }

        // Both users must bu updated
        catalogDBAdaptor.updateUserLastActivity(sessionUserId);
        catalogDBAdaptor.updateUserLastActivity(studyOwnerId);
        //TODO get all shared users to updateUserLastActivity

        return catalogDBAdaptor.renameStudy(studyId, newStudyAlias);

    }
    /**
     * Modify some params from the specified study:
     * <p/>
     * name
     * description
     * organization
     * status
     * <p/>
     * attributes
     * stats
     *
     * @param studyId    Study identifier
     * @param parameters Parameters to change.
     * @param sessionId  sessionId to check permissions
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

        String ownerId = catalogDBAdaptor.getStudyOwnerId(studyId);
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return catalogDBAdaptor.modifyStudy(studyId, parameters);
    }

    public QueryResult shareStudy(int studyId, Acl acl, String sessionId) throws CatalogManagerException {
        checkObj(acl, "acl");
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl studyAcl = getStudyAcl(userId, studyId);
        if (!studyAcl.isWrite()) {
            throw new CatalogManagerException("Permission denied. Can't modify project");
        }

        return catalogDBAdaptor.setStudyAcl(studyId, acl);
    }

    /**
     * File methods
     * ***************************
     */

    public String getFileOwner(int fileId) throws CatalogManagerException {
        return catalogDBAdaptor.getFileOwnerId(fileId);
    }

    public int getStudyIdByFileId(int fileId) throws CatalogManagerException {
        return catalogDBAdaptor.getStudyIdByFileId(fileId);
    }

//    public int getStudyIdByAnalysisId(int analysisId) throws CatalogManagerException {
//        return catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
//    }

    public QueryResult<File> createFile(int studyId, String format, String bioformat, String path, String description,
                                        boolean parents, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException, IOException, InterruptedException {
        return createFile(studyId, format, bioformat, path, description, parents, -1, sessionId);
    }

    public QueryResult<File> createFile(int studyId, String format, String bioformat, String path, String description,
                                        boolean parents, int jobId, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException, IOException, InterruptedException {
        return createFile(studyId, File.TYPE_FILE, format, bioformat, path, description, parents, jobId, sessionId, null);
    }

    public QueryResult<File> createFile(int studyId, String type, String format, String bioformat, String path, String description,
                                        boolean parents, int jobId, String sessionId, Map<String, Object> attributes)
            throws CatalogManagerException, CatalogIOManagerException {
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        checkParameter(format, "format");
        checkParameter(description, "description");
        checkParameter(sessionId, "sessionId");
        checkPath(path, "filePath");
        checkObj(bioformat, "bioformat");   //Bioformat can be empty

        Study study = catalogDBAdaptor.getStudy(studyId, null).getResult().get(0); // if no studies are found, an exception is raised

        File file = new File(Paths.get(path).getFileName().toString(), type, format, bioformat,
                path, userId, description, File.UPLOADING, 0);
        file.setJobId(jobId);

        Path parent = Paths.get(path).getParent();

//        if null acl de study
        int fileId = -1;
        if(parent != null) {
            fileId = catalogDBAdaptor.getFileId(studyId, parent.toString() + "/");
        }
        if (fileId < 0 && parent != null) {
            if (parents) {
                createFolder(studyId, parent, true, sessionId);
                fileId = catalogDBAdaptor.getFileId(studyId, parent.toString() + "/");
            } else {
                throw new CatalogManagerException("Directory not found " + parent.toString());
            }
        }

        Acl parentAcl;
        if(fileId < 0){
            parentAcl = getStudyAcl(userId, studyId);
        } else {
            parentAcl = getFileAcl(userId, fileId);
        }

        if (parentAcl.isWrite()) {
            file.setStatus(File.UPLOADING);
            file.setOwnerId(userId);
            file.setCreationDate(TimeUtils.getTime());
            if(attributes != null) {
                file.getAttributes().putAll(attributes);
            }
            return catalogDBAdaptor.createFileToStudy(studyId, file);
        } else {
            throw new CatalogManagerException("Permission denied, " + userId + " can not write in " +
                    (parent!=null? "directory " + parent.toString() : "study " + studyId));
        }
    }

    public QueryResult<File> uploadFile(int studyId, String format, String bioformat, String path, String description,
                                  boolean parents, InputStream fileIs, String sessionId)
            throws CatalogIOManagerException, InterruptedException, IOException, CatalogManagerException {
        QueryResult<File> fileResult = createFile(studyId, format, bioformat, path, description, parents, sessionId);
        int fileId = fileResult.getResult().get(0).getId();
        try {
            fileResult = uploadFile(fileId, fileIs, sessionId);
        } catch (CatalogIOManagerException | InterruptedException | CatalogManagerException | IOException e) {
            deleteFile(fileId, sessionId);
            e.printStackTrace();
        }
        return fileResult;
    }

    public QueryResult<File> uploadFile(int fileId, InputStream fileIs, String sessionId) throws CatalogManagerException,
            CatalogIOManagerException, IOException, InterruptedException {

        checkObj(fileIs, "InputStream");
        checkParameter(sessionId, "SessionId");

        String userId = catalogDBAdaptor.getFileOwnerId(fileId);
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
        if(!file.getOwnerId().equals(userId)){
            throw new CatalogManagerException("UserId mismatch with file creator");
        }
        ioManager.createFile(userId, Integer.toString(projectId), Integer.toString(studyId), file.getPath(), fileIs);
        Study study = catalogDBAdaptor.getStudy(studyId, null).getResult().get(0);

        ObjectMap modifyParameters = new ObjectMap("status", File.UPLOADED);
        modifyParameters.put("uriScheme", study.getUri().getScheme());
        catalogDBAdaptor.modifyFile(fileId, modifyParameters);
        return catalogDBAdaptor.getFile(fileId);
    }

    public QueryResult<File> createFolder(int studyId, Path folderPath, boolean parents, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkPath(folderPath, "folderPath");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = catalogDBAdaptor.getStudyOwnerId(studyId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);

        LinkedList<File> folders = new LinkedList<>();
        Path parent = folderPath.getParent();
        int parentId = -1;
        if(parent != null) {
            parentId = catalogDBAdaptor.getFileId(studyId, parent.toString() + "/");
        }
        if(!parents && parentId < 0 && parent != null){  //If !parents and parent does not exist in the DB (but should exist)
            throw new CatalogManagerException("Path '" + parent + "' does not exist");
        }

        /*
            PERMISSION CHECK
         */
        Acl fileAcl;
        if(parentId < 0) { //If it hasn't got parent, take the StudyAcl
            fileAcl = getStudyAcl(userId, studyId);
        } else {
            fileAcl = getFileAcl(userId, parentId);
        }

        if (!fileAcl.isWrite()) {
            throw new CatalogManagerException("Permission denied. Can't create files or folders in this study");
        }

        /*
            CHECK ALREADY EXISTS
         */
        if(catalogDBAdaptor.getFileId(studyId, folderPath.toString() + "/") >= 0) {
            throw new CatalogManagerException("Cannot create directory ‘" + folderPath + "’: File exists");
        }

        /*
            PARENTS FOLDERS
         */
        while(parentId < 0 && parent != null){  //Add all the parents that should be created
            folders.addFirst(new File(parent.getFileName().toString(), File.TYPE_FOLDER, "", "",
                    parent.toString() + "/", userId, "", File.READY, 0));
            parent = parent.getParent();
            if(parent != null) {
                parentId = catalogDBAdaptor.getFileId(studyId, parent.toString() + "/");
            }
        }

        ioManager.createFolder(ownerId, Integer.toString(projectId), Integer.toString(studyId), folderPath.toString(), parents);
        File mainFolder = new File(folderPath.getFileName().toString(), File.TYPE_FOLDER, "", "", folderPath.toString() + "/", userId
                , "", File.READY, 0);

        QueryResult<File> result;
        try {
            assert folders.size() == 0 && !parents || parents;
            for (File folder : folders) {
                catalogDBAdaptor.createFileToStudy(studyId, folder);
            }
            result = catalogDBAdaptor.createFileToStudy(studyId, mainFolder);
        } catch (CatalogManagerException e) {
            ioManager.deleteFile(ownerId, Integer.toString(projectId), Integer.toString(studyId), folderPath.toString());
            throw e;
        }
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return result;
    }

    public QueryResult deleteFile(int fileId, String sessionId)
            throws CatalogManagerException, IOException, CatalogIOManagerException {
        return deleteDataFromStudy(fileId, sessionId);
    }

    public QueryResult deleteFolder(int folderId, String sessionId)
            throws CatalogManagerException, IOException, CatalogIOManagerException {
        return deleteFile(folderId, sessionId);
    }

    @Deprecated
    private QueryResult deleteDataFromStudy(int fileId, String sessionId)
            throws CatalogManagerException, IOException, CatalogIOManagerException {
        //TODO: Save delete: Don't delete. Just rename file and set {deleted:true}
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = catalogDBAdaptor.getProjectOwnerId(projectId);

        if (!getFileAcl(userId, fileId).isDelete()) {
            throw new CatalogManagerException("Permission denied. User can't delete this file");
        }
        QueryResult<File> fileResult = catalogDBAdaptor.getFile(fileId);
        if (fileResult.getResult().isEmpty()) {
            return new QueryResult("Delete file", 0, 0, 0, "File not found", null, Collections.emptyList());
        }
        File file = fileResult.getResult().get(0);
        System.out.println("file = " + file);
        if(!file.getStatus().equals(File.READY)) {
            return new QueryResult("Delete file", 0, 0, 0, null,
                    "File is not ready. {id: " + file.getId() + ", status: '" + file.getStatus() + "'}",
                    Collections.emptyList());
        }
        /*
        try {
            ioManager.deleteFile(ownerId, Integer.toString(projectId), Integer.toString(studyId), file.getPath());
        } catch (CatalogIOManagerException e) {
            throw new CatalogManagerException(e);
        }
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return catalogDBAdaptor.deleteFile(fileId);*/

        catalogDBAdaptor.updateUserLastActivity(ownerId);
        ObjectMap objectMap = new ObjectMap();
        objectMap.put("status", File.DELETING);
        objectMap.put("attributes", new ObjectMap(File.DELETE_DATE, System.currentTimeMillis()));

        switch (file.getType()) {
            case File.TYPE_FOLDER:
                throw new UnsupportedOperationException("Unsupported deleting folder");
            case File.TYPE_FILE:
                renameFile(fileId, ".deleted_" + TimeUtils.getTime() + file.getName(), sessionId);
                return catalogDBAdaptor.modifyFile(fileId, objectMap);
            case File.TYPE_INDEX:
                renameFile(fileId, ".deleted_" + TimeUtils.getTime() + file.getName(), sessionId);
                return catalogDBAdaptor.modifyFile(fileId, objectMap);
        }
        return null;
    }

    public QueryResult moveFile(int fileId, int folderId, String sessionId) throws CatalogManagerException {
//        checkParameter(sessionId, "sessionId");
//        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
//        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
//        String ownerId = catalogDBAdaptor.getProjectOwnerId(projectId);
//
//        if (!getFileAcl(userId, fileId).isWrite()) {
//            throw new CatalogManagerException("Permission denied. User can't rename this file");
//        }
//        QueryResult<File> fileResult = catalogDBAdaptor.getFile(fileId);
//        if (fileResult.getResult().isEmpty()) {
//            return new QueryResult("Delete file", 0, 0, 0, "File not found", null, null);
//        }
//        File file = fileResult.getResult().get(0);
        throw new UnsupportedClassVersionError("move File unsupported");
    }

    public QueryResult renameFile(int fileId, String newName, String sessionId)
            throws CatalogManagerException, IOException, CatalogIOManagerException {
        checkParameter(sessionId, "sessionId");
        checkPath(newName, "newName");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = catalogDBAdaptor.getProjectOwnerId(projectId);

        if (!getFileAcl(userId, fileId).isWrite()) {
            throw new CatalogManagerException("Permission denied. User can't rename this file");
        }
        QueryResult<File> fileResult = catalogDBAdaptor.getFile(fileId);
        if (fileResult.getResult().isEmpty()) {
            return new QueryResult("Delete file", 0, 0, 0, "File not found", null, null);
        }
        File file = fileResult.getResult().get(0);
        System.out.println("file = " + file);

        String newPath = Paths.get(file.getPath()).getParent().resolve(newName).toString();

        catalogDBAdaptor.updateUserLastActivity(ownerId);
        switch (file.getType()) {
            case File.TYPE_FOLDER:
                throw new UnsupportedOperationException("Unsupported folder renaming");
            case File.TYPE_FILE:
                QueryResult<Study> studyQueryResult = catalogDBAdaptor.getStudy(studyId, null);
                Study study = studyQueryResult.getResult().get(0);
                CatalogIOManager catalogIOManager = catalogIOManagerFactory.get(study.getUri());
                catalogIOManager.rename(getFileUri(study.getUri(), file.getPath()), getFileUri(study.getUri(), newPath));
                return catalogDBAdaptor.renameFile(fileId, newPath);
            case File.TYPE_INDEX:
                return catalogDBAdaptor.renameFile(fileId, newName);
        }

        return null;
    }

    /**
     * Modify some params from the specified file:
     * <p/>
     * name
     * type
     * format
     * bioformat
     * description
     * status
     * <p/>
     * attributes
     * stats
     *
     * @param fileId     File identifier
     * @param parameters Parameters to change.
     * @param sessionId  sessionId to check permissions
     * @return
     * @throws CatalogManagerException
     */
    public QueryResult modifyFile(int fileId, ObjectMap parameters, String sessionId)
            throws CatalogManagerException {
        checkObj(parameters, "Parameters");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        switch (getUserRole(userId)) {
            case User.ROLE_ADMIN:
                logger.info("UserAdmin " + userId + " modifies file {id: " + fileId + "}");
                break;
            default:
                if (!getFileAcl(userId, fileId).isWrite()) {
                    throw new CatalogManagerException("User " + userId + " can't modify the file " + fileId);
                }
                for (String s : parameters.keySet()) {
                    if (!s.matches("name|type|format|bioformat|description|status|attributes|stats|jobId")) {
                        throw new CatalogManagerException("Parameter '" + s + "' can't be changed");
                    }
                }
            break;
        }
        String ownerId = catalogDBAdaptor.getFileOwnerId(fileId);
        QueryResult queryResult = catalogDBAdaptor.modifyFile(fileId, parameters);
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return queryResult;
    }

//    public QueryResult setIndexFile(int fileId, String backend, Index index, String sessionId) throws CatalogManagerException {
//        checkObj(backend, "backend");
//        checkParameter(sessionId, "sessionId");
//        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        if (!getFileAcl(userId, fileId).isWrite()) {
//            throw new CatalogManagerException("User " + userId + " can't modify the file " + fileId);
//        }
//        return catalogDBAdaptor.setIndexFile(fileId, backend, index);
//    }

    public QueryResult<File> getFileByIndexJobId(String indexJobId) throws CatalogManagerException {
        return catalogDBAdaptor.searchFile(new QueryOptions("indexJobId", indexJobId), null);
    }

    public QueryResult<File> getFile(int fileId, String sessionId)
            throws IOException, CatalogIOManagerException, CatalogManagerException {
        return getFile(fileId, null, sessionId);
    }
    public QueryResult<File> getFile(int fileId, QueryOptions options, String sessionId)
            throws CatalogIOManagerException, IOException, CatalogManagerException {
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!getFileAcl(userId, fileId).isRead()) {
            throw new CatalogManagerException("Permission denied. User can't read file");
        }

        return catalogDBAdaptor.getFile(fileId, options);
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

    public QueryResult<File> getAllFilesInFolder(int folderId, String sessionId) throws CatalogManagerException {
        checkParameter(sessionId, "sessionId");
        checkId(folderId, "folderId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByFileId(folderId);
        Acl studyAcl = getStudyAcl(userId, studyId);
        if (!studyAcl.isRead()) {
            throw new CatalogManagerException("Permission denied. User can't read file");
        }
        QueryResult<File> allFilesResult = catalogDBAdaptor.getAllFilesInFolder(folderId);
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


    /**
     * TODO: Set per-file ACL
     **/
    private QueryResult shareFile(int fileId, Acl acl, String sessionId) throws CatalogManagerException {
        checkObj(acl, "acl");
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl fileAcl = getFileAcl(userId, fileId);
        if (!fileAcl.isWrite()) {
            throw new CatalogManagerException("Permission denied. Can't modify file");
        }

        return catalogDBAdaptor.setFileAcl(fileId, acl);
    }

    /*Require role admin*/
    public QueryResult<File> searchFile(QueryOptions query, QueryOptions options, String sessionId) throws CatalogManagerException {
        return searchFile(-1, query, options, sessionId);
    }
    public QueryResult<File> searchFile(int studyId, QueryOptions query, String sessionId) throws CatalogManagerException {
        return searchFile(studyId, query, null, sessionId);
    }
    public QueryResult<File> searchFile(int studyId, QueryOptions query, QueryOptions options, String sessionId) throws CatalogManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        if(studyId < 0) {
            String role = getUserRole(userId);
            switch(role){
                case User.ROLE_ADMIN:
                    break;
                default:
                    throw new CatalogManagerException("Permission denied. StudyId or Admin role required");
            }
        } else {
            getStudyAcl(userId, studyId);
            query.put("studyId", studyId);
        }
        return catalogDBAdaptor.searchFile(query, options);
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
//            queryResult = queryBuilder.getVariantFrequencyByRegion(region, studyName,
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
//                indexReady = BamManager.checkIndex(ioManager.getFileUri(userId, bucketId, objectId));
//                if (force || !indexReady) {
//                    sgeJobName = BamManager.createIndex(getFileUri(userId, bucketId, objectId));
//                    catalogDBAdaptor.setObjectStatus(userId, bucketId, objectId, sgeJobName, sessionId);
//                }
//                break;
//            case "vcf":
//                indexReady = VcfManager.checkIndex(ioManager.getFileUri(userId, bucketId, objectId));
//                if (force || !indexReady) {
//                    sgeJobName = VcfManager.createIndex(getFileUri(userId, bucketId, objectId));
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

    public QueryResult refreshFolder(final int folderId, final String sessionId)
            throws CatalogManagerException, IOException {

        throw new UnsupportedOperationException();
//        final Path bucketPath = ioManager.getFileUri(userId, bucketId);
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
    }

    /*****************************
     * Analysis methods
     * ***************************
     */

//    public QueryResult<Analysis> createAnalysis(int studyId, String name, String alias, String description, String sessionId) throws CatalogManagerException {
//        checkParameter(name, "name");
//        checkAlias(alias, "alias");
//        checkParameter(sessionId, "sessionId");
//
//        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        if (!getStudyAcl(userId, studyId).isWrite()) {
//            throw new CatalogManagerException("Permission denied. Can't write in this study");
//        }
//        Analysis analysis = new Analysis(name, alias, TimeUtils.getTime(), userId, description);
//
//        String ownerId = catalogDBAdaptor.getStudyOwnerId(studyId);
//        catalogDBAdaptor.updateUserLastActivity(ownerId);
//        return catalogDBAdaptor.createAnalysis(studyId, analysis);
//    }
//
//
//    public QueryResult<Analysis> getAnalysis(int analysisId, String sessionId) throws CatalogManagerException {
//        checkParameter(sessionId, "sessionId");
//        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
//
//        if (!getStudyAcl(userId, studyId).isRead()) {
//            throw new CatalogManagerException("Permission denied. User can't read from this study"); //TODO: Should Analysis have ACL?
//        }
//
//        return catalogDBAdaptor.getAnalysis(analysisId);
//
//    }
//
//    public QueryResult<Analysis> getAllAnalysis(int studyId, String sessionId) throws CatalogManagerException {
//        checkParameter(sessionId, "sessionId");
//        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//
//        if (!getStudyAcl(userId, studyId).isRead()) {
//            throw new CatalogManagerException("Permission denied. Can't read from this study"); //TODO: Should Analysis have ACL?
//        }
//
//        return catalogDBAdaptor.getAllAnalysis(studyId);
//    }
//
//    public QueryResult modifyAnalysis(int analysisId, ObjectMap parameters, String sessionId) throws CatalogManagerException {
//        checkParameter(sessionId, "sessionId");
//        checkObj(parameters, "Parameters");
//        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
//
//        for (String s : parameters.keySet()) {
//            if (!s.matches("name|date|description|attributes")) {
//                throw new CatalogManagerException("Parameter '" + s + "' can't be changed");
//            }
//        }
//
//        if (!getStudyAcl(userId, studyId).isWrite()) {
//            throw new CatalogManagerException("Permission denied. Can't modify this analysis"); //TODO: Should Analysis have ACL?
//        }
//
//        String ownerId = catalogDBAdaptor.getAnalysisOwner(analysisId);
//        catalogDBAdaptor.updateUserLastActivity(ownerId);
//        return catalogDBAdaptor.modifyAnalysis(analysisId, parameters);
//    }

    public int getStudyIdByJobId(int jobId) throws CatalogManagerException {
        return catalogDBAdaptor.getStudyIdByJobId(jobId);
    }

    /**
     * Job methods
     * ***************************
     */

//    @Deprecated
//    public QueryResult<Job> createJob(int studyId, String name, String toolName, String description, String commandLine,
//                                      int outDirId, int tmpOutDirId, List<Integer> inputFiles, String sessionId)
//            throws CatalogManagerException, CatalogIOManagerException {
//        QueryOptions options = new QueryOptions("include", Arrays.asList("id", "type", "path"));
//        File tmpOutDir = catalogDBAdaptor.getFile(tmpOutDirId, options).getResult().get(0);     //TODO: Create tmpOutDir outside
//        return createJob(studyId, name, toolName, description, commandLine, outDirId, getFileUri(tmpOutDir), inputFiles, sessionId);
//    }


    public QueryResult<Job> createJob(int studyId, String name, String toolName, String description, String commandLine,
                                      URI tmpOutDirUri, int outDirId, List<Integer> inputFiles,
                                      Map<String, Object> resourceManagerAttributes, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(sessionId, "sessionId");
        checkParameter(name, "name");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        checkParameter(toolName, "toolName");
        checkParameter(commandLine, "commandLine");
        description = description != null? description : "";

        // FIXME check inputFiles? is a null conceptually valid?

//        URI tmpOutDirUri = createJobOutdir(studyId, randomString, sessionId);

        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogManagerException("Permission denied. Can't create job");
        }
        QueryOptions fileQueryOptions = new QueryOptions("include", Arrays.asList("id", "type", "path"));
        File outDir = catalogDBAdaptor.getFile(outDirId, fileQueryOptions).getResult().get(0);

        if(!outDir.getType().equals(File.TYPE_FOLDER)) {
            throw new CatalogManagerException("Bad outDir type. Required type : " + File.TYPE_FOLDER);
        }

        Job job = new Job(name, userId, toolName, description, commandLine, outDir.getId(), tmpOutDirUri, inputFiles);
        if(resourceManagerAttributes != null) {
            job.getResourceManagerAttributes().putAll(resourceManagerAttributes);
        }

        return catalogDBAdaptor.createJob(studyId, job);
    }

    public URI createJobOutDir(int studyId, String dirName, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(sessionId, "sessionId");
        checkParameter(dirName, "dirName");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        QueryOptions studyQueryOptions = null;
//        studyQueryOptions = new QueryOptions("include", Arrays.asList("projects.studies.uri", "projects.studies.id"));
        QueryResult<Study> studyQueryResult = getStudy(studyId, sessionId, studyQueryOptions);

        URI uri = studyQueryResult.getResult().get(0).getUri();
        CatalogIOManager catalogIOManager;
        try {
            catalogIOManager = catalogIOManagerFactory.get(uri);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogIOManagerException("Error in catalogIOManagerFactory", e);
        }
        return catalogIOManager.createJobOutDir(userId, dirName);
    }
//
//    public String checkJobStatus(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException {
//        return catalogDBAdaptor.getJobStatus(userId, jobId, sessionId);
//    }

    public QueryResult<ObjectMap> incJobVisites(int jobId, String sessionId) throws CatalogManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        int analysisId = catalogDBAdaptor.getStudyIdByJobId(jobId);
//        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
        int studyId = catalogDBAdaptor.getStudyIdByJobId(jobId);
        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogManagerException("Permission denied. Can't read job");
        }
        return catalogDBAdaptor.incJobVisits(jobId);
    }

    public QueryResult deleteJob(int jobId, String sessionId)
            throws CatalogManagerException, CatalogIOManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        int analysisId = catalogDBAdaptor.getStudyIdByJobId(jobId);
//        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
        int studyId = catalogDBAdaptor.getStudyIdByJobId(jobId);
        if (!getStudyAcl(userId, studyId).isDelete()) {
            throw new CatalogManagerException("Permission denied. Can't delete job");
        }

        return catalogDBAdaptor.deleteJob(jobId);
    }


    public QueryResult<Job> getJob(int jobId, String sessionId) throws IOException, CatalogIOManagerException, CatalogManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        int analysisId = catalogDBAdaptor.getStudyIdByJobId(jobId);
//        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
        int studyId = catalogDBAdaptor.getStudyIdByJobId(jobId);
        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogManagerException("Permission denied. Can't read job");
        }

        return catalogDBAdaptor.getJob(jobId);
    }

    public QueryResult<Job> getUnfinishedJobs(String sessionId) throws CatalogManagerException {
        String userId = getUserIdBySessionId(sessionId);
        String role = getUserRole(userId);
        switch(role){
            case User.ROLE_ADMIN:
                return catalogDBAdaptor.searchJob(new QueryOptions("ready", false));
            default:
                throw new CatalogManagerException("Permission denied. Admin role required");
        }
    }

//    public QueryResult<Job> getJobsByAnalysis(int analysisId, String sessionId) throws CatalogManagerException {
//        String userId = getUserIdBySessionId(sessionId);
////        getAnalysisAcl(); //TODO: Look for ACLs !!!
//        int studyId = getStudyIdByAnalysisId(analysisId);
//        if (getStudyAcl(userId, studyId).isRead()) {
//            return catalogDBAdaptor.searchJob(new QueryOptions("analysisId", analysisId));
//        } else {
//            throw new CatalogManagerException("Permission denied. User can't read this analysis");
//        }
//    }

    public QueryResult modifyJob(int jobId, ObjectMap parameters, String sessionId) throws CatalogManagerException {
        String userId = getUserIdBySessionId(sessionId);

        String role = getUserRole(userId);
        switch(role){
            case User.ROLE_ADMIN:
                return catalogDBAdaptor.modifyJob(jobId, parameters);
            default:
                throw new CatalogManagerException("Permission denied. Admin role required");
        }
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
//        Path jobPath = getUserUri(userId).resolve(catalogDBAdaptor.getJobPath(userId, jobId, sessionId));
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
//        Path jobPath = getUserUri(userId).resolve(catalogDBAdaptor.getJobPath(userId, jobId, sessionId));
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

    public QueryResult<Tool> createTool(String alias, String description, Object manifest, Object result,
                                        String path, boolean openTool, String sessionId) throws CatalogManagerException {
        checkParameter(alias, "alias");
        checkObj(description, "description"); //description can be empty
        checkParameter(path, "path");
        checkParameter(sessionId, "sessionId");
        //TODO: Check Path

        String userId = getUserIdBySessionId(sessionId);

        List<Acl> acl = Arrays.asList(new Acl(userId, true, true, true, true));
        if(openTool) {
            acl.add(new Acl(Acl.USER_OTHERS_ID, true, false, true, false));
        }

        String name = Paths.get(path).getFileName().toString();

        Tool tool = new Tool(-1, alias, name, description, manifest, result, path, acl);

        return catalogDBAdaptor.createTool(userId, tool);
    }

    public QueryResult<Tool> getTool(int id, String sessionId) throws CatalogManagerException {
        String userId = getUserIdBySessionId(sessionId);
        checkParameter(sessionId, "sessionId");

        //TODO: Check ACLs
        return catalogDBAdaptor.getTool(id);
    }

//    public QueryResult<Tool> searchTool(QueryOptions options, String sessionId) {
//        String userId = getUserIdBySessionId(sessionId);
//
//        catalogDBAdaptor.searchTool(options);
//    }

    /**
     * ****************
     */
    private void checkEmail(String email) throws CatalogManagerException {
        if (email == null || !emailPattern.matcher(email).matches()) {
            throw new CatalogManagerException("email not valid");
        }
    }

    private void checkId(int id, String name) throws CatalogManagerException {
        if (id < 0) {
            throw new CatalogManagerException("Error in id: '" + name + "' is not valid: "
                    + id + ".");
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
        if(path == null){
            throw new CatalogManagerException("parameter '" + name + "' is null.");
        }
        checkPath(Paths.get(path), name);
    }

    private void checkPath(Path path, String name) throws CatalogManagerException {
        checkObj(path, name);
        if (path.isAbsolute()) {
            throw new CatalogManagerException("Error in path: Path '" + name + "' can't be absolute");
        } else if (path.toString().matches("\\.|\\.\\.")){
            throw new CatalogManagerException("Error in path: Path '" + name + "' can't have relative names '.' or '..'");
        }
    }

    private void checkAlias(String alias, String name) throws CatalogManagerException {
        if(alias == null || alias.isEmpty() || !alias.matches("^[_A-Za-z0-9-\\+]+$") ){
            throw new CatalogManagerException("Error in alias: Invalid alias for '" + name + "'.");
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

    private String getUserRole(String userId) throws CatalogManagerException {
        return catalogDBAdaptor.getUser(userId, new QueryOptions("include", Arrays.asList("role")), null).getResult().get(0).getRole();
    }

    private Acl getProjectAcl(String userId, int projectId) throws CatalogManagerException {
        Acl projectAcl;
        if(getUserRole(userId).equals(User.ROLE_ADMIN)) {
            return new Acl(userId, true, true, true, true);
        }
        boolean sameOwner = catalogDBAdaptor.getProjectOwnerId(projectId).equals(userId);

        if(sameOwner){
            projectAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = catalogDBAdaptor.getProjectAcl(projectId, userId);
            if (!result.getResult().isEmpty()) {
                projectAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = catalogDBAdaptor.getProjectAcl(projectId, Acl.USER_OTHERS_ID);
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
        if(getUserRole(userId).equals(User.ROLE_ADMIN)) {
            return new Acl(userId, true, true, true, true);
        }
        boolean sameOwner = catalogDBAdaptor.getStudyOwnerId(studyId).equals(userId);

        if(sameOwner){
            studyAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = catalogDBAdaptor.getStudyAcl(studyId, userId);
            if (!result.getResult().isEmpty()) {
                studyAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = catalogDBAdaptor.getStudyAcl(studyId, Acl.USER_OTHERS_ID);
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
        if(getUserRole(userId).equals(User.ROLE_ADMIN)) {
            return new Acl(userId, true, true, true, true);
        }
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        return getStudyAcl(userId, studyId);
    }

    /**
     * Use StudyACL for all files.
     */
    private Acl getFileAcl(String userId, int fileId, Acl studyAcl) throws CatalogManagerException {
        return studyAcl;
    }

    //TODO: Check folder ACLs
    private Acl __getFileAcl(String userId, int fileId, Acl studyAcl) throws CatalogManagerException {
        Acl fileAcl;
        boolean sameOwner = catalogDBAdaptor.getFileOwnerId(fileId).equals(userId);

        if(sameOwner){
            fileAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = catalogDBAdaptor.getFileAcl(fileId, userId);
            if (!result.getResult().isEmpty()) {
                fileAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = catalogDBAdaptor.getFileAcl(fileId, Acl.USER_OTHERS_ID);
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


}
