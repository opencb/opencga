package org.opencb.opencga.catalog;

import com.mongodb.MongoCredential;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.core.CatalogDBClient;
import org.opencb.opencga.catalog.core.CatalogClient;
import org.opencb.opencga.catalog.core.ICatalogManager;
import org.opencb.opencga.catalog.db.CatalogDBAdaptor;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.db.CatalogMongoDBAdaptor;
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
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;

public class CatalogManager implements ICatalogManager {

    /* DBAdaptor properties */
    public static final String CATALOG_DB_USER = "OPENCGA.CATALOG.DB.USER";
    public static final String CATALOG_DB_DATABASE = "OPENCGA.CATALOG.DB.DATABASE";
    public static final String CATALOG_DB_PASSWORD = "OPENCGA.CATALOG.DB.PASSWORD";
    public static final String CATALOG_DB_HOST = "OPENCGA.CATALOG.DB.HOST";
    public static final String CATALOG_DB_PORT = "OPENCGA.CATALOG.DB.PORT";
    /* IOManager properties */
    public static final String CATALOG_MAIN_ROOTDIR = "OPENCGA.CATALOG.MAIN.ROOTDIR";
    /* Manager policies properties */
    public static final String CATALOG_MANAGER_POLICY_CREATION_USER = "OPENCGA.CATALOG.MANAGER.POLICY.CREATION_USER";
    /* Other properties */
    public static final String CATALOG_MAIL_USER = "CATALOG.MAIL.USER";
    public static final String CATALOG_MAIL_PASSWORD = "CATALOG.MAIL.PASSWORD";
    public static final String CATALOG_MAIL_HOST = "CATALOG.MAIL.HOST";
    public static final String CATALOG_MAIL_PORT = "CATALOG.MAIL.PORT";


    private CatalogDBAdaptor catalogDBAdaptor;
    private CatalogIOManager ioManager;
    private CatalogIOManagerFactory catalogIOManagerFactory;
    private CatalogClient catalogClient;

//    private PosixCatalogIOManager ioManager;


    private Properties properties;
    private String creationUserPolicy;

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

        configureManager(properties);
        configureIOManager(properties);
    }

    @Deprecated
    public CatalogManager(String rootdir)
            throws IOException, CatalogIOManagerException, CatalogDBException {
//        properties = Config.getAccountProperties();

        logger.debug("CatalogManager rootdir");
        Path path = Paths.get(rootdir, "conf", "catalog.properties");
        properties = new Properties();
        try {
            properties.load(Files.newInputStream(path));
        } catch (IOException e) {
            logger.error("Failed to load account.properties: " + e.getMessage());
            throw e;
        }

        configureManager(properties);
        configureDBAdaptor(properties);
        configureIOManager(properties);
    }

    public CatalogManager(Properties properties)
            throws CatalogIOManagerException, CatalogDBException {
        this.properties = properties;
        logger.debug("CatalogManager configureManager");
        configureManager(properties);
        logger.debug("CatalogManager configureDBAdaptor");
        configureDBAdaptor(properties);
        logger.debug("CatalogManager configureIOManager");
        configureIOManager(properties);
    }

    @Override
    public CatalogClient client() {
        return client("");
    }

    @Override
    public CatalogClient client(String sessionId) {
        catalogClient.setSessionId(sessionId);
        return catalogClient;
    }

    @Override
    public CatalogIOManagerFactory getCatalogIOManagerFactory() {
        return catalogIOManagerFactory;
    }

    private void configureIOManager(Properties properties)
            throws CatalogIOManagerException {
        catalogIOManagerFactory = new CatalogIOManagerFactory(properties);
//        ioManager = this.catalogIOManagerFactory.get(properties.getProperty("CATALOG.MODE", DEFAULT_CATALOG_SCHEME));
        String scheme = URI.create(properties.getProperty(CATALOG_MAIN_ROOTDIR)).getScheme();
        if (scheme == null) {
            scheme = "file";
        }
        ioManager = this.catalogIOManagerFactory.get(scheme);
    }

    private void configureDBAdaptor(Properties properties)
            throws CatalogDBException {

        MongoCredential mongoCredential = MongoCredential.createMongoCRCredential(
                properties.getProperty(CATALOG_DB_USER, ""),
                properties.getProperty(CATALOG_DB_DATABASE, ""),
                properties.getProperty(CATALOG_DB_PASSWORD, "").toCharArray());

        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                properties.getProperty(CATALOG_DB_HOST, ""),
                Integer.parseInt(properties.getProperty(CATALOG_DB_PORT, "0")));

        catalogDBAdaptor = new CatalogMongoDBAdaptor(dataStoreServerAddress, mongoCredential);
    }

    private void configureManager(Properties properties) {
        creationUserPolicy = properties.getProperty(CATALOG_MANAGER_POLICY_CREATION_USER, "always");
        catalogClient = new CatalogDBClient(this);

        //TODO: Check if is empty
        //TODO: Setup catalog if it's empty.
    }

    /**
     * Getter path methods
     * ***************************
     */

    @Override
    public URI getUserUri(String userId) throws CatalogIOManagerException {
        return ioManager.getUserUri(userId);
    }

    @Override
    public URI getProjectUri(String userId, String projectId) throws CatalogIOManagerException {
        return ioManager.getProjectUri(userId, projectId);
    }

    @Override
    public URI getStudyUri(int studyId)
            throws CatalogException {
        return catalogDBAdaptor.getStudy(studyId, new QueryOptions("include", Arrays.asList("projects.studies.uri"))).first().getUri();
    }

    @Override
    public URI getFileUri(int studyId, String relativeFilePath)
            throws CatalogException {
        return ioManager.getFileUri(getStudyUri(studyId), relativeFilePath);
    }

    @Override
    public URI getFileUri(URI studyUri, String relativeFilePath)
            throws CatalogIOManagerException, IOException {
        return catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, relativeFilePath);
    }

    @Override
    public URI getFileUri(File file) throws CatalogException {
        int studyId = catalogDBAdaptor.getStudyIdByFileId(file.getId());
        return getFileUri(studyId, file.getPath());
    }

    @Override
    public int getProjectIdByStudyId(int studyId) throws CatalogException {
        return catalogDBAdaptor.getProjectIdByStudyId(studyId);
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

    @Override
    public int getProjectId(String id) throws CatalogDBException {
        try {
            return Integer.parseInt(id);
        } catch (NumberFormatException ignore) {
        }

        String[] split = id.split("@");
        if (split.length != 2) {
            return -1;
        }
        return catalogDBAdaptor.getProjectId(split[0], split[1]);
    }

    @Override
    public int getStudyId(String id) throws CatalogDBException {
        try {
            return Integer.parseInt(id);
        } catch (NumberFormatException ignore) {
        }

        String[] split = id.split("@");
        if (split.length != 2) {
            return -1;
        }
        String[] projectStudy = split[1].replace(':', '/').split("/", 2);
        if (projectStudy.length != 2) {
            return -2;
        }
        int projectId = catalogDBAdaptor.getProjectId(split[0], projectStudy[0]);
        return catalogDBAdaptor.getStudyId(projectId, projectStudy[1]);
    }

    @Override
    public int getFileId(String id) throws CatalogDBException {
        try {
            return Integer.parseInt(id);
        } catch (NumberFormatException ignore) {
        }

        String[] split = id.split("@", 2);
        if (split.length != 2) {
            return -1;
        }
        String[] projectStudyPath = split[1].replace(':', '/').split("/", 3);
        if (projectStudyPath.length <= 2) {
            return -2;
        }
        int projectId = catalogDBAdaptor.getProjectId(split[0], projectStudyPath[0]);
        int studyId = catalogDBAdaptor.getStudyId(projectId, projectStudyPath[1]);
        return catalogDBAdaptor.getFileId(studyId, projectStudyPath[2]);
    }

    @Override
    public int getToolId(String id) throws CatalogDBException {
        try {
            return Integer.parseInt(id);
        } catch (NumberFormatException ignore) {
        }

        String[] split = id.split("@");
        if (split.length != 2) {
            return -1;
        }
        return catalogDBAdaptor.getToolId(split[0], split[1]);
    }

    /**
     * User methods
     * ***************************
     */

    @Override
    public QueryResult<User> createUser(String id, String name, String email, String password, String organization, QueryOptions options)
            throws CatalogException {
        return createUser(id, name, email, password, organization, options, null);
    }

    @Override
    public QueryResult<User> createUser(String id, String name, String email, String password, String organization, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(id, "id");
        checkParameter(password, "password");
        checkParameter(name, "name");
        checkParameter(email, "email");
        organization = organization != null ? organization : "";

        User user = new User(id, name, email, password, organization, User.Role.USER, "");

        String userId = null;
        switch (creationUserPolicy) {
            case "onlyAdmin":
                userId = getUserIdBySessionId(sessionId);
                if (!userId.isEmpty() && getUserRole(userId).equals(User.Role.ADMIN)) {
                    user.getAttributes().put("creatorUserId", userId);
                } else {
                    throw new CatalogException("CreateUser Fail. Required Admin role");
                }
                break;
            case "anyLoggedUser":
                checkParameter(sessionId, "sessionId");
                userId = getUserIdBySessionId(sessionId);
                if (userId.isEmpty()) {
                    throw new CatalogException("CreateUser Fail. Required existing account");
                }
                user.getAttributes().put("creatorUserId", userId);
                break;
            case "always":
            default:
                break;
        }


        try {
            ioManager.createUser(user.getId());
            return catalogDBAdaptor.insertUser(user, options);
        } catch (CatalogIOManagerException | CatalogDBException e) {
            ioManager.deleteUser(user.getId());
            throw e;
        }
    }

    @Override
    public QueryResult<ObjectMap> loginAsAnonymous(String sessionIp)
            throws CatalogException, IOException {
        checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp);

        String userId = "anonymous_" + session.getId();

        // TODO sessionID should be created here

        ioManager.createAnonymousUser(userId);

        try {
            return catalogDBAdaptor.loginAsAnonymous(session);
        } catch (CatalogDBException e) {
            ioManager.deleteUser(userId);
            throw e;
        }

    }

    @Override
    public QueryResult<ObjectMap> login(String userId, String password, String sessionIp)
            throws CatalogException, IOException {
        checkParameter(userId, "userId");
        checkParameter(password, "password");
        checkParameter(sessionIp, "sessionIp");
        Session session = new Session(sessionIp);

        return catalogDBAdaptor.login(userId, password, session);
    }

    @Override
    public QueryResult logout(String userId, String sessionId) throws CatalogException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        switch (getUserRole(userId)) {
            default:
                return catalogDBAdaptor.logout(userId, sessionId);
            case ANONYMOUS:
                return logoutAnonymous(sessionId);
        }
    }

    @Override
    public QueryResult logoutAnonymous(String sessionId) throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = getUserIdBySessionId(sessionId);
        checkParameter(userId, "userId");
        checkSessionId(userId, sessionId);

        logger.info("logout anonymous user. userId: " + userId + " sesionId: " + sessionId);

        ioManager.deleteAnonymousUser(userId);
        return catalogDBAdaptor.logoutAnonymous(sessionId);
    }

    @Override
    public QueryResult changePassword(String userId, String oldPassword, String newPassword, String sessionId)
            throws CatalogException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkParameter(oldPassword, "oldPassword");
        checkParameter(newPassword, "newPassword");
        checkSessionId(userId, sessionId);  //Only the user can change his own password
        catalogDBAdaptor.updateUserLastActivity(userId);
        return catalogDBAdaptor.changePassword(userId, oldPassword, newPassword);
    }

    @Override
    public QueryResult changeEmail(String userId, String nEmail, String sessionId) throws CatalogException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        checkEmail(nEmail);
        catalogDBAdaptor.updateUserLastActivity(userId);
        return catalogDBAdaptor.changeEmail(userId, nEmail);
    }

    @Override
    public QueryResult resetPassword(String userId, String email) throws CatalogException {
        checkParameter(userId, "userId");
        checkEmail(email);
        catalogDBAdaptor.updateUserLastActivity(userId);

        String newPassword = StringUtils.randomString(6);
        String newCryptPass;
        try {
            newCryptPass = StringUtils.sha1(newPassword);
        } catch (NoSuchAlgorithmException e) {
            throw new CatalogDBException("could not encode password");
        }

        QueryResult qr = catalogDBAdaptor.resetPassword(userId, email, newCryptPass);

        String mailUser = properties.getProperty(CATALOG_MAIL_USER);
        String mailPassword = properties.getProperty(CATALOG_MAIL_PASSWORD);
        String mailHost = properties.getProperty(CATALOG_MAIL_HOST);
        String mailPort = properties.getProperty(CATALOG_MAIL_PORT);

        MailUtils.sendResetPasswordMail(email, newPassword, mailUser, mailPassword, mailHost, mailPort);

        return qr;
    }


    @Override
    public QueryResult<User> getUser(String userId, String lastActivity, String sessionId) throws CatalogException {
        return getUser(userId, lastActivity, new QueryOptions(), sessionId);
    }

    @Override
    public QueryResult<User> getUser(String userId, String lastActivity, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkSessionId(userId, sessionId);
        options = defaultObject(options, new QueryOptions());

        if (!options.containsKey("include") && !options.containsKey("exclude")) {
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

    @Override
    public String getUserIdBySessionId(String sessionId) {
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
     * @throws org.opencb.opencga.catalog.db.CatalogDBException
     */
    @Override
    public QueryResult modifyUser(String userId, ObjectMap parameters, String sessionId)
            throws CatalogException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        checkObj(parameters, "parameters");
        checkSessionId(userId, sessionId);
        for (String s : parameters.keySet()) {
            if (!s.matches("name|email|organization|attributes|configs")) {
                throw new CatalogDBException("Parameter '" + s + "' can't be changed");
            }
        }
        if (parameters.containsKey("email")) {
            checkEmail(parameters.getString("email"));
        }
        catalogDBAdaptor.updateUserLastActivity(userId);
        return catalogDBAdaptor.modifyUser(userId, parameters);
    }

    @Override
    public void deleteUser(String userId, String sessionId) throws CatalogException {
        checkParameter(userId, "userId");
        checkParameter(sessionId, "sessionId");
        String userIdBySessionId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (userIdBySessionId.equals(userId) || getUserRole(userIdBySessionId).equals(User.Role.ADMIN)) {
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

    @Override
    public QueryResult<Project> createProject(String ownerId, String name, String alias, String description,
                                              String organization, QueryOptions options, String sessionId)
            throws CatalogException {
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

        QueryResult<Project> result = catalogDBAdaptor.createProject(ownerId, project, options);
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

    @Override
    public QueryResult<Project> getProject(int projectId, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        Acl projectAcl = getProjectAcl(userId, projectId);
        if (projectAcl.isRead()) {
            QueryResult<Project> projectResult = catalogDBAdaptor.getProject(projectId, options);
            if (!projectResult.getResult().isEmpty()) {
                filterStudies(userId, projectAcl, projectResult.getResult().get(0).getStudies());
            }
            return projectResult;
        } else {
            throw new CatalogDBException("Permission denied. Can't read project.");
        }
    }

    @Override
    public QueryResult<Project> getAllProjects(String ownerId, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(ownerId, "ownerId");
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        QueryResult<Project> allProjects = catalogDBAdaptor.getAllProjects(ownerId, options);

        List<Project> projects = allProjects.getResult();
        filterProjects(userId, projects);
        allProjects.setResult(projects);

        return allProjects;
    }

    @Override
    public QueryResult renameProject(int projectId, String newProjectAlias, String sessionId)
            throws CatalogException {
        checkAlias(newProjectAlias, "newProjectAlias");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = catalogDBAdaptor.getProjectOwnerId(projectId);

        Acl projectAcl = getProjectAcl(userId, projectId);
        if (projectAcl.isWrite()) {
            catalogDBAdaptor.updateUserLastActivity(ownerId);
            return catalogDBAdaptor.renameProjectAlias(projectId, newProjectAlias);
        } else {
            throw new CatalogDBException("Permission denied. Can't rename project");
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
     * @throws org.opencb.opencga.catalog.db.CatalogDBException
     */
    @Override
    public QueryResult modifyProject(int projectId, ObjectMap parameters, String sessionId)
            throws CatalogException {
        checkObj(parameters, "Parameters");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = catalogDBAdaptor.getProjectOwnerId(projectId);
        if (!getProjectAcl(userId, projectId).isWrite()) {
            throw new CatalogDBException("User '" + userId + "' can't modify the project " + projectId);
        }
        for (String s : parameters.keySet()) {
            if (!s.matches("name|description|organization|status|attributes")) {
                throw new CatalogDBException("Parameter '" + s + "' can't be changed");
            }
        }
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return catalogDBAdaptor.modifyProject(projectId, parameters);
    }

    @Override
    public QueryResult shareProject(int projectId, Acl acl, String sessionId) throws CatalogException {
        checkObj(acl, "acl");
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl projectAcl = getProjectAcl(userId, projectId);
        if (!projectAcl.isWrite()) {
            throw new CatalogDBException("Permission denied. Can't modify project");
        }

        return catalogDBAdaptor.setProjectAcl(projectId, acl);
    }

    /**
     * Study methods
     * ***************************
     */
    @Override
    public QueryResult<Study> createStudy(int projectId, String name, String alias, Study.Type type, String description,
                                          String sessionId)
            throws CatalogException, IOException {
        return createStudy(projectId, name, alias, type, null, null, description, null, null, null, null, null, null, sessionId);
    }

    @Override
    public QueryResult<Study> createStudy(int projectId, String name, String alias, Study.Type type,
                                          String creatorId, String creationDate, String description, String status,
                                          String cipher, String uriScheme, Map<String, Object> stats,
                                          Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException, IOException {
        return createStudy(projectId, name, alias, type, creatorId, creationDate, description, status, cipher, uriScheme,
                null, stats, attributes, options, sessionId);
    }

    /**
     * Creates a new Study in catalog
     * @param projectId     Parent project id
     * @param name          Study Name
     * @param alias         Study Alias. Must be unique in the project's studies
     * @param type          Study type: CONTROL_CASE, CONTROL_SET, ... (see org.opencb.opencga.catalog.beans.Study.Type)
     * @param creatorId     Creator user id. If null, user by sessionId
     * @param creationDate  Creation date. If null, now
     * @param description   Study description. If null, empty string
     * @param status        Unused
     * @param cipher        Unused
     * @param uriScheme     UriScheme to select the CatalogIOManager. Default: CatalogIOManagerFactory.DEFAULT_CATALOG_SCHEME
     * @param uri           URI for the folder where to place the study. Scheme must match with the uriScheme. Folder must exist.
     * @param stats         Optional stats
     * @param attributes    Optional attributes
     * @param options       QueryOptions
     * @param sessionId     User's sessionId
     * @return              Generated study
     * @throws CatalogException
     * @throws IOException
     */
    @Override
    public QueryResult<Study> createStudy(int projectId, String name, String alias, Study.Type type,
                                          String creatorId, String creationDate, String description, String status,
                                          String cipher, String uriScheme, URI uri, Map<String, Object> stats,
                                          Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException, IOException {
        checkParameter(name, "name");
        checkParameter(alias, "alias");
        checkObj(type, "type");
        checkAlias(alias, "alias");
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        description = defaultString(description, "");
        creatorId = defaultString(creatorId, userId);
        creationDate = defaultString(creationDate, TimeUtils.getTime());
        status = defaultString(status, "active");
        cipher = defaultString(cipher, "none");
        if (uri != null) {
            if (uri.getScheme() == null) {
                throw new CatalogException("StudyUri must specify the scheme");
            } else {
                if (uriScheme != null && !uriScheme.isEmpty()) {
                    if (!uriScheme.equals(uri.getScheme())) {
                        throw new CatalogException("StudyUri must specify the scheme");
                    }
                } else {
                    uriScheme = uri.getScheme();
                }
            }
        } else {
            uriScheme = defaultString(uriScheme, CatalogIOManagerFactory.DEFAULT_CATALOG_SCHEME);
        }
        stats = defaultObject(stats, new HashMap<String, Object>());
        attributes = defaultObject(attributes, new HashMap<String, Object>());

        CatalogIOManager catalogIOManager = catalogIOManagerFactory.get(uriScheme);

        String projectOwnerId = catalogDBAdaptor.getProjectOwnerId(projectId);


        /* Check project permissions */
        if (!getProjectAcl(userId, projectId).isWrite()) { //User can't write/modify the project
            throw new CatalogDBException("Permission denied. Can't write in project");
        }
        if (!creatorId.equals(userId)) {
            if (!getUserRole(userId).equals(User.Role.ADMIN)) {
                throw new CatalogException("Permission denied. Required ROLE_ADMIN to create a study with creatorId != userId");
            } else {
                if (!catalogDBAdaptor.userExists(creatorId)) {
                    throw new CatalogException("ERROR: CreatorId does not exist.");
                }
            }
        }

//        URI projectUri = catalogIOManager.getProjectUri(projectOwnerId, Integer.toString(projectId));
        LinkedList<File> files = new LinkedList<>();
        LinkedList<Experiment> experiments = new LinkedList<>();
        LinkedList<Job> jobs = new LinkedList<>();
        LinkedList<Acl> acls = new LinkedList<>();

        /* Add default ACL */
        if (!creatorId.equals(projectOwnerId)) {
            //Add full permissions for the creator if he is not the owner
            acls.add(new Acl(creatorId, true, true, true, true));
        }

        //Copy generic permissions from the project.

        QueryResult<Acl> aclQueryResult = catalogDBAdaptor.getProjectAcl(projectId, Acl.USER_OTHERS_ID);
        if (!aclQueryResult.getResult().isEmpty()) {
            //study.getAcl().add(aclQueryResult.getResult().get(0));
        } else {
            throw new CatalogDBException("Project " + projectId + " must have generic ACL");
        }


        files.add(new File(".", File.Type.FOLDER, null, null, "", creatorId, "study root folder", File.Status.READY, 0));

        Study study = new Study(-1, name, alias, type, creatorId, creationDate, description, status, TimeUtils.getTime(),
                0, cipher, acls, experiments, files, jobs, new LinkedList<Sample>(), new LinkedList<Dataset>(),
                new LinkedList<Cohort>(), new LinkedList<VariableSet>(), null, stats, attributes);


        /* CreateStudy */
        QueryResult<Study> result = catalogDBAdaptor.createStudy(projectId, study, options);
        study = result.getResult().get(0);

//        URI studyUri;
        if (uri == null) {
            try {
                uri = catalogIOManager.createStudy(projectOwnerId, Integer.toString(projectId), Integer.toString(study.getId()));
            } catch (CatalogIOManagerException e) {
                e.printStackTrace();
                catalogDBAdaptor.deleteStudy(study.getId());
                throw e;
            }
        }

        catalogDBAdaptor.modifyStudy(study.getId(), new ObjectMap("uri", uri));

        createFolder(result.getResult().get(0).getId(), Paths.get("data"), true, null, sessionId);
        createFolder(result.getResult().get(0).getId(), Paths.get("analysis"), true, null, sessionId);

        catalogDBAdaptor.updateUserLastActivity(projectOwnerId);
        return result;
    }

    @Override
    public QueryResult<Study> getStudy(int studyId, String sessionId)
            throws CatalogException {
        return getStudy(studyId, sessionId, null);
    }

    @Override
    public QueryResult<Study> getStudy(int studyId, String sessionId, QueryOptions options)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl studyAcl = getStudyAcl(userId, studyId);
        if (studyAcl.isRead()) {
            QueryResult<Study> studyResult = catalogDBAdaptor.getStudy(studyId, options);
            if (!studyResult.getResult().isEmpty()) {
                filterFiles(userId, studyAcl, studyResult.getResult().get(0).getFiles());
            }
            return studyResult;
        } else {
            throw new CatalogDBException("Permission denied. Can't read this study");
        }
    }

    @Override
    public QueryResult<Study> getAllStudies(int projectId, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        Acl projectAcl = getProjectAcl(userId, projectId);
        if (!projectAcl.isRead()) {
            throw new CatalogDBException("Permission denied. Can't read project");
        }

        QueryResult<Study> allStudies = catalogDBAdaptor.getAllStudies(projectId, options);
        List<Study> studies = allStudies.getResult();
        filterStudies(userId, projectAcl, studies);
        allStudies.setResult(studies);

        return allStudies;


    }

    @Override
    public QueryResult renameStudy(int studyId, String newStudyAlias, String sessionId)
            throws CatalogException {
        checkAlias(newStudyAlias, "newStudyAlias");
        checkParameter(sessionId, "sessionId");
        String sessionUserId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String studyOwnerId = catalogDBAdaptor.getStudyOwnerId(studyId);

        if (!getStudyAcl(sessionUserId, studyId).isWrite()) {  //User can't write/modify the study
            throw new CatalogDBException("Permission denied. Can't write in project");
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
     * @throws org.opencb.opencga.catalog.db.CatalogDBException
     */
    @Override
    public QueryResult modifyStudy(int studyId, ObjectMap parameters, String sessionId)
            throws CatalogException {
        checkObj(parameters, "Parameters");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogDBException("User " + userId + " can't modify the study " + studyId);
        }
        for (String s : parameters.keySet()) {
            if (!s.matches("name|type|description|status|attributes|stats")) {
                throw new CatalogDBException("Parameter '" + s + "' can't be changed");
            }
        }

        String ownerId = catalogDBAdaptor.getStudyOwnerId(studyId);
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return catalogDBAdaptor.modifyStudy(studyId, parameters);
    }

    @Override
    public QueryResult shareStudy(int studyId, Acl acl, String sessionId) throws CatalogException {
        checkObj(acl, "acl");
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl studyAcl = getStudyAcl(userId, studyId);
        if (!studyAcl.isWrite()) {
            throw new CatalogDBException("Permission denied. Can't modify project");
        }

        return catalogDBAdaptor.setStudyAcl(studyId, acl);
    }

    /**
     * File methods
     * ***************************
     */

    @Override
    public String getFileOwner(int fileId) throws CatalogDBException {
        return catalogDBAdaptor.getFileOwnerId(fileId);
    }

    @Override
    public int getStudyIdByFileId(int fileId) throws CatalogDBException {
        return catalogDBAdaptor.getStudyIdByFileId(fileId);
    }

//    public int getStudyIdByAnalysisId(int analysisId) throws CatalogManagerException {
//        return catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
//    }

    @Override
    @Deprecated
    public QueryResult<File> createFile(int studyId, File.Format format, File.Bioformat bioformat, String path, String description,
                                        boolean parents, String sessionId)
            throws CatalogException, CatalogIOManagerException {
        return createFile(studyId, format, bioformat, path, description, parents, -1, sessionId);
    }


//    @Deprecated
//    public QueryResult<File> uploadFile(int studyId, File.Format format, File.Bioformat bioformat, String path, String description,
//                                        boolean parents, String sessionId)
//            throws CatalogException {
//
//        QueryResult<File> result = createFile(studyId, format, bioformat, path, description, parents, -1, sessionId);
//        File file = result.getResult().get(0);
//
//        ObjectMap modifyParameters = new ObjectMap("status", File.Status.READY);
//        catalogDBAdaptor.modifyFile(file.getId(), modifyParameters);
//
//        return result;
//    }

    //create file with byte[]
    @Override
    public QueryResult<File> createFile(int studyId, File.Format format, File.Bioformat bioformat, String path, byte[] bytes, String description,
                                        boolean parents, String sessionId)
            throws CatalogException, IOException {

        QueryResult<File> result = createFile(studyId, format, bioformat, path, description, parents, -1, sessionId);
        File file = result.getResult().get(0);
        //path is relative to user, get full path...
        URI studyURI = getStudyUri(studyId);
        URI fileURI = getFileUri(studyURI, path);
        Files.write(Paths.get(fileURI), bytes);

        ObjectMap modifyParameters = new ObjectMap("status", File.Status.READY);
        catalogDBAdaptor.modifyFile(file.getId(), modifyParameters);

        return result;
    }

    public QueryResult<File> createFile(int studyId, File.Format format, File.Bioformat bioformat, String path, Path completedFilePath, String description,
                                        boolean parents, String sessionId)
            throws CatalogException, IOException {

        QueryResult<File> result = createFile(studyId, format, bioformat, path, description, parents, -1, sessionId);
        File file = result.getResult().get(0);
        //path is relative to user, get full path...
        URI studyURI = getStudyUri(studyId);
        URI fileURI = getFileUri(studyURI, path);
        Files.move(completedFilePath, Paths.get(fileURI));

        ObjectMap modifyParameters = new ObjectMap("status", File.Status.READY);
        catalogDBAdaptor.modifyFile(file.getId(), modifyParameters);

        return result;
    }

    public QueryResult<File> createFile(int studyId, File.Format format, File.Bioformat bioformat, String path, String description,
                                        boolean parents, int jobId, String sessionId)
            throws CatalogException, CatalogIOManagerException {
        return createFile(studyId, File.Type.FILE, format, bioformat, path, null, null, description, null, 0, -1, null,
                jobId, null, null, parents, null, sessionId);
    }


    @Override
    public QueryResult<File> createFile(int studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                        String ownerId, String creationDate, String description, File.Status status,
                                        long diskUsage, int experimentId, List<Integer> sampleIds, int jobId,
                                        Map<String, Object> stats, Map<String, Object> attributes,
                                        boolean parents, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        checkParameter(sessionId, "sessionId");
        checkPath(path, "filePath");

        type = defaultObject(type, File.Type.FILE);
        format = defaultObject(format, File.Format.PLAIN);  //TODO: Inference from the file name
        bioformat = defaultObject(bioformat, File.Bioformat.NONE);
        ownerId = defaultString(ownerId, userId);
        creationDate = defaultString(creationDate, TimeUtils.getTime());
        description = defaultString(description, "");
        status = defaultObject(status, File.Status.UPLOADING);

        if (diskUsage < 0) {
            throw new CatalogException("Error: DiskUsage can't be negative!");
        }
        if (experimentId > 0 && !catalogDBAdaptor.experimentExists(experimentId)) {
            throw new CatalogException("Experiment { id: " + experimentId + "} does not exist.");
        }
        sampleIds = defaultObject(sampleIds, new LinkedList<Integer>());

        for (Integer sampleId : sampleIds) {
            if (!catalogDBAdaptor.sampleExists(sampleId)) {
                throw new CatalogException("Sample { id: " + sampleId + "} does not exist.");
            }
        }

        if (jobId > 0 && !catalogDBAdaptor.jobExists(jobId)) {
            throw new CatalogException("Job { id: " + jobId + "} does not exist.");
        }
        stats = defaultObject(stats, new HashMap<String, Object>());
        attributes = defaultObject(attributes, new HashMap<String, Object>());

        if (!catalogDBAdaptor.studyExists(studyId)) {
            throw new CatalogException("Study { id: " + studyId + "} does not exist.");
        }

        if (!ownerId.equals(userId)) {
            if (!getUserRole(userId).equals(User.Role.ADMIN)) {
                throw new CatalogException("Permission denied. Required ROLE_ADMIN to create a file with ownerId != userId");
            } else {
                if (!catalogDBAdaptor.userExists(ownerId)) {
                    throw new CatalogException("ERROR: ownerId does not exist.");
                }
            }
        }

        if (status != File.Status.UPLOADING && status != File.Status.INDEXING) {
            if (!getUserRole(userId).equals(User.Role.ADMIN)) {
                throw new CatalogException("Permission denied. Required ROLE_ADMIN to create a file with status != UPLOADING and INDEXING");
            }
        }

        //Find parent. If parents == true, create folders.
        Path parent = Paths.get(path).getParent();
        int fileId = -1;
        if (parent != null) {
            fileId = catalogDBAdaptor.getFileId(studyId, parent.toString() + "/");
        }
        if (fileId < 0 && parent != null) {
            if (parents) {
                createFolder(studyId, parent, true, null, sessionId);
                fileId = catalogDBAdaptor.getFileId(studyId, parent.toString() + "/");
            } else {
                throw new CatalogDBException("Directory not found " + parent.toString());
            }
        }

        //Check permissions
        Acl parentAcl;
        if (fileId < 0) {
            parentAcl = getStudyAcl(userId, studyId);
        } else {
            parentAcl = getFileAcl(userId, fileId);
        }

        if (!parentAcl.isWrite()) {
            throw new CatalogException("Permission denied, " + userId + " can not write in " +
                    (parent != null ? "directory " + parent.toString() : "study " + studyId));
        }


        //Create file entry
        File file = new File(-1, Paths.get(path).getFileName().toString(), type, format, bioformat,
                path, ownerId, creationDate, description, status, diskUsage, experimentId, sampleIds, jobId,
                new LinkedList<Acl>(), stats, attributes);

        return catalogDBAdaptor.createFileToStudy(studyId, file, options);

    }

    private <T> T defaultObject(T object, T defaultObject) {
        if (object == null) {
            object = defaultObject;
        }
        return object;
    }

    @Override
    @Deprecated
    public QueryResult<File> uploadFile(int studyId, File.Format format, File.Bioformat bioformat, String path, String description,
                                        boolean parents, InputStream fileIs, String sessionId)
            throws IOException, CatalogException {
        QueryResult<File> fileResult = createFile(studyId, format, bioformat, path, description, parents, sessionId);
        int fileId = fileResult.getResult().get(0).getId();
        try {
            fileResult = uploadFile(fileId, fileIs, sessionId);
        } catch (CatalogIOManagerException | InterruptedException | CatalogDBException | IOException e) {
            deleteFile(fileId, sessionId);
            e.printStackTrace();
        }
        return fileResult;
    }


    @Override
    @Deprecated
    public QueryResult<File> uploadFile(int fileId, InputStream fileIs, String sessionId) throws CatalogException,
            IOException, InterruptedException {

        checkObj(fileIs, "InputStream");
        checkParameter(sessionId, "SessionId");

        String userId = catalogDBAdaptor.getFileOwnerId(fileId);
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);

        List<File> result = catalogDBAdaptor.getFile(fileId).getResult();
        if (result.isEmpty()) {
            throw new CatalogDBException("FileId '" + fileId + "' for found");
        }
        File file = result.get(0);
        if (!file.getStatus().equals(File.Status.UPLOADING)) {
            throw new CatalogDBException("File '" + fileId + "' already uploaded.");
        }
        if (!file.getOwnerId().equals(userId)) {
            throw new CatalogDBException("UserId mismatch with file creator");
        }
        ioManager.createFile(getStudyUri(studyId), file.getPath(), fileIs);
        Study study = catalogDBAdaptor.getStudy(studyId, null).getResult().get(0);

        ObjectMap modifyParameters = new ObjectMap("status", File.Status.UPLOADED);
        modifyParameters.put("uriScheme", study.getUri().getScheme());
        catalogDBAdaptor.modifyFile(fileId, modifyParameters);
        return catalogDBAdaptor.getFile(fileId);
    }

    @Override
    public QueryResult<File> createFolder(int studyId, Path folderPath, boolean parents, QueryOptions options, String sessionId)
            throws CatalogException {
        checkPath(folderPath, "folderPath");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        String ownerId = catalogDBAdaptor.getStudyOwnerId(studyId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);

        LinkedList<File> folders = new LinkedList<>();
        Path parent = folderPath.getParent();
        int parentId = -1;
        if (parent != null) {
            parentId = catalogDBAdaptor.getFileId(studyId, parent.toString() + "/");
        }
        if (!parents && parentId < 0 && parent != null) {  //If !parents and parent does not exist in the DB (but should exist)
            throw new CatalogDBException("Path '" + parent + "' does not exist");
        }

        /*
            PERMISSION CHECK
         */
        Acl fileAcl;
        if (parentId < 0) { //If it hasn't got parent, take the StudyAcl
            fileAcl = getStudyAcl(userId, studyId);
        } else {
            fileAcl = getFileAcl(userId, parentId);
        }

        if (!fileAcl.isWrite()) {
            throw new CatalogDBException("Permission denied. Can't create files or folders in this study");
        }

        /*
            CHECK ALREADY EXISTS
         */
        if (catalogDBAdaptor.getFileId(studyId, folderPath.toString() + "/") >= 0) {
            throw new CatalogException("Cannot create directory ‘" + folderPath + "’: File exists");
        }

        /*
            PARENTS FOLDERS
         */
        while (parentId < 0 && parent != null) {  //Add all the parents that should be created
            folders.addFirst(new File(parent.getFileName().toString(), File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE,
                    parent.toString() + "/", userId, "", File.Status.READY, 0));
            parent = parent.getParent();
            if (parent != null) {
                parentId = catalogDBAdaptor.getFileId(studyId, parent.toString() + "/");
            }
        }

        ioManager.createFolder(getStudyUri(studyId), folderPath.toString(), parents);
        File mainFolder = new File(folderPath.getFileName().toString(), File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE,
                folderPath.toString() + "/", userId, "", File.Status.READY, 0);

        QueryResult<File> result;
        try {
            assert folders.size() == 0 && !parents || parents;
            for (File folder : folders) {
                catalogDBAdaptor.createFileToStudy(studyId, folder, options);
            }
            result = catalogDBAdaptor.createFileToStudy(studyId, mainFolder, options);
        } catch (CatalogDBException e) {
            ioManager.deleteFile(getStudyUri(studyId), folderPath.toString());
            throw e;
        }
        catalogDBAdaptor.updateUserLastActivity(ownerId);
        return result;
    }


    @Override
    public QueryResult deleteFolder(int folderId, String sessionId)
            throws CatalogException, IOException {
        return deleteFile(folderId, sessionId);
    }

    @Override
    public QueryResult deleteFile(int fileId, String sessionId)
            throws CatalogException, IOException {
        //Safe delete: Don't delete. Just rename file and set {deleting:true}
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = catalogDBAdaptor.getProjectOwnerId(projectId);

        if (!getFileAcl(userId, fileId).isDelete()) {
            throw new CatalogDBException("Permission denied. User can't delete this file");
        }
        QueryResult<File> fileResult = catalogDBAdaptor.getFile(fileId);
        if (fileResult.getResult().isEmpty()) {
            return new QueryResult("Delete file", 0, 0, 0, "File not found", null, Collections.emptyList());
        }
        File file = fileResult.getResult().get(0);
        switch (file.getStatus()) {
            case INDEXING:
            case UPLOADING:
            case UPLOADED:
                throw new CatalogException("File is not ready. {id: " + file.getId() + ", status: '" + file.getStatus() + "'}");
            case DELETING:
            case DELETED:
                //Send warning message
                return new QueryResult("Delete file", 0, 0, 0,
                        "File already deleted. {id: " + file.getId() + ", status: '" + file.getStatus() + "'}",
                        null, Collections.emptyList());
            case READY:
                break;
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
        objectMap.put("status", File.Status.DELETING);
        objectMap.put("attributes", new ObjectMap(File.DELETE_DATE, System.currentTimeMillis()));

        switch (file.getType()) {
            case FOLDER:
                QueryResult<File> allFilesInFolder = catalogDBAdaptor.getAllFilesInFolder(fileId, null);// delete recursively
                for (File subfile : allFilesInFolder.getResult()) {
                    deleteFile(subfile.getId(), sessionId);
                }

                renameFile(fileId, ".deleted_" + TimeUtils.getTime() + "_" +  file.getName(), sessionId);
                QueryResult queryResult = catalogDBAdaptor.modifyFile(fileId, objectMap);
                return queryResult;
            case FILE:
                renameFile(fileId, ".deleted_" + TimeUtils.getTime() + "_" +file.getName(), sessionId);
                return catalogDBAdaptor.modifyFile(fileId, objectMap);
            case INDEX:
                throw new CatalogException("Can't delete INDEX file");
                //renameFile(fileId, ".deleted_" + TimeUtils.getTime() + "_" + file.getName(), sessionId);
                //return catalogDBAdaptor.modifyFile(fileId, objectMap);
        }
        return null;
    }

    @Override
    public QueryResult moveFile(int fileId, int folderId, String sessionId) throws CatalogException {
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

    @Override
    public QueryResult renameFile(int fileId, String newName, String sessionId)
            throws CatalogException, IOException, CatalogIOManagerException {
        checkParameter(sessionId, "sessionId");
        checkPath(newName, "newName");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = catalogDBAdaptor.getProjectOwnerId(projectId);

        if (!getFileAcl(userId, fileId).isWrite()) {
            throw new CatalogDBException("Permission denied. User can't rename this file");
        }
        QueryResult<File> fileResult = catalogDBAdaptor.getFile(fileId);
        if (fileResult.getResult().isEmpty()) {
            return new QueryResult("Rename file", 0, 0, 0, "File not found", null, null);
        }
        File file = fileResult.getResult().get(0);
//        System.out.println("file = " + file);

        String oldPath = file.getPath();
        Path parent = Paths.get(oldPath).getParent();
        String newPath;
        if (parent == null) {
            newPath = newName;
        } else {
            newPath = parent.resolve(newName).toString();
        }

        catalogDBAdaptor.updateUserLastActivity(ownerId);
        QueryResult<Study> studyQueryResult;
        Study study;
        CatalogIOManager catalogIOManager;
        switch (file.getType()) {
            case FOLDER:
                studyQueryResult = catalogDBAdaptor.getStudy(studyId, null);   // TODO? check if something in the subtree is not READY?
                study = studyQueryResult.getResult().get(0);
                catalogIOManager = catalogIOManagerFactory.get(study.getUri());
                catalogIOManager.rename(getFileUri(study.getUri(), oldPath), getFileUri(study.getUri(), newPath));   // io.move() 1
                QueryResult queryResult = catalogDBAdaptor.renameFile(fileId, newPath);
                return queryResult;
            case FILE:
                studyQueryResult = catalogDBAdaptor.getStudy(studyId, null);
                study = studyQueryResult.getResult().get(0);
                catalogIOManager = catalogIOManagerFactory.get(study.getUri());
                catalogIOManager.rename(getFileUri(study.getUri(), file.getPath()), getFileUri(study.getUri(), newPath));
                return catalogDBAdaptor.renameFile(fileId, newPath);
            case INDEX:
                return catalogDBAdaptor.renameFile(fileId, newPath);
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
     * @throws org.opencb.opencga.catalog.db.CatalogDBException
     */
    @Override
    public QueryResult modifyFile(int fileId, ObjectMap parameters, String sessionId)
            throws CatalogException {
        checkObj(parameters, "Parameters");
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        File file = getFile(fileId, sessionId).getResult().get(0);
        switch (getUserRole(userId)) {
            case ADMIN:
                logger.info("UserAdmin " + userId + " modifies file {id: " + fileId + "}");
                break;
            default:
                if (!getFileAcl(userId, fileId).isWrite()) {
                    throw new CatalogException("User " + userId + " can't modify the file " + fileId);
                }
                for (String s : parameters.keySet()) {
                    switch (s) { //Special cases
                        //Can be modified anytime
                        case "format":
                        case "bioformat":
                        case "description":
                        case "status":
                        case "attributes":
                        case "stats":
                        case "sampleIds":
                            break;

                        //Can only be modified when file.status == INDEXING
                        case "jobId":
//                            if (!file.getStatus().equals(File.Status.INDEXING)) {
//                                throw new CatalogException("Parameter '" + s + "' can't be changed when " +
//                                        "status == " + file.getStatus().name() + ". " +
//                                        "Required status INDEXING or admin account");
//                            }
                            break;

                        //Can only be modified when file.status == UPLOADING
                        case "creationDate":
                        case "diskUsage":
                            if (!file.getStatus().equals(File.Status.UPLOADING)) {
                                throw new CatalogException("Parameter '" + s + "' can't be changed when " +
                                        "status == " + file.getStatus().name() + ". " +
                                        "Required status UPLOADING or admin account");
                            }
                            break;
                        //Path and Name must be changed with "raname" and/or "move" methods.
                        case "path":
                        case "name":
                            throw new CatalogException("Parameter '" + s + "' can't be changed directly. " +
                                    "Use \"renameFile\" instead");
                        case "type":
                        default:
                            throw new CatalogException("Parameter '" + s + "' can't be changed. " +
                                    "Requires admin account");
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

    @Override
    public QueryResult<File> getFileParent(int fileId, QueryOptions options, String sessionId)
            throws CatalogException {
        QueryResult<File> queryResult = getFile(fileId, null, sessionId);

        int studyId = getStudyIdByFileId(fileId);
        File file = queryResult.getResult().get(0);
        Path parent = Paths.get(file.getPath()).getParent();
        String parentPath;
        if (parent == null) {
            parentPath = "";
        } else {
            parentPath = parent.toString().endsWith("/") ? parent.toString() : parent.toString() + "/";
        }
        return searchFile(studyId, new QueryOptions("path", parentPath), sessionId);
    }

    @Override
    public QueryResult<File> getFile(int fileId, String sessionId)
            throws CatalogException {
        return getFile(fileId, null, sessionId);
    }

    @Override
    public QueryResult<File> getFile(int fileId, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!getFileAcl(userId, fileId).isRead()) {
            throw new CatalogException("Permission denied. User can't read file");
        }

        return catalogDBAdaptor.getFile(fileId, options);
    }

    @Override
    public QueryResult<File> getAllFiles(int studyId, QueryOptions options, String sessionId) throws CatalogException {
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl studyAcl = getStudyAcl(userId, studyId);
        if (!studyAcl.isRead()) {
            throw new CatalogException("Permission denied. User can't read file");
        }
        QueryResult<File> allFilesResult = catalogDBAdaptor.getAllFiles(studyId, options);
        List<File> files = allFilesResult.getResult();
        filterFiles(userId, studyAcl, files);
        allFilesResult.setResult(files);
        return allFilesResult;
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options, String sessionId) throws CatalogException {
        checkParameter(sessionId, "sessionId");
        checkId(folderId, "folderId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByFileId(folderId);
        Acl studyAcl = getStudyAcl(userId, studyId);
        if (!studyAcl.isRead()) {
            throw new CatalogDBException("Permission denied. User can't read file");
        }
        QueryResult<File> allFilesResult = catalogDBAdaptor.getAllFilesInFolder(folderId, options);
        List<File> files = allFilesResult.getResult();
        filterFiles(userId, studyAcl, files);
        allFilesResult.setResult(files);
        return allFilesResult;
    }

    @Override
    public DataInputStream downloadFile(int fileId, String sessionId)
            throws IOException, CatalogException {
        return downloadFile(fileId, -1, -1, sessionId);
    }

    @Override
    public DataInputStream downloadFile(int fileId, int start, int limit, String sessionId)    //TODO: start & limit does not work
            throws IOException, CatalogException {
        checkParameter(sessionId, "sessionId");


        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!getFileAcl(userId, fileId).isRead()) {
            throw new CatalogDBException("Permission denied. User can't download file");
        }
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        QueryResult<File> fileResult = catalogDBAdaptor.getFile(fileId);
        if (fileResult.getResult().isEmpty()) {
            throw new CatalogDBException("File not found");
        }
        File file = fileResult.getResult().get(0);

        return ioManager.getFileObject(getStudyUri(studyId),
                file.getPath(), start, limit);
    }

    @Override
    public DataInputStream grepFile(int fileId, String pattern, boolean ignoreCase, boolean multi, String sessionId)
            throws IOException, CatalogException {
        checkParameter(sessionId, "sessionId");


        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!getFileAcl(userId, fileId).isRead()) {
            throw new CatalogException("Permission denied. User can't download file");
        }
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
        QueryResult<File> fileResult = catalogDBAdaptor.getFile(fileId);
        if (fileResult.getResult().isEmpty()) {
            throw new CatalogException("File not found");
        }
        File file = fileResult.getResult().get(0);

        return ioManager.getGrepFileObject(getStudyUri(studyId),
                file.getPath(), pattern, ignoreCase, multi);
    }


    /**
     * TODO: Set per-file ACL
     */
    private QueryResult shareFile(int fileId, Acl acl, String sessionId)
            throws CatalogException {
        checkObj(acl, "acl");
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        Acl fileAcl = getFileAcl(userId, fileId);
        if (!fileAcl.isWrite()) {
            throw new CatalogDBException("Permission denied. Can't modify file");
        }

        return catalogDBAdaptor.setFileAcl(fileId, acl);
    }

    /*Require role admin*/
    @Override
    public QueryResult<File> searchFile(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        return searchFile(-1, query, options, sessionId);
    }

    @Override
    public QueryResult<File> searchFile(int studyId, QueryOptions query, String sessionId)
            throws CatalogException {
        return searchFile(studyId, query, null, sessionId);
    }

    @Override
    public QueryResult<File> searchFile(int studyId, QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        if (studyId < 0) {
            switch (getUserRole(userId)) {
                case ADMIN:
                    break;
                default:
                    throw new CatalogDBException("Permission denied. StudyId or Admin role required");
            }
        } else {
            if (!getStudyAcl(userId, studyId).isRead()) {
                throw new CatalogException("Permission denied. User " + userId + " can't read data from the study " + studyId);
            }
            query.put("studyId", studyId);
        }
        return catalogDBAdaptor.searchFile(query, options);
    }

    @Override
    public QueryResult<Dataset> createDataset(int studyId, String name, String description, List<Integer> files,
                                              Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        checkParameter(name, "name");
        checkObj(files, "files");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        description = defaultString(description, "");
        attributes = defaultObject(attributes, Collections.<String, Object>emptyMap());

        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. User " + userId + " can't modify the study " + studyId);
        }
        for (Integer fileId : files) {
            if (catalogDBAdaptor.getStudyIdByFileId(fileId) != studyId) {
                throw new CatalogException("Can't create a dataset with files from different files.");
            }
            if (!getFileAcl(userId, fileId).isRead()) {
                throw new CatalogException("Permission denied. User " + userId + " can't read the file " + fileId);
            }
        }

        Dataset dataset = new Dataset(-1, name, TimeUtils.getTime(), description, files, attributes);

        return catalogDBAdaptor.createDataset(studyId, dataset, options);
    }

    @Override
    public QueryResult<Dataset> getDataset(int dataSetId, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByDatasetId(dataSetId);

        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. User " + userId + " can't modify the study " + studyId);
        }

        return catalogDBAdaptor.getDataset(dataSetId, options);
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

    @Override
    public QueryResult refreshFolder(final int folderId, final String sessionId)
            throws CatalogDBException, IOException {

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

    /**
     * **************************
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
    @Override
    public int getStudyIdByJobId(int jobId) throws CatalogDBException {
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
    @Override
    public QueryResult<Job> createJob(int studyId, String name, String toolName, String description, String commandLine,
                                      URI tmpOutDirUri, int outDirId, List<Integer> inputFiles,
                                      Map<String, Object> resourceManagerAttributes, QueryOptions options, String sessionId) throws CatalogException {
        return createJob(studyId, name, toolName, description, commandLine, tmpOutDirUri, outDirId, inputFiles, resourceManagerAttributes, null, options, sessionId);
    }

    public QueryResult<Job> createJob(int studyId, String name, String toolName, String description, String commandLine,
                                      URI tmpOutDirUri, int outDirId, List<Integer> inputFiles,
                                      Map<String, Object> resourceManagerAttributes, Job.Status status, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        checkParameter(name, "name");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        checkParameter(toolName, "toolName");
        checkParameter(commandLine, "commandLine");
        description = defaultString(description, "");
        status = defaultObject(status, Job.Status.PREPARED);

        // FIXME check inputFiles? is a null conceptually valid?

//        URI tmpOutDirUri = createJobOutdir(studyId, randomString, sessionId);

        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. Can't create job");
        }
        QueryOptions fileQueryOptions = new QueryOptions("include", Arrays.asList("id", "type", "path"));
        File outDir = catalogDBAdaptor.getFile(outDirId, fileQueryOptions).getResult().get(0);

        if (!outDir.getType().equals(File.Type.FOLDER)) {
            throw new CatalogException("Bad outDir type. Required type : " + File.Type.FOLDER);
        }

        Job job = new Job(name, userId, toolName, description, commandLine, outDir.getId(), tmpOutDirUri, inputFiles);
        job.setStatus(status);
        if (resourceManagerAttributes != null) {
            job.getResourceManagerAttributes().putAll(resourceManagerAttributes);
        }

        return catalogDBAdaptor.createJob(studyId, job, options);
    }

    @Override
    public URI createJobOutDir(int studyId, String dirName, String sessionId)
            throws CatalogException, CatalogIOManagerException {
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

    @Override
    public QueryResult<ObjectMap> incJobVisites(int jobId, String sessionId) throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        int analysisId = catalogDBAdaptor.getStudyIdByJobId(jobId);
//        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
        int studyId = catalogDBAdaptor.getStudyIdByJobId(jobId);
        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. Can't read job");
        }
        return catalogDBAdaptor.incJobVisits(jobId);
    }

    @Override
    public QueryResult deleteJob(int jobId, String sessionId)
            throws CatalogException, CatalogIOManagerException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        int analysisId = catalogDBAdaptor.getStudyIdByJobId(jobId);
//        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
        int studyId = catalogDBAdaptor.getStudyIdByJobId(jobId);
        if (!getStudyAcl(userId, studyId).isDelete()) {
            throw new CatalogException("Permission denied. Can't delete job");
        }

        return catalogDBAdaptor.deleteJob(jobId);
    }


    @Override
    public QueryResult<Job> getJob(int jobId, QueryOptions options, String sessionId) throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        int analysisId = catalogDBAdaptor.getStudyIdByJobId(jobId);
//        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
        int studyId = catalogDBAdaptor.getStudyIdByJobId(jobId);
        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. Can't read job");
        }

        return catalogDBAdaptor.getJob(jobId, options);
    }

    @Override
    public QueryResult<Job> getUnfinishedJobs(String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        User.Role role = getUserRole(userId);
        switch (role) {
            case ADMIN:
                return catalogDBAdaptor.searchJob(new QueryOptions("ready", false));
            default:
                throw new CatalogException("Permission denied. Admin role required");
        }
    }


    @Override
    public QueryResult<Job> getAllJobs(int studyId, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. Can't get jobs");
        }
        return catalogDBAdaptor.getAllJobs(studyId, new QueryOptions());
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

    @Override
    public QueryResult modifyJob(int jobId, ObjectMap parameters, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);

//        User.Role role = getUserRole(userId);
//        switch (role) {
//            case ADMIN:
                return catalogDBAdaptor.modifyJob(jobId, parameters);
//            default:
//                throw new CatalogException("Permission denied. Admin role required");
//        }
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

    /**
     * Samples methods
     * ***************************
     */

    @Override
    public QueryResult<Sample> createSample(int studyId, String name, String source, String description,
                                            Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        checkParameter(name, "name");
        source = defaultString(source, "");
        description = defaultString(description, "");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. User " + userId + " can't modify study");
        }
        Sample sample = new Sample(-1, name, source, null, description, Collections.<AnnotationSet>emptyList(),
                attributes);

        return catalogDBAdaptor.createSample(studyId, sample, options);
    }

    @Override
    public QueryResult<Sample> getSample(int sampleId, QueryOptions options, String sessionId) throws CatalogException {
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdBySampleId(sampleId);

        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. User " + userId + " can't read study");
        }

        return catalogDBAdaptor.getSample(sampleId, options);
    }

    @Override
    public QueryResult<Sample> getAllSamples(int studyId, QueryOptions options, String sessionId) throws CatalogException {
        checkParameter(sessionId, "sessionId");

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);

        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. User " + userId + " can't read study");
        }

        return catalogDBAdaptor.getAllSamples(studyId, options);
    }

    @Override
    public QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique,
                                                      String description, Map<String, Object> attributes,
                                                      List<Variable> variables, String sessionId)
            throws CatalogException {

        checkObj(variables, "Variables List");
        Set<Variable> variablesSet = new HashSet<>(variables);
        if (variables.size() != variablesSet.size()) {
            throw new CatalogException("Error. Repeated variables");
        }
        return createVariableSet(studyId, name, unique, description, attributes, variablesSet, sessionId);
    }

    @Override
    public QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique,
                                                      String description, Map<String, Object> attributes,
                                                      Set<Variable> variables, String sessionId)
            throws CatalogException {
        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        checkParameter(sessionId, "sessionId");
        checkParameter(name, "name");
        checkObj(variables, "Variables Set");
        unique = defaultObject(unique, true);
        description = defaultString(description, "");
        attributes = defaultObject(attributes, new HashMap<String, Object>());

        for (Variable variable : variables) {
            checkParameter(variable.getId(), "variable ID");
            checkObj(variable.getType(), "variable Type");
            variable.setAllowedValues(defaultObject(variable.getAllowedValues(), Collections.<String>emptyList()));
            variable.setAttributes(defaultObject(variable.getAttributes(), Collections.<String, Object>emptyMap()));
            variable.setCategory(defaultString(variable.getCategory(), ""));
            variable.setDependsOn(defaultString(variable.getDependsOn(), ""));
            variable.setDescription(defaultString(variable.getDescription(), ""));
//            variable.setRank(defaultString(variable.getDescription(), ""));
        }

        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. User " + userId + " can't modify study");
        }

        VariableSet variableSet = new VariableSet(-1, name, unique, description, variables, attributes);
        CatalogSampleAnnotationsValidator.checkVariableSet(variableSet);

        return catalogDBAdaptor.createVariableSet(studyId, variableSet);
    }

    @Override
    public QueryResult<VariableSet> getVariableSet(int variableSet, QueryOptions options, String sessionId)
            throws CatalogException {

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdByVariableSetId(variableSet);
        if (!getStudyAcl(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. User " + userId + " can't read study");
        }

        return catalogDBAdaptor.getVariableSet(variableSet, options);
    }


    @Override
    public QueryResult<AnnotationSet> annotateSample(int sampleId, String id, int variableSetId,
                                                     Map<String, Object> annotations,
                                                     Map<String, Object> attributes,
                                                     String sessionId) throws CatalogException {
        return annotateSample(sampleId, id, variableSetId, annotations, attributes, true, sessionId);
    }

    /* package */ QueryResult<AnnotationSet> annotateSample(int sampleId, String id, int variableSetId,
                                                            Map<String, Object> annotations,
                                                            Map<String, Object> attributes,
                                                            boolean checkAnnotationSet,
                                                            String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        checkParameter(id, "id");
        checkObj(annotations, "annotations");
        attributes = defaultObject(attributes, new HashMap<String, Object>());

        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = catalogDBAdaptor.getStudyIdBySampleId(sampleId);
        if (!getStudyAcl(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. User " + userId + " can't modify study");
        }

        QueryResult<VariableSet> variableSetResult = catalogDBAdaptor.getVariableSet(variableSetId, null);
        if (variableSetResult.getResult().isEmpty()) {
            throw new CatalogException("VariableSet " + variableSetId + " does not exists");
        }
        VariableSet variableSet = variableSetResult.getResult().get(0);

        AnnotationSet annotationSet = new AnnotationSet(id, variableSetId, new HashSet<Annotation>(), TimeUtils.getTime(), attributes);

        for (Map.Entry<String, Object> entry : annotations.entrySet()) {
            annotationSet.getAnnotations().add(new Annotation(entry.getKey(), entry.getValue()));
        }
        QueryResult<Sample> sampleQueryResult = catalogDBAdaptor.getSample(sampleId,
                new QueryOptions("include", Collections.singletonList("annotationSets")));

        List<AnnotationSet> annotationSets = sampleQueryResult.getResult().get(0).getAnnotationSets();
        if (checkAnnotationSet) {
            CatalogSampleAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);
        }

        return catalogDBAdaptor.annotateSample(sampleId, annotationSet);
    }

    /**
     * Cohort methods
     * ***************************
     */

    public int getStudyIdByCohortId(int cohortId) throws CatalogException {
        return catalogDBAdaptor.getStudyIdByCohortId(cohortId);
    }

    public QueryResult<Cohort> getCohort(int cohortId, QueryOptions options, String sessionId) throws CatalogException {
        checkParameter(sessionId, "sessionId");

        int studyId = catalogDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = getUserIdBySessionId(sessionId);

        if (getStudyAcl(userId, studyId).isRead()) {
            return catalogDBAdaptor.getCohort(cohortId);
        } else {
            throw new CatalogException("Permission denied. User " + userId + " can't read cohorts from study");
        }
    }

    public QueryResult<Cohort> createCohort(int studyId, String name, String description, List<Integer> sampleIds,
                                            Map<String, Object> attributes, String sessionId) throws CatalogException {
        checkParameter(name, "name");
        checkObj(sampleIds, "Samples list");
        description = defaultString(description, "");
        attributes = defaultObject(attributes, Collections.<String, Object>emptyMap());

        if (getAllSamples(studyId, new QueryOptions("id", sampleIds), sessionId).getResult().size() != sampleIds.size()) {
//            for (Integer sampleId : samples) {
//                getSample(sampleId, new QueryOptions("include", "id"), sessionId).first();
//            }
            throw new CatalogException("Error: Some sampleId does not exist in the study " + studyId);
        }

        Cohort cohort = new Cohort(name, TimeUtils.getTime(), description, sampleIds, attributes);

        return catalogDBAdaptor.createCohort(studyId, cohort);
    }


    /**
     * Tools methods
     * ***************************
     */

    @Override
    public QueryResult<Tool> createTool(String alias, String description, Object manifest, Object result,
                                        String path, boolean openTool, String sessionId) throws CatalogException {
        checkParameter(alias, "alias");
        checkObj(description, "description"); //description can be empty
        checkParameter(path, "path");
        checkParameter(sessionId, "sessionId");
        //TODO: Check Path

        String userId = getUserIdBySessionId(sessionId);

        List<Acl> acl = Arrays.asList(new Acl(userId, true, true, true, true));
        if (openTool) {
            acl.add(new Acl(Acl.USER_OTHERS_ID, true, false, true, false));
        }

        String name = Paths.get(path).getFileName().toString();

        Tool tool = new Tool(-1, alias, name, description, manifest, result, path, acl);

        return catalogDBAdaptor.createTool(userId, tool);
    }

    @Override
    public QueryResult<Tool> getTool(int id, String sessionId) throws CatalogException {
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
    private void checkEmail(String email) throws CatalogException {
        if (email == null || !emailPattern.matcher(email).matches()) {
            throw new CatalogException("email not valid");
        }
    }

    private void checkId(int id, String name) throws CatalogException {
        if (id < 0) {
            throw new CatalogException("Error in id: '" + name + "' is not valid: "
                    + id + ".");
        }
    }

    private void checkParameter(String param, String name) throws CatalogException {
        if (param == null || param.equals("") || param.equals("null")) {
            throw new CatalogException("Error in parameter: parameter '" + name + "' is null or empty: "
                    + param + ".");
        }
    }

    private void checkParameters(String... args) throws CatalogException {
        if (args.length % 2 == 0) {
            for (int i = 0; i < args.length; i += 2) {
                checkParameter(args[i], args[i + 1]);
            }
        } else {
            throw new CatalogException("Error in parameter: parameter list is not multiple of 2");
        }
    }

    private void checkObj(Object obj, String name) throws CatalogException {
        if (obj == null) {
            throw new CatalogException("parameter '" + name + "' is null.");
        }
    }

    private void checkRegion(String regionStr, String name) throws CatalogException {
        if (Pattern.matches("^([a-zA-Z0-9])+:([0-9])+-([0-9])+$", regionStr)) {//chr:start-end
            throw new CatalogException("region '" + name + "' is not valid");
        }
    }

    private void checkSessionId(String userId, String sessionId) throws CatalogException {
        String userIdBySessionId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
        if (!userIdBySessionId.equals(userId)) {
            throw new CatalogException("Invalid sessionId for user: " + userId);
        }
    }

    private void checkPath(String path, String name) throws CatalogException {
        if (path == null) {
            throw new CatalogException("parameter '" + name + "' is null.");
        }
        checkPath(Paths.get(path), name);
    }

    private void checkPath(Path path, String name) throws CatalogException {
        checkObj(path, name);
        if (path.isAbsolute()) {
            throw new CatalogException("Error in path: Path '" + name + "' can't be absolute");
        } else if (path.toString().matches("\\.|\\.\\.")) {
            throw new CatalogException("Error in path: Path '" + name + "' can't have relative names '.' or '..'");
        }
    }

    private void checkAlias(String alias, String name) throws CatalogException {
        if (alias == null || alias.isEmpty() || !alias.matches("^[_A-Za-z0-9-\\+]+$")) {
            throw new CatalogException("Error in alias: Invalid alias for '" + name + "'.");
        }
    }

    private String defaultString(String string, String defaultValue) {
        if (string == null || string.isEmpty()) {
            string = defaultValue;
        }
        return string;
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
     * @param userId   UserId
     * @param projects Projects list
     * @throws org.opencb.opencga.catalog.db.CatalogDBException
     */
    private void filterProjects(String userId, List<Project> projects) throws CatalogDBException {
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
     * @param userId     UserId
     * @param projectAcl Project ACL
     * @param studies    Studies list
     * @throws org.opencb.opencga.catalog.db.CatalogDBException
     */
    private void filterStudies(String userId, Acl projectAcl, List<Study> studies) throws CatalogDBException {
        Iterator<Study> studyIt = studies.iterator();
        while (studyIt.hasNext()) {
            Study s = studyIt.next();
            Acl studyAcl = getStudyAcl(userId, s.getId(), projectAcl);
            if (!studyAcl.isRead()) {
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
     * @param userId   UserId
     * @param studyAcl Study ACL
     * @param files    Files list
     * @throws org.opencb.opencga.catalog.db.CatalogDBException
     */
    private void filterFiles(String userId, Acl studyAcl, List<File> files) throws CatalogDBException {
        if (files == null || files.isEmpty()) {
            return;
        }
        Iterator<File> fileIt = files.iterator();
        while (fileIt.hasNext()) {
            File f = fileIt.next();
            Acl fileAcl = getFileAcl(userId, f.getId(), studyAcl);
            if (!fileAcl.isRead()) {
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

    private User.Role getUserRole(String userId) throws CatalogDBException {
        return catalogDBAdaptor.getUser(userId, new QueryOptions("include", Arrays.asList("role")), null).getResult().get(0).getRole();
    }

    private Acl getProjectAcl(String userId, int projectId) throws CatalogDBException {
        Acl projectAcl;
        if (getUserRole(userId).equals(User.Role.ADMIN)) {
            return new Acl(userId, true, true, true, true);
        }
        boolean sameOwner = catalogDBAdaptor.getProjectOwnerId(projectId).equals(userId);

        if (sameOwner) {
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

    private Acl getStudyAcl(String userId, int studyId) throws CatalogDBException {
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
        return getStudyAcl(userId, studyId, getProjectAcl(userId, projectId));
    }

    private Acl getStudyAcl(String userId, int studyId, Acl projectAcl) throws CatalogDBException {
        Acl studyAcl;
        if (getUserRole(userId).equals(User.Role.ADMIN)) {
            return new Acl(userId, true, true, true, true);
        }
        boolean sameOwner = catalogDBAdaptor.getStudyOwnerId(studyId).equals(userId);

        if (sameOwner) {
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

    private Acl getFileAcl(String userId, int fileId) throws CatalogDBException {
        if (getUserRole(userId).equals(User.Role.ADMIN)) {
            return new Acl(userId, true, true, true, true);
        }
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        return getStudyAcl(userId, studyId);
    }

    /**
     * Use StudyACL for all files.
     */
    private Acl getFileAcl(String userId, int fileId, Acl studyAcl) throws CatalogDBException {
        return studyAcl;
    }

    //TODO: Check folder ACLs
    private Acl __getFileAcl(String userId, int fileId, Acl studyAcl) throws CatalogDBException {
        Acl fileAcl;
        boolean sameOwner = catalogDBAdaptor.getFileOwnerId(fileId).equals(userId);

        if (sameOwner) {
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
