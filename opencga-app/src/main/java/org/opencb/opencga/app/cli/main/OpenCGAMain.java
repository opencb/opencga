package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.ParameterException;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogFileManager;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by jacobo on 29/09/14.
 */
public class OpenCGAMain {

    private String userId;
    private static String shellUserId;
    private static String shellSessionId;
    private CatalogManager catalogManager;
    public String sessionId;
    private static boolean interactive;

    //    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args)
            throws CatalogException, IOException, InterruptedException, IllegalOpenCGACredentialsException, AnalysisExecutionException, URISyntaxException {

        OpenCGAMain opencgaMain = new OpenCGAMain();

        OptionsParser optionsParser = new OptionsParser(false);
        try {
            optionsParser.parse(args);
        } catch (ParameterException e){

            if(!optionsParser.getGeneralOptions().help && !optionsParser.getCommonOptions().help ) {
                System.out.println(e.getMessage());
            }

            optionsParser.printUsage();
            System.exit(1);
        }

        // Interactive mode
        interactive = optionsParser.getGeneralOptions().interactive;
        if(interactive){
            BufferedReader reader;//create BufferedReader object

            reader = new BufferedReader(new InputStreamReader(System.in));

            do {
                if(shellUserId != null && !shellUserId.isEmpty()){
                    System.out.print("(" + shellUserId + "," + shellSessionId + ")> ");
                } else {
                    System.out.print("> ");
                }
                String line = reader.readLine();
                if (line != null) {
                    args = line.trim().split("[ \\t\\n]+");
                } else {
                    break;
                }

                if (args.length != 0 && !args[0].isEmpty()) {
                    try {
                        opencgaMain.runCommand(args);
                    } catch (Exception e) {
                        //e.printStackTrace();
                        System.out.println(e.getMessage());
                    }
                }
            } while(!args[0].equals("exit"));
            System.out.println("bye");
        } else {
            System.exit(opencgaMain.runCommand(args));
        }
    }

    private int runCommand(String[] args) throws CatalogException, IOException, InterruptedException, IllegalOpenCGACredentialsException, AnalysisExecutionException, URISyntaxException {
        OptionsParser optionsParser = new OptionsParser(interactive);
        try {
            optionsParser.parse(args);
        } catch (ParameterException e){

            if(!optionsParser.getGeneralOptions().help && !optionsParser.getCommonOptions().help ) {
                System.out.println(e.getMessage());
            }

            optionsParser.printUsage();
            return 1;
        }
        if(optionsParser.getCommonOptions().help) {
            optionsParser.printUsage();
            return 1;
        }

        if(catalogManager == null && !optionsParser.getSubCommand().isEmpty() ) {
            catalogManager = getCatalogManager();
        }


        switch (optionsParser.getCommand()) {
            case "users":
                switch (optionsParser.getSubCommand()) {
                    case "create": {
                        OptionsParser.UserCommands.CreateCommand c = optionsParser.getUserCommands().createCommand;
                        //QueryResult<User> user = catalogManager.insertUser(new User(c.up.user, c.name, c.email, c.up.password, c.organization, User.Role.USER, ""));
                        QueryResult<User> user = catalogManager.createUser(c.up.user, c.name, c.email, c.up.password, c.organization, null);
                        System.out.println(user);
                        break;
                    }
                    case "info": {
                        OptionsParser.UserCommands.InfoCommand c = optionsParser.getUserCommands().infoCommand;
                        login(c.up);

                        QueryResult<User> user = catalogManager.getUser(c.up.user != null ? c.up.user : userId, null, sessionId);
                        System.out.println(user);

                        logout();
                        break;
                    }
                    case "list": {
                        OptionsParser.UserCommands.ListCommand c = optionsParser.getUserCommands().lsitCommand;
                        login(c.up);

                        String ident = "";
                        User user = catalogManager.getUser(c.up.user != null ? c.up.user : userId, null,
                                new QueryOptions("include", Arrays.asList("id", "name", "projects.id","projects.alias","projects.name")), sessionId).first();
                        System.out.println(user.getId() + " - " + user.getName());
                        ident+= "\t";
                        boolean studies = c.studies;
                        boolean files = c.files;
                        listProjects(user.getProjects(), studies, files, ident);

                        logout();
                        break;
                    }
                    case "login": {
                        OptionsParser.UserCommands.LoginCommand c = optionsParser.getUserCommands().loginCommand;

                        if (shellSessionId == null || shellUserId == null || !shellUserId.equals(c.up.user)) {
                            shellSessionId = login(c.up);
                            if (shellSessionId != null) {
                                shellUserId = c.up.user;
                            }
                            System.out.println(shellSessionId);
                        }
                        break;
                    }
                    case "logout": {
                        OptionsParser.UserCommands.LogoutCommand c = optionsParser.getUserCommands().logoutCommand;

                        QueryResult logout;
                        if (c.user == null && c.sessionId == null) {
                            logout = catalogManager.logout(shellUserId, shellSessionId);
                            shellUserId = null;
                            shellSessionId = null;
                        } else {
                            logout = catalogManager.logout(c.user, c.sessionId);
                        }
                        System.out.println(logout);

                        break;
                    }
                    default:
                        optionsParser.printUsage();
                        break;
                }
                break;
            case "projects":
                switch (optionsParser.getSubCommand()) {
                    case "create": {
                        OptionsParser.ProjectCommands.CreateCommand c = optionsParser.getProjectCommands().createCommand;
                        login(c.up);

                        QueryResult<Project> project = catalogManager.createProject(userId, c.name, c.alias, c.description, c.organization, null, sessionId);
                        System.out.println(project);

                        logout();
                        break;
                    }
                    case "info": {
                        OptionsParser.ProjectCommands.InfoCommand c = optionsParser.getProjectCommands().infoCommand;
                        login(c.up);

                        int projectId = catalogManager.getProjectId(c.id);
                        QueryResult<Project> project = catalogManager.getProject(projectId, null, sessionId);
                        System.out.println(project);

                        logout();
                        break;
                    }
                    case "share": {
                        OptionsParser.CommandShareResource c = optionsParser.commandShareResource;
                        login(c.up);

                        int projectId = catalogManager.getProjectId(c.id);
                        QueryResult result = catalogManager.shareProject(projectId, new Acl(c.user, c.read, c.write, c.execute, c.delete), sessionId);
                        System.out.println(result);

                        logout();
                        break;
                    }
                    default:
                        optionsParser.printUsage();
                        break;
                }
                break;
            case "studies":
                switch (optionsParser.getSubCommand()) {
                    case "create": {
                        OptionsParser.StudyCommands.CreateCommand c = optionsParser.getStudyCommands().createCommand;
                        login(c.up);

                        int projectId = catalogManager.getProjectId(c.projectId);
                        QueryResult<Study> study = catalogManager.createStudy(projectId, c.name, c.alias, Study.Type.valueOf(c.type), c.description, sessionId);
                        System.out.println(study);

                        logout();
                        break;
                    }
                    case "info": {
                        OptionsParser.StudyCommands.InfoCommand c = optionsParser.getStudyCommands().infoCommand;
                        login(c.up);

                        int studyId = catalogManager.getStudyId(c.id);
                        QueryResult<Study> study = catalogManager.getStudy(studyId, sessionId);
                        System.out.println(study);

                        logout();
                        break;
                    }
                    case "share": {
                        OptionsParser.CommandShareResource c = optionsParser.commandShareResource;
                        login(c.up);

                        int studyId = catalogManager.getStudyId(c.id);
                        QueryResult result = catalogManager.shareProject(studyId, new Acl(c.user, c.read, c.write, c.execute, c.delete), sessionId);
                        System.out.println(result);

                        logout();
                        break;
                    }
                    default:
                        optionsParser.printUsage();
                        break;
                }
                break;
            case "files": {
                switch (optionsParser.getSubCommand()) {
                    case "create": {
                        OptionsParser.FileCommands.CreateCommand c = optionsParser.getFileCommands().createCommand;
                        login(c.up);

                        int studyId = catalogManager.getStudyId(c.studyId);
                        Path inputFile = Paths.get(c.file);
                        URI sourceUri = new URI(null, c.file, null);
                        //String outPath = Paths.get(c.outdir , inputFile.getFileName().toString()).toString();
//                        QueryResult<File> file = catalogManager.uploadFile(studyId, File.Format.valueOf(c.format), File.Bioformat.valueOf(c.bioformat),
//                                Paths.get(c.path, inputFile.getFileName().toString()).toString(), c.description,
//                                c.parents, is, sessionId);
                        QueryResult<File> file = catalogManager.createFile(studyId, c.format, c.bioformat,
                                Paths.get(c.path, inputFile.getFileName().toString()).toString(), c.description,
                                c.parents, -1, sessionId);
                        new CatalogFileManager(catalogManager).upload(sourceUri, file.first(), null, sessionId, false, false, false, c.calculateChecksum);
                        System.out.println(file);

                        logout();
                        break;
                    }
                    case "create-folder": {
                        OptionsParser.FileCommands.CreateFolderCommand c = optionsParser.getFileCommands().createFolderCommand;
                        login(c.up);

                        int studyId = catalogManager.getStudyId(c.studyId);
                        QueryResult<File> folder = catalogManager.createFolder(studyId, Paths.get(c.path), c.parents, null, sessionId);
                        System.out.println(folder);

                        logout();
                        break;
                    }
                    case "info": {
                        OptionsParser.FileCommands.InfoCommand c = optionsParser.getFileCommands().infoCommand;
                        login(c.up);

                        int fileId = catalogManager.getFileId(c.id);
                        QueryResult<File> file = catalogManager.getFile(fileId, sessionId);
                        System.out.println(file);

                        logout();
                        break;
                    }
                    case "list": {
                        OptionsParser.FileCommands.ListCommand c = optionsParser.getFileCommands().listCommand;
                        login(c.up);

                        int fileId = catalogManager.getFileId(c.id);
                        QueryResult<File> file = catalogManager.getAllFilesInFolder(fileId, null, sessionId);
                        System.out.println(file);

                        logout();
                        break;
                    }
                    case "index": {
                        OptionsParser.FileCommands.IndexCommand c = optionsParser.getFileCommands().indexCommand;
                        login(c.up);

                        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);

                        int fileId = catalogManager.getFileId(c.id);
                        int outdirId = catalogManager.getFileId(c.outdir);
                        String storageEngine = c.storageEngine != null? c.storageEngine : StorageManagerFactory.getDefaultStorageManagerName();
                        File index = analysisFileIndexer.index(fileId, outdirId, storageEngine, sessionId, new QueryOptions("launch", true));
                        System.out.println(index);

                        logout();
                        break;
                    }
                    default:
                        optionsParser.printUsage();
                        break;
                }
                break;
            }
            case "tools" : {
                switch (optionsParser.getSubCommand()) {
                    case "create": {
                        OptionsParser.ToolCommands.CreateCommand c = optionsParser.getToolCommands().createCommand;
                        login(c.up);

                        QueryResult<Tool> tool = catalogManager.createTool(c.alias, c.description, null, null,
                                c.path, c.openTool, sessionId);
                        System.out.println(tool);

                        break;
                    }
                    case "info": {
                        OptionsParser.ToolCommands.InfoCommand c = optionsParser.getToolCommands().infoCommand;
                        login(c.up);

                        QueryResult<Tool> tool = catalogManager.getTool(c.id, sessionId);
                        System.out.println(tool);

                        break;
                    }
                    default: {
                        optionsParser.printUsage();
                        break;
                    }
                }
                break;
            }
            case "exit": {
            }
            break;
            case "help":
            default:
                optionsParser.printUsage();
//                logger.info("Unknown command");
                break;
        }
        return 0;
    }

    private void listProjects(List<Project> projects, boolean studies, boolean files, String ident) throws CatalogException {
        for (Project project : projects) {
            System.out.printf("%s%d - %s (%s)\n", ident, project.getId(), project.getName(), project.getAlias());

            if (studies || files) {
                listStudies(project.getId(), files, ident + "\t");
            }

        }
    }

    private void listStudies(int projectId, boolean files, String ident) throws CatalogException {
        List<Study> studies = catalogManager.getAllStudies(projectId,
                new QueryOptions("include", Arrays.asList("projects.studies.id", "projects.studies.name", "projects.studies.alias")),
                sessionId).getResult();

        for (Study study : studies) {
            System.out.printf("%s%d - %s (%s)\n", ident,  study.getId(), study.getName(), study.getAlias());
            if (files) {
                listFiles(study.getId(), ".", ident + "\t");
            }
        }
    }

    private void listFiles(int studyId, String path, String ident) throws CatalogException {
        List<File> files = catalogManager.searchFile(studyId, new QueryOptions("directory", path), sessionId).getResult();
        for (File file : files) {
            System.out.printf("%s%d - %s \t\t[%s]\n", ident, file.getId(), file.getName(), file.getStatus());
            if (file.getType() == File.Type.FOLDER) {
                listFiles(studyId, file.getPath(), ident + "\t");
            }
        }
    }

    private String login(OptionsParser.UserAndPasswordOptions up) throws CatalogException, IOException {
        //String sessionId;
        if(up.user != null && up.password != null) {
            QueryResult<ObjectMap> login = catalogManager.login(up.user, up.password, "localhost");
            sessionId = login.getResult().get(0).getString("sessionId");
            userId = up.user;
        } else {
            sessionId = shellSessionId;
        }
        return sessionId;

    }

    private void logout() throws CatalogException, IOException {
        if(sessionId != null && !sessionId.equals(shellSessionId)){
            catalogManager.logout(userId, sessionId);
        }
    }

    private static CatalogManager getCatalogManager()
            throws IOException, CatalogIOManagerException, CatalogDBException, IllegalOpenCGACredentialsException {
        CatalogManager catalogManager;
        String appHome = System.getProperty("app.home");
        Config.setGcsaHome(appHome);
        Properties catalogProperties = Config.getCatalogProperties();
        catalogManager = new CatalogManager(catalogProperties);

////        InputStream is = OpenCGAMain.class.getClassLoader().getResourceAsStream("catalog.properties");
//        Properties properties = new Properties();
//        //try {
////            properties.load(is);
//            properties.load(new FileInputStream(Paths.get(System.getProperty("app.home"), "conf", "catalog.properties").toFile()));
////            System.out.println("catalog.properties");
////            System.out.println("HOST "+properties.getProperty("HOST"));
////            System.out.println("PORT "+properties.getProperty("PORT"));
////            System.out.println("DATABASE "+properties.getProperty("DATABASE"));
////            System.out.println("USER "+properties.getProperty("USER"));
////            System.out.println("PASS "+properties.getProperty("PASSWORD"));
////            System.out.println("ROOTDIR "+properties.getProperty("ROOTDIR"));
////        } catch (IOException e) {
////            System.out.println("Error loading properties");
////            System.out.println(e.getMessage());
////            e.printStackTrace();
////        }
//
//        catalogManager = new CatalogManager(properties);
////        try {
////            catalogManager = new CatalogManager(properties);
////        } catch (IOException e) {
////            System.out.println(e.getMessage());
////            e.printStackTrace();
////        } catch (CatalogIOManagerException e) {
////            System.out.println(e.getMessage());
////            e.printStackTrace();
////        } catch (CatalogManagerException e) {
////            e.printStackTrace();
////        }
        return catalogManager;
    }

}
