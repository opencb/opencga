package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.ParameterException;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.CatalogManager;
import org.opencb.opencga.catalog.core.beans.*;
import org.opencb.opencga.catalog.core.db.CatalogManagerException;
import org.opencb.opencga.catalog.core.io.CatalogIOManagerException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by jacobo on 29/09/14.
 */
public class Main {
    private static String userId;
    private static String sessionId;
    private static CatalogManager catalogManager;
    //    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws CatalogManagerException, IOException, CatalogIOManagerException, InterruptedException {
        OptionsParser optionsParser = new OptionsParser();
        //args = "projects share -u user -p asdf -U user2 -r true -d true -w true -x true --id 1".split(" "); System.out.println("HARCODE ARGS!");
        try {
            optionsParser.parse(args);
        } catch (ParameterException e){
            if(!optionsParser.generalOptions.help) {
                System.out.println(e.getMessage());
            }
            optionsParser.printUsage();
            System.exit(1);
        }
        if(optionsParser.generalOptions.help) {
            optionsParser.printUsage();
            System.exit(1);
        }

        catalogManager = getCatalogManager();


//        optionsParser.parseCommand("user create -u jcoll --name jacobo --organization EBI --email mimail".split(" "));
//        logger.info(optionsParser.getCommand());
//        logger.info(optionsParser.getSubcommand());
//        System.out.println(optionsParser.getCommand());
//        System.out.println(optionsParser.getSubcommand());

        switch (optionsParser.getCommand()) {
            case "users":
                switch (optionsParser.getSubcommand()) {
                    case "create": {
                        OptionsParser.CommandUser.CommandUserCreate c = optionsParser.commandUserCreate;
                        //QueryResult<User> user = catalogManager.createUser(new User(c.up.user, c.name, c.email, c.up.password, c.organization, User.ROLE_USER, ""));
                        QueryResult<User> user = catalogManager.createUser(c.up.user, c.name, c.email, c.up.password, c.organization);
                        System.out.println(user);
                        break;
                    }
                    case "info": {
                        OptionsParser.CommandUser.CommandUserInfo c = optionsParser.commandUserInfo;
                        login(c.up);

                        QueryResult<User> user = catalogManager.getUser(userId, null, sessionId);
                        System.out.println(user);

                        logout();
                        break;
                    }
                    default:
                        optionsParser.printUsage();
                        break;
                }
                break;
            case "projects":
                switch (optionsParser.getSubcommand()) {
                    case "create": {
                        OptionsParser.CommandProject.CommandProjectCreate c = optionsParser.commandProjectCreate;
                        login(c.up);

                        QueryResult<Project> project = catalogManager.createProject(userId, c.name, c.alias, c.description, c.organization, sessionId);
                        System.out.println(project);

                        logout();
                        break;
                    }
                    case "info": {
                        OptionsParser.CommandProject.CommandProjectInfo c = optionsParser.commandProjectInfo;
                        login(c.up);

                        int projectId = catalogManager.getProjectId(c.id);
                        QueryResult<Project> project = catalogManager.getProject(projectId, sessionId);
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
                switch (optionsParser.getSubcommand()) {
                    case "create": {
                        OptionsParser.CommandStudy.CommandStudyCreate c = optionsParser.commandStudyCreate;
                        login(c.up);

                        int projectId = catalogManager.getProjectId(c.projectId);
                        QueryResult<Study> study = catalogManager.createStudy(projectId, c.name, c.alias, c.type, c.description, sessionId);
                        System.out.println(study);

                        logout();
                        break;
                    }
                    case "info": {
                        OptionsParser.CommandStudy.CommandStudyInfo c = optionsParser.commandStudyInfo;
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
            case "files":
                switch (optionsParser.getSubcommand()) {
                    case "create": {
                        OptionsParser.CommandFile.CommandFileCreate c = optionsParser.commandFileCreate;
                        login(c.up);

                        int studyId = catalogManager.getStudyId(c.studyId);
                        Path inputFile = Paths.get(c.file);
                        InputStream is = new FileInputStream(inputFile.toFile());
                        //String outPath = Paths.get(c.path , inputFile.getFileName().toString()).toString();
                        QueryResult<File> file = catalogManager.uploadFile(studyId, c.format, c.bioformat,
                                Paths.get(c.path, inputFile.getFileName().toString()).toString(), c.description,
                                c.parents, is, sessionId);
                        System.out.println(file);

                        logout();
                        break;
                    }
                    case "info": {
                        OptionsParser.CommandFile.CommandFileInfo c = optionsParser.commandFileInfo;
                        login(c.up);

                        int fileId = catalogManager.getFileId(c.id);
                        QueryResult<File> file = catalogManager.getFile(fileId, sessionId);
                        System.out.println(file);

                        logout();
                        break;
                    }
                    default:
                        optionsParser.printUsage();
                        break;
                }
                break;
            default:
                optionsParser.printUsage();
//                logger.info("Unknown command");
                break;
        }
    }

    private static void login(OptionsParser.UserPassword up) throws CatalogManagerException, IOException {
        userId = up.user;
        QueryResult<ObjectMap> login = catalogManager.login(userId, up.password, "localhost");
        sessionId = login.getResult().get(0).getString("sessionId");
    }

    private static void logout() throws CatalogManagerException, IOException {
        catalogManager.logout(userId, sessionId);
        sessionId = null;
    }

    private static CatalogManager getCatalogManager() throws IOException, CatalogIOManagerException, CatalogManagerException {
        CatalogManager catalogManager;InputStream is = Main.class.getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        //try {
            properties.load(is);
//            System.out.println("catalog.properties");
//            System.out.println("HOST "+properties.getProperty("HOST"));
//            System.out.println("PORT "+properties.getProperty("PORT"));
//            System.out.println("DATABASE "+properties.getProperty("DATABASE"));
//            System.out.println("USER "+properties.getProperty("USER"));
//            System.out.println("PASS "+properties.getProperty("PASSWORD"));
//            System.out.println("ROOTDIR "+properties.getProperty("ROOTDIR"));
//        } catch (IOException e) {
//            System.out.println("Error loading properties");
//            System.out.println(e.getMessage());
//            e.printStackTrace();
//        }

        catalogManager = new CatalogManager(properties);
//        try {
//            catalogManager = new CatalogManager(properties);
//        } catch (IOException e) {
//            System.out.println(e.getMessage());
//            e.printStackTrace();
//        } catch (CatalogIOManagerException e) {
//            System.out.println(e.getMessage());
//            e.printStackTrace();
//        } catch (CatalogManagerException e) {
//            e.printStackTrace();
//        }
        return catalogManager;
    }

}
