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

package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.ToolManager;
import org.opencb.opencga.catalog.models.tool.Execution;
import org.opencb.opencga.catalog.models.tool.InputParam;
import org.opencb.opencga.analysis.JobFactory;
import org.opencb.opencga.catalog.monitor.ExecutionOutputRecorder;
import org.opencb.opencga.catalog.monitor.daemons.IndexDaemon;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.variant.VariantStorage;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.catalog.utils.CatalogSampleAnnotationsLoader;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 29/09/14.
 */
@Deprecated
public class OpenCGAMainOld {

    private static String shellUserId;
    private static String shellSessionId;
    private static boolean interactive;
    private CatalogManager catalogManager;
//    private String userId;
//    private String sessionId;
    private boolean logoutAtExit = false;
    private static boolean sessionIdFromFile = false;

    private static Logger logger;
    private static SessionFile sessionFile;
    private static String logLevel;

    public static void main(String[] args) throws IOException {

        OpenCGAMainOld opencgaMain = new OpenCGAMainOld();

        OptionsParser optionsParser = new OptionsParser(false);
        try {
            optionsParser.parse(args);
            logLevel = "info";
            if (optionsParser.getCommonOptions().verbose) {
                logLevel = "debug";
            }
            if (optionsParser.getCommonOptions().logLevel != null) {
                logLevel = optionsParser.getCommonOptions().logLevel;
            }
            setLogLevel(logLevel);
            Config.setAutoOpenCGAHome();
        } catch (ParameterException e){

            if(!optionsParser.getGeneralOptions().help && !optionsParser.getCommonOptions().help ) {
                System.out.println(e.getMessage());
            }

            optionsParser.printUsage();
            System.exit(1);
        }

        if(optionsParser.getCommonOptions().help) {
            optionsParser.printUsage();
            System.exit(1);
        }
        if (optionsParser.getGeneralOptions().version) {
            System.out.println("Version " + GitRepositoryState.get().getBuildVersion());
            System.out.println("git version: " + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId());
            System.exit(0);
        }

        sessionFile = loadUserFile();

        // Interactive mode
        interactive = optionsParser.getGeneralOptions().interactive;
        if (interactive) {
            BufferedReader reader;//create BufferedReader object

            reader = new BufferedReader(new InputStreamReader(System.in));

            if (sessionFile != null) {
                shellUserId = sessionFile.getUserId();
                shellSessionId = sessionFile.getSessionId();
                sessionIdFromFile = true;
            }
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
            try {
                System.exit(opencgaMain.runCommand(optionsParser));
            } catch (RuntimeException e) {
                logger.error(e.getMessage(), e);
                System.exit(1);
            } catch (Exception e) {
                logger.error(e.getMessage());
                logger.debug(e.getMessage(), e);
                System.exit(1);
            }
        }
    }

    private int runCommand(String[] args) throws Exception {
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

        return runCommand(optionsParser);
    }

    private int runCommand(OptionsParser optionsParser) throws Exception {
        int returnValue = 0;
        if(catalogManager == null && !optionsParser.getSubCommand().isEmpty() ) {
            CatalogConfiguration catalogConfiguration = CatalogConfiguration.load(new FileInputStream(Paths.get(Config.getOpenCGAHome(),
                    "conf", "catalog-configuration.yml").toFile()));
            catalogManager = new CatalogManager(catalogConfiguration);
        }

        String sessionId = login(optionsParser.getUserAndPasswordOptions());

        switch (optionsParser.getCommand()) {
            case "users":
                switch (optionsParser.getSubCommand()) {
                    case "create": {
                        OptionsParser.UserCommands.CreateCommand c = optionsParser.getUserCommands().createCommand;
                        //QueryResult<User> user = catalogManager.insertUser(new User(c.up.user, c.name, c.email, c.up.password, c.organization, User.Role.USER, ""));
                        QueryResult<User> user = catalogManager.createUser(c.user, c.name, c.email, c.password, c.organization, null, null);
                        System.out.println(createOutput(c.cOpt, user, null));
                        break;
                    }
                    case "info": {
                        OptionsParser.UserCommands.InfoCommand c = optionsParser.getUserCommands().infoCommand;

                        QueryResult<User> user = catalogManager.getUser(c.up.user != null ? c.up.user : catalogManager.getUserIdBySessionId(sessionId), null, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(c.cOpt, user, null));

                        break;
                    }
                    case "list": {
                        OptionsParser.UserCommands.ListCommand c = optionsParser.getUserCommands().listCommand;

                        String indent = "";
                        User user = catalogManager.getUser(c.up.user != null ? c.up.user : catalogManager.getUserIdBySessionId(sessionId), null,
                                new QueryOptions("include", Arrays.asList("id", "name", "projects.id","projects.alias","projects.name")), sessionId).first();
                        System.out.println(user.getId() + " - " + user.getName());
                        indent+= "\t";
                        System.out.println(listProjects(user.getProjects(), c.recursive ? c.level : 1, indent, c.uries, new StringBuilder(), sessionId));

                        break;
                    }
                    case "login": {
                        OptionsParser.UserCommands.LoginCommand c = optionsParser.getUserCommands().loginCommand;

//                        if (c.up.user == null || c.up.user.isEmpty()) {
//                            throw new CatalogException("Required userId");
//                        }
                        shellSessionId = sessionId;
                        logoutAtExit = false;
                        if (shellSessionId != null) {
                            shellUserId = c.up.user;
                        }
                        if (sessionFile == null) {
                            sessionFile = new SessionFile();
                        }
                        sessionFile.setSessionId(sessionId);
                        sessionFile.setUserId(catalogManager.getUserIdBySessionId(sessionId));
                        saveUserFile(sessionFile);

                        System.out.println(shellSessionId);

                        break;
                    }
                    case "logout": {
                        OptionsParser.UserCommands.LogoutCommand c = optionsParser.getUserCommands().logoutCommand;

                        QueryResult logout;
                        if (c.sessionId == null) {  //Logout from interactive shell
                            logout = catalogManager.logout(shellUserId, shellSessionId);
                            shellUserId = null;
                            shellSessionId = null;
                            if (sessionIdFromFile) {
                                sessionFile.setSessionId(null);
                                sessionFile.setUserId(null);
                                saveUserFile(sessionFile);
                            }
                        } else {
                            String userId = catalogManager.getUserIdBySessionId(c.sessionId);
                            logout = catalogManager.logout(userId, c.sessionId);
                        }
                        logoutAtExit = false;
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

                        String user = c.up.user == null || c.up.user.isEmpty() ? catalogManager.getUserIdBySessionId(sessionId) : c.up.user;
                        QueryResult<Project> project = catalogManager.createProject(c.name, c.alias, c.description, c.organization, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(c.cOpt, project, null));

                        break;
                    }
                    case "info": {
                        OptionsParser.ProjectCommands.InfoCommand c = optionsParser.getProjectCommands().infoCommand;

                        long projectId = catalogManager.getProjectId(c.id);
                        QueryResult<Project> project = catalogManager.getProject(projectId, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(c.cOpt, project, null));

                        break;
                    }
//                    case "share": {
//                        OptionsParser.CommandShareResource c = optionsParser.commandShareResource;
//
//                        int projectId = catalogManager.getProjectId(c.id);
//                        QueryResult result = catalogManager.shareProject(projectId, new AclEntry(c.user, c.read, c.write, c.execute, c.delete), sessionId);
//                        System.out.println(createOutput(c.cOpt, result, null));
//
//                        break;
//                    }
                    default:
                        optionsParser.printUsage();
                        break;
                }
                break;
            case "studies":
                switch (optionsParser.getSubCommand()) {
                    case "create": {
                        OptionsParser.StudyCommands.CreateCommand c = optionsParser.getStudyCommands().createCommand;

                        URI uri = null;
                        if (c.uri != null && !c.uri.isEmpty()) {
                            uri = UriUtils.createUri(c.uri);
                        }
                        Map<File.Bioformat, DataStore> dataStoreMap = parseBioformatDataStoreMap(c);
                        long projectId = catalogManager.getProjectId(c.projectId);
                        ObjectMap attributes = new ObjectMap();
                        attributes.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), c.aggregated.toString());
                        QueryResult<Study> study = catalogManager.createStudy(projectId, c.name, c.alias, c.type, null, c.description, null, null, null, uri, dataStoreMap, null, attributes,
                                new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        if (uri != null) {
                            File root = catalogManager.searchFile(study.first().getId(),
                                    new Query(FileDBAdaptor.QueryParams.PATH.key(), ""), sessionId).first();
                            new FileScanner(catalogManager).scan(root, uri, FileScanner.FileScannerPolicy.REPLACE, true, false, sessionId);
                        }
                        System.out.println(createOutput(c.cOpt, study, null));

                        break;
                    }
                    case "resync": {
                        OptionsParser.StudyCommands.ResyncCommand c = optionsParser.getStudyCommands().resyncCommand;
                        long studyId = catalogManager.getStudyId(c.id);

                        Study study = catalogManager.getStudy(studyId, sessionId).first();
                        FileScanner fileScanner = new FileScanner(catalogManager);
                        List<File> scan = fileScanner.reSync(study, c.calculateChecksum, sessionId);
                        System.out.println(createOutput(c.cOpt, scan, null));

                        break;
                    }
                    case "check-files": {
                        OptionsParser.StudyCommands.CheckCommand c = optionsParser.getStudyCommands().checkCommand;
                        long studyId = catalogManager.getStudyId(c.id);

                        Study study = catalogManager.getStudy(studyId, sessionId).first();
                        FileScanner fileScanner = new FileScanner(catalogManager);
                        List<File> check = fileScanner.checkStudyFiles(study, c.calculateChecksum, sessionId);
                        System.out.println(createOutput(c.cOpt, check, null));

                        break;
                    }
                    case "info": {
                        OptionsParser.StudyCommands.InfoCommand c = optionsParser.getStudyCommands().infoCommand;

                        long studyId = catalogManager.getStudyId(c.id);
                        QueryResult<Study> study = catalogManager.getStudy(studyId, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(c.cOpt, study, null));

                        break;
                    }
                    case "list": {
                        OptionsParser.StudyCommands.ListCommand c = optionsParser.getStudyCommands().listCommand;

                        long studyId = catalogManager.getStudyId(c.id);
                        List<Study> studies = catalogManager.getStudy(studyId, sessionId).getResult();
                        String indent = "";
                        System.out.println(listStudies(studies, c.recursive ? c.level : 1, indent, c.uries, new StringBuilder(), sessionId));

                        break;
                    }
                    case "status": {
                        OptionsParser.StudyCommands.StatusCommand c = optionsParser.getStudyCommands().statusCommand;

                        long studyId = catalogManager.getStudyId(c.id);
                        Study study = catalogManager.getStudy(studyId, sessionId).first();
                        FileScanner fileScanner = new FileScanner(catalogManager);

                        /** First, run CheckStudyFiles to find new missing files **/
                        List<File> checkStudyFiles = fileScanner.checkStudyFiles(study, false, sessionId);
                        List<File> found = checkStudyFiles.stream().filter(f -> f.getStatus().getName().equals(File.FileStatus.READY))
                                .collect(Collectors.toList());
                        int maxFound = found.stream().map(f -> f.getPath().length()).max(Comparator.<Integer>naturalOrder()).orElse(0);

                        /** Get untracked files **/
//                        List<URI> untrackedFiles = fileScanner.untrackedFiles(study, sessionId);
//
//                        URI studyUri = catalogManager.getStudyUri(studyId);
//                        Map<URI, String> relativeUrisMap = untrackedFiles.stream().collect(Collectors.toMap((k) -> k, (u) -> studyUri.relativize(u).toString()));

                        Map<String, URI> relativeUrisMap = fileScanner.untrackedFiles(study, sessionId);
                        int maxUntracked = relativeUrisMap.keySet().stream().map(String::length).max(Comparator.<Integer>naturalOrder()).orElse(0);

                        /** Get missing files **/
                        List<File> missingFiles = catalogManager.getAllFiles(studyId,
                                new Query(FileDBAdaptor.QueryParams.FILE_STATUS.key(), File.FileStatus.MISSING),
                                new QueryOptions(), sessionId).getResult();
                        int maxMissing = missingFiles.stream().map(f -> f.getPath().length()).max(Comparator.<Integer>naturalOrder()).orElse(0);


                        /** Print pretty **/
                        String format = "\t%-" + Math.max(Math.max(maxMissing, maxUntracked), maxFound) + "s  -> %s\n";

                        if (!relativeUrisMap.isEmpty()) {
                            System.out.println("UNTRACKED files");
                            relativeUrisMap.forEach((s, u) -> System.out.printf(format, s, u));
                            System.out.println("\n");
                        }

                        if (!missingFiles.isEmpty()) {
                            System.out.println("MISSING files");
                            for (File file : missingFiles) {
                                System.out.printf(format, file.getPath(), catalogManager.getFileUri(file));
                            }
                            System.out.println("\n");
                        }

                        if (!found.isEmpty()) {
                            System.out.println("FOUND files");
                            for (File file : found) {
                                System.out.printf(format, file.getPath(), catalogManager.getFileUri(file));
                            }
                        }

                        break;
                    }
                    case "annotate-variants": {
                        OptionsParser.StudyCommands.AnnotationCommand c = optionsParser.getStudyCommands().annotationCommand;
                        VariantStorage variantStorage = new VariantStorage(catalogManager);

                        long studyId = catalogManager.getStudyId(c.id);
                        long outdirId = catalogManager.getFileId(c.outdir);
                        QueryOptions queryOptions = new QueryOptions(c.cOpt.getQueryOptions());

                        queryOptions.put(ExecutorManager.EXECUTE, !c.enqueue);
                        queryOptions.add(AnalysisFileIndexer.PARAMETERS, c.dashDashParameters);
                        queryOptions.add(AnalysisFileIndexer.LOG_LEVEL, logLevel);
                        System.out.println(createOutput(c.cOpt, variantStorage.annotateVariants(studyId, outdirId, sessionId, queryOptions), null));

                        break;
                    }
//                    case "share": {
//                        OptionsParser.CommandShareResource c = optionsParser.commandShareResource;
//
//                        int studyId = catalogManager.getStudyId(c.id);
//                        QueryResult result = catalogManager.shareProject(studyId, new AclEntry(c.user, c.read, c.write, c.execute, c.delete), sessionId);
//                        System.out.println(createOutput(c.cOpt, result, null));
//
//                        break;
//                    }
                    default:
                        optionsParser.printUsage();
                        break;
                }
                break;
            case "files": {
                switch (optionsParser.getSubCommand()) {
                    case "create": {
                        OptionsParser.FileCommands.CreateCommand c = optionsParser.getFileCommands().createCommand;

                        long studyId = catalogManager.getStudyId(c.studyId);
                        Path inputFile = Paths.get(c.inputFile);
                        URI sourceUri = new URI(null, c.inputFile, null);
                        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
                            sourceUri = inputFile.toUri();
                        }
                        if (!catalogManager.getCatalogIOManagerFactory().get(sourceUri).exists(sourceUri)) {
                            throw new IOException("File " + sourceUri + " does not exist");
                        }

                        QueryResult<File> file = catalogManager.createFile(studyId, c.format, c.bioformat,
                                Paths.get(c.path, inputFile.getFileName().toString()).toString(), c.description,
                                c.parents, -1, sessionId);
                        new CatalogFileUtils(catalogManager).upload(sourceUri, file.first(), null, sessionId, false, false, c.move, c.calculateChecksum);
                        FileMetadataReader.get(catalogManager).setMetadataInformation(file.first(), null, new QueryOptions(c.cOpt.getQueryOptions()), sessionId, false);
                        System.out.println(createOutput(c.cOpt, file, null));

                        break;
                    }
                    case "create-folder": {
                        OptionsParser.FileCommands.CreateFolderCommand c = optionsParser.getFileCommands().createFolderCommand;

                        long studyId = catalogManager.getStudyId(c.studyId);
                        QueryResult<File> folder = catalogManager.createFolder(studyId, Paths.get(c.path), c.parents, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(c.cOpt, folder, null));

                        break;
                    }
                    case "upload": {
                        OptionsParser.FileCommands.UploadCommand c = optionsParser.getFileCommands().uploadCommand;
                        URI sourceUri = new URI(null, c.inputFile, null);
                        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
                            sourceUri = Paths.get(c.inputFile).toUri();
                        }
                        if (!catalogManager.getCatalogIOManagerFactory().get(sourceUri).exists(sourceUri)) {
                            throw new IOException("File " + sourceUri + " does not exist");
                        }

                        long fileId = catalogManager.getFileId(c.id);
                        QueryResult<File> file = catalogManager.getFile(fileId, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);

                        new CatalogFileUtils(catalogManager).upload(sourceUri, file.first(), null, sessionId, c.replace, c.replace, c.move, c.calculateChecksum);
                        System.out.println(createOutput(c.cOpt, catalogManager.getFile(file.first().getId(), new QueryOptions(c.cOpt.getQueryOptions()), sessionId), null));
                        break;
                    }
                    case "link": {
                        OptionsParser.FileCommands.LinkCommand c = optionsParser.getFileCommands().linkCommand;

                        Path inputFile = Paths.get(c.inputFile);

                        URI inputUri = UriUtils.createUri(c.inputFile);


                        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(inputUri);
                        if (!ioManager.exists(inputUri)) {
                            throw new FileNotFoundException("File " + inputUri + " not found");
                        }

//                        long studyId = catalogManager.getStudyId(c.studyId);
                        String path = c.path.isEmpty() ? inputFile.getFileName().toString()
                                : Paths.get(c.path, inputFile.getFileName().toString()).toString();
                        File file;
                        CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);
                        if (ioManager.isDirectory(inputUri)) {
                            ObjectMap params = new ObjectMap("parents", c.parents);
                            file = catalogManager.link(inputUri, c.path, c.studyId, params, sessionId).first();
//                            file = catalogFileUtils.linkFolder(studyId, path, c.parents, c.description, c.calculateChecksum, inputUri, false, false, sessionId);
                            new FileScanner(catalogManager).scan(file, null, FileScanner.FileScannerPolicy.REPLACE, c.calculateChecksum, false, sessionId);
                        } else {
                            ObjectMap params = new ObjectMap("parents", c.parents);
                            file = catalogManager.link(inputUri, c.path, c.studyId, params, sessionId).first();
//                            file = catalogManager.createFile(studyId, null, null,
//                                    path, c.description, c.parents, -1, sessionId).first();
//                            file = catalogFileUtils.link(file, c.calculateChecksum, inputUri, false, false, sessionId);
                            file = FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, new QueryOptions(c.cOpt.getQueryOptions()), sessionId, false);
                        }

                        System.out.println(createOutput(c.cOpt, file, null));

                        break;
                    }
                    case "relink": {
                        OptionsParser.FileCommands.RelinkCommand c = optionsParser.getFileCommands().relinkCommand;

                        Path inputFile = Paths.get(c.inputFile);
                        URI uri = UriUtils.createUri(c.inputFile);

                        if (!inputFile.toFile().exists()) {
                            throw new FileNotFoundException("File " + uri + " not found");
                        }

                        long fileId = catalogManager.getFileId(c.id, sessionId);
                        File file = catalogManager.getFile(fileId, sessionId).first();

                        new CatalogFileUtils(catalogManager).link(file, c.calculateChecksum, uri, false, true, sessionId);
                        file = catalogManager.getFile(file.getId(), new QueryOptions(c.cOpt.getQueryOptions()), sessionId).first();
                        file = FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, new QueryOptions(c.cOpt.getQueryOptions()), sessionId, false);


                        System.out.println(createOutput(c.cOpt, file, null));

                        break;
                    }
                    case "refresh": {
                        OptionsParser.FileCommands.RefreshCommand c = optionsParser.getFileCommands().refreshCommand;

                        long fileId = catalogManager.getFileId(c.id);
                        File file = catalogManager.getFile(fileId, sessionId).first();

                        List<File> files;
                        QueryOptions queryOptions = new QueryOptions(c.cOpt.getQueryOptions());
                        CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);
                        FileMetadataReader fileMetadataReader = FileMetadataReader.get(catalogManager);
                        if (file.getType() == File.Type.FILE) {
                            File file1 = catalogFileUtils.checkFile(file, false, sessionId);
                            file1 = fileMetadataReader.setMetadataInformation(file1, null, queryOptions, sessionId, false);
                            if (file == file1) {    //If the file is the same, it was not modified. Only return modified files.
                                files = Collections.emptyList();
                            } else {
                                files = Collections.singletonList(file);
                            }
                        } else {
                            List<File> result = catalogManager.getAllFilesInFolder(file.getId(), null, sessionId).getResult();
                            files = new ArrayList<>(result.size());
                            for (File f : result) {
                                File file1 = fileMetadataReader.setMetadataInformation(f, null, queryOptions, sessionId, false);
                                if (f != file1) {    //Add only modified files.
                                    files.add(file1);
                                }
                            }
                        }

                        System.out.println(createOutput(c.cOpt, files, null));
                        break;
                    }
                    case "info": {
                        OptionsParser.FileCommands.InfoCommand c = optionsParser.getFileCommands().infoCommand;

                        long fileId = catalogManager.getFileId(c.id);
                        QueryResult<File> file = catalogManager.getFile(fileId, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(optionsParser.getCommonOptions(), file, null));

                        break;
                    }
                    case "search": {
                        OptionsParser.FileCommands.SearchCommand c = optionsParser.getFileCommands().searchCommand;

                        long studyId = catalogManager.getStudyId(c.studyId);
                        Query query = new Query();
                        if (c.name != null) query.put(FileDBAdaptor.QueryParams.NAME.key(), "~" + c.name);
                        if (c.directory != null) query.put(FileDBAdaptor.QueryParams.DIRECTORY.key(), c.directory);
                        if (c.bioformats != null) query.put(FileDBAdaptor.QueryParams.BIOFORMAT.key(), c.bioformats);
                        if (c.types != null) query.put(FileDBAdaptor.QueryParams.TYPE.key(), c.types);
                        if (c.status != null) query.put(FileDBAdaptor.QueryParams.STATUS_NAME.key(), c.status);

                        QueryResult<File> fileQueryResult = catalogManager.searchFile(studyId, query, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(optionsParser.getCommonOptions(), fileQueryResult, null));

                        break;
                    }
                    case "list": {
                        OptionsParser.FileCommands.ListCommand c = optionsParser.getFileCommands().listCommand;

                        long fileId = catalogManager.getFileId(c.id);
                        List<File> result = catalogManager.getFile(fileId, sessionId).getResult();
                        long studyId = catalogManager.getStudyIdByFileId(fileId);
                        System.out.println(listFiles(result, studyId, c.recursive? c.level : 1, "", c.uries, new StringBuilder(), sessionId));

                        break;
                    }
                    case "index": {
                        OptionsParser.FileCommands.IndexCommand c = optionsParser.getFileCommands().indexCommand;

                        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);

                        long fileId = catalogManager.getFileId(c.id);
                        long outdirId = catalogManager.getFileId(c.outdir);
                        if (outdirId < 0) {
                            outdirId  = catalogManager.getFileParent(fileId, null, sessionId).first().getId();
                        }
                        String sid = sessionId;
                        QueryOptions queryOptions = new QueryOptions(c.cOpt.getQueryOptions());
                        if (c.enqueue) {
                            queryOptions.put(ExecutorManager.EXECUTE, false);
                            if (c.up.sessionId == null || c.up.sessionId.isEmpty()) {
                                sid = login(c.up);
                            }
                        } else {
                            queryOptions.add(ExecutorManager.EXECUTE, true);
                        }
                        queryOptions.put(AnalysisFileIndexer.TRANSFORM, c.transform);
                        queryOptions.put(AnalysisFileIndexer.LOAD, c.load);
                        queryOptions.add(AnalysisFileIndexer.PARAMETERS, c.dashDashParameters);
                        queryOptions.add(AnalysisFileIndexer.LOG_LEVEL, logLevel);
                        queryOptions.add(VariantStorageManager.Options.CALCULATE_STATS.key(), c.calculateStats);
                        queryOptions.add(VariantStorageManager.Options.ANNOTATE.key(), c.annotate);
                        logger.debug("logLevel: {}", logLevel);
                        QueryResult<Job> queryResult = analysisFileIndexer.index(fileId, outdirId, sid, queryOptions);
                        System.out.println(createOutput(c.cOpt, queryResult, null));

                        break;
                    }
                    default:
                        optionsParser.printUsage();
                        break;
                }
                break;
            }
            case "samples" : {
                switch (optionsParser.getSubCommand()) {
                    case "info": {
                        OptionsParser.SampleCommands.InfoCommand c = optionsParser.sampleCommands.infoCommand;

                        QueryResult<Sample> sampleQueryResult = catalogManager.getSample(c.id, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(c.cOpt, sampleQueryResult, null));

                        break;
                    }
                    case "search": {
                        OptionsParser.SampleCommands.SearchCommand c = optionsParser.sampleCommands.searchCommand;

                        long studyId = catalogManager.getStudyId(c.studyId);
                        QueryOptions queryOptions = new QueryOptions(c.cOpt.getQueryOptions());
                        Query query = new Query();
                        if (c.sampleIds != null && !c.sampleIds.isEmpty()) {
                            query.append(SampleDBAdaptor.QueryParams.ID.key(), c.sampleIds);
                        }
                        if (c.sampleNames != null && !c.sampleNames.isEmpty()) {
                            query.append(SampleDBAdaptor.QueryParams.NAME.key(), c.sampleNames);
                        }
                        if (c.annotation != null && !c.annotation.isEmpty()) {
                            for (String s : c.annotation) {
                                String[] strings = org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils.splitOperator(s);
                                query.append(SampleDBAdaptor.QueryParams.ANNOTATION.key() + "." + strings[0], strings[1] + strings[2]);
                            }
                        }
                        if (c.variableSetId != null && !c.variableSetId.isEmpty()) {
                            query.append(SampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), c.variableSetId);
                        }
                        QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(studyId, query, queryOptions, sessionId);
                        System.out.println(createOutput(c.cOpt, sampleQueryResult, null));

                        break;
                    }
                    case "load": {
                        OptionsParser.SampleCommands.LoadCommand c = optionsParser.sampleCommands.loadCommand;

                        CatalogSampleAnnotationsLoader catalogSampleAnnotationsLoader = new CatalogSampleAnnotationsLoader(catalogManager);
                        long fileId = catalogManager.getFileId(c.pedigreeFileId);
                        File pedigreeFile = catalogManager.getFile(fileId, sessionId).first();

                        QueryResult<Sample> sampleQueryResult = catalogSampleAnnotationsLoader.loadSampleAnnotations(pedigreeFile, c.variableSetId == 0 ? null : c.variableSetId, sessionId);
                        System.out.println(createOutput(c.cOpt, sampleQueryResult, null));

                        break;
                    }
                    case "delete": {
                        OptionsParser.SampleCommands.DeleteCommand c = optionsParser.sampleCommands.deleteCommand;

                        QueryResult<Sample> sampleQueryResult = catalogManager.deleteSample(c.id, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(c.cOpt, sampleQueryResult, null));

                        break;
                    }
                    default: {
                        optionsParser.printUsage();
                        break;
                    }

                }
                break;
            }
            case "cohorts" : {
                switch (optionsParser.getSubCommand()) {
                    case OptionsParser.CohortCommands.InfoCommand.COMMAND_NAME: {
                        OptionsParser.CohortCommands.InfoCommand c = optionsParser.cohortCommands.infoCommand;

                        QueryResult<Cohort> cohortQueryResult = catalogManager.getCohort(c.id, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(c.cOpt, cohortQueryResult, null));

                        break;
                    }
                    case OptionsParser.CohortCommands.SamplesCommand.COMMAND_NAME: {
                        OptionsParser.CohortCommands.SamplesCommand c = optionsParser.cohortCommands.samplesCommand;

                        Cohort cohort = catalogManager.getCohort(c.id, null, sessionId).first();
                        QueryOptions queryOptions = new QueryOptions(c.cOpt.getQueryOptions());
                        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), cohort.getSamples());
                        QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(
                                catalogManager.getStudyIdByCohortId(cohort.getId()), query, queryOptions, sessionId);
                        OptionsParser.CommonOptions cOpt = c.cOpt;
                        StringBuilder sb = createOutput(cOpt, sampleQueryResult, null);
                        System.out.println(sb.toString());

                        break;
                    }
                    case OptionsParser.CohortCommands.CreateCommand.COMMAND_NAME: {
                        OptionsParser.CohortCommands.CreateCommand c = optionsParser.cohortCommands.createCommand;

                        Map<String, List<Sample>> cohorts = new HashMap<>();
                        long studyId = catalogManager.getStudyId(c.studyId);

                        if (c.sampleIds != null && !c.sampleIds.isEmpty()) {
                            QueryOptions queryOptions = new QueryOptions("include", "projects.studies.samples.id");
                            Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), c.sampleIds);
//                            queryOptions.put("variableSetId", c.variableSetId);
                            QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(studyId, query, queryOptions, sessionId);
                            cohorts.put(c.name, sampleQueryResult.getResult());
                        } else if (StringUtils.isNotEmpty(c.tagmap)) {
                            List<QueryResult<Cohort>> queryResults = createCohorts(sessionId, studyId, c.tagmap, catalogManager, logger);
                            System.out.println(createOutput(c.cOpt, queryResults, null));
                        } else {
//                            QueryOptions queryOptions = c.cOpt.getQueryOptions();
//                            queryOptions.put("annotation", c.annotation);
                            final long variableSetId;
                            final VariableSet variableSet;
                            if (StringUtils.isNumeric(c.variableSet)) {
                                variableSetId = Long.parseLong(c.variableSet);
                                variableSet = catalogManager.getVariableSet(variableSetId, null, sessionId).first();
                            } else if (StringUtils.isEmpty(c.variableSet)) {
                                List<VariableSet> variableSets = catalogManager.getStudy(studyId, new QueryOptions("include", "projects.studies.variableSets"), sessionId).first().getVariableSets();
                                if (!variableSets.isEmpty()) {
                                    variableSet = variableSets.get(0); //Get the first VariableSetId
                                    variableSetId = variableSet.getId();
                                } else {
                                    throw new CatalogException("Expected variableSetId");
                                }
                            } else {
                                QueryOptions query = new QueryOptions(StudyDBAdaptor.VariableSetParams.NAME.key(), c.variableSet);
                                variableSet = catalogManager.getAllVariableSet(studyId, query, sessionId).first();
                                if (variableSet == null) {
                                    throw new CatalogException("Variable set \"" + c.variableSet + "\" not found");
                                }
                                variableSetId = variableSet.getId();
                            }
                            c.name = ((c.name == null) || c.name.isEmpty()) ? "" : (c.name + ".");
                            for (Variable variable : variableSet.getVariables()) {
                                if (variable.getName().equals(c.variable)) {
                                    for (String value : variable.getAllowedValues()) {
                                        QueryOptions queryOptions = new QueryOptions(c.cOpt.getQueryOptions());
                                        Query query = new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key() + "." + c.variable, value)
                                                .append(SampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), variableSetId);
                                        QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(studyId, query, queryOptions, sessionId);
                                        cohorts.put(c.name + value, sampleQueryResult.getResult());
                                    }
                                }
                            }
                            if (cohorts.isEmpty()) {
                                logger.error("VariableSetId {} does not contain any variable with id = {}.", variableSetId, c.variable);
                                returnValue = 2;
                            }
                        }

                        if (!cohorts.isEmpty()) {
                            List<QueryResult<Cohort>> queryResults = new ArrayList<>(cohorts.size());
                            for (Map.Entry<String, List<Sample>> entry : cohorts.entrySet()) {
                                List<Long> sampleIds = new LinkedList<>();
                                for (Sample sample : entry.getValue()) {
                                    sampleIds.add(sample.getId());
                                }
                                QueryResult<Cohort> cohort = catalogManager.createCohort(studyId, entry.getKey(), c.type, c.description, sampleIds, c.cOpt.getQueryOptions(), sessionId);
                                queryResults.add(cohort);
                            }
                            System.out.println(createOutput(c.cOpt, queryResults, null));
                        }
//                        System.out.println(createSamplesOutput(c.cOpt, sampleQueryResult));


                        break;
                    }
                    case OptionsParser.CohortCommands.StatsCommand.COMMAND_NAME: {
                        OptionsParser.CohortCommands.StatsCommand c = optionsParser.cohortCommands.statsCommand;

                        VariantStorage variantStorage = new VariantStorage(catalogManager);

                        long outdirId = catalogManager.getFileId(c.outdir);
                        QueryOptions queryOptions = new QueryOptions(c.cOpt.getQueryOptions());
                        if (c.enqueue) {
                            queryOptions.put(ExecutorManager.EXECUTE, false);
                        } else {
                            queryOptions.add(ExecutorManager.EXECUTE, true);
                        }
                        queryOptions.add(AnalysisFileIndexer.PARAMETERS, c.dashDashParameters);
                        queryOptions.add(AnalysisFileIndexer.LOG_LEVEL, logLevel);
                        if (c.tagmap != null) {
                            queryOptions.put(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), c.tagmap);
                        } else if (c.cohortIds == null) {
                            logger.error("--cohort-id nor --aggregation-mapping-file provided");
                            throw new IllegalArgumentException("--cohort-id or --aggregation-mapping-file is required to specify cohorts");
                        }
                        System.out.println(createOutput(c.cOpt, variantStorage.calculateStats(outdirId, c.cohortIds, sessionId, queryOptions), null));

                        break;
                    }

                    default: {
                        optionsParser.printUsage();
                        break;
                    }
                }
                break;
            }
            case "jobs" : {
                switch (optionsParser.getSubCommand()) {
                    case "info": {
                        OptionsParser.JobsCommands.InfoCommand c = optionsParser.getJobsCommands().infoCommand;

                        QueryResult<Job> jobQueryResult = catalogManager.getJob(c.id, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(c.cOpt, jobQueryResult, null));

                        break;
                    }
                    case "finished": {
                        OptionsParser.JobsCommands.DoneJobCommand c = optionsParser.getJobsCommands().doneJobCommand;

                        QueryResult<Job> jobQueryResult = catalogManager.getJob(c.id, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        Job job = jobQueryResult.first();
                        if (c.force) {
                            if (job.getStatus().getName().equals(Job.JobStatus.ERROR) || job.getStatus().getName().equals(Job.JobStatus.READY)) {
                                logger.info("Job status is '{}' . Nothing to do.", job.getStatus().getName());
                                System.out.println(createOutput(c.cOpt, jobQueryResult, null));
                            }
                        } else if (!job.getStatus().getName().equals(Job.JobStatus.DONE)) {
                            throw new Exception("Job status != DONE. Need --force to continue");
                        }

                        /** Record output **/
                        ExecutionOutputRecorder outputRecorder = new ExecutionOutputRecorder(catalogManager, sessionId);
                        if (c.discardOutput) {
                            String tempJobsDir = catalogManager.getCatalogConfiguration().getTempJobsDir();
                            URI tmpOutDirUri = IndexDaemon.getJobTemporaryFolder(job.getId(), tempJobsDir).toUri();
                            CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(tmpOutDirUri);
                            if (ioManager.exists(tmpOutDirUri)) {
                                logger.info("Deleting temporal job output folder: {}", tmpOutDirUri);
                                ioManager.deleteDirectory(tmpOutDirUri);
                            } else {
                                logger.info("Temporal job output folder already removed: {}", tmpOutDirUri);
                            }
                        } else {
                            outputRecorder.recordJobOutput(job);
                        }
                        outputRecorder.postProcessJob(job, c.error);

                        /** Change status to ERROR or READY **/
                        ObjectMap parameters = new ObjectMap();
                        if (c.error) {
                            parameters.put("status.name", Job.JobStatus.ERROR);
                            parameters.put("error", Job.ERRNO_ABORTED);
                            parameters.put("errorDescription", Job.ERROR_DESCRIPTIONS.get(Job.ERRNO_ABORTED));
                        } else {
                            parameters.put("status.name", Job.JobStatus.READY);
                        }
                        catalogManager.modifyJob(job.getId(), parameters, sessionId);

                        jobQueryResult = catalogManager.getJob(c.id, new QueryOptions(c.cOpt.getQueryOptions()), sessionId);
                        System.out.println(createOutput(c.cOpt, jobQueryResult, null));

                        break;
                    }
                    case "status": {
                        OptionsParser.JobsCommands.StatusCommand c = optionsParser.getJobsCommands().statusCommand;

                        final List<Long> studyIds;
                        if (c.studyId == null || c.studyId.isEmpty()) {
                            studyIds = catalogManager.getAllStudies(new Query(), new QueryOptions("include", "id"), sessionId)
                                    .getResult().stream().map(Study::getId).collect(Collectors.toList());
                        } else {
                            studyIds = new LinkedList<>();
                            for (String s : c.studyId.split(",")) {
                                studyIds.add(catalogManager.getStudyId(s));
                            }
                        }
                        for (Long studyId : studyIds) {
                            QueryResult<Job> allJobs = catalogManager.getAllJobs(studyId,
                                    new Query(JobDBAdaptor.QueryParams.STATUS_NAME.key(),
                                            Collections.singletonList(Job.JobStatus.RUNNING.toString())), new QueryOptions(), sessionId);

                            for (Iterator<Job> iterator = allJobs.getResult().iterator(); iterator.hasNext(); ) {
                                Job job = iterator.next();
                                System.out.format("Job - %s [%d] - %s\n", job.getName(), job.getId(), job.getDescription());
//                                URI tmpOutDirUri = job.getTmpOutDirUri();
                                String tempJobsDir = catalogManager.getCatalogConfiguration().getTempJobsDir();
                                URI tmpOutDirUri = IndexDaemon.getJobTemporaryFolder(job.getId(), tempJobsDir).toUri();
                                CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(tmpOutDirUri);
                                try {
                                    ioManager.listFilesStream(tmpOutDirUri)
                                            .sorted()
                                            .forEach(uri -> {
                                                        String count;
                                                        try {
                                                            long fileSize = ioManager.getFileSize(uri);
                                                            count = humanReadableByteCount(fileSize, false);
                                                        } catch (CatalogIOException e) {
                                                            count = "ERROR";
                                                        }
                                                        System.out.format("\t%s [%s]\n", tmpOutDirUri.relativize(uri), count);
                                                    }
                                            );
                                } catch (CatalogIOException e) {
                                    System.out.println("Unable to read files from " + tmpOutDirUri + " - " + e.getCause().getMessage());
                                }
                                if (iterator.hasNext()) {
                                    System.out.println("-----------------------------------------");
                                }
                            }
                        }
                        break;
                    }
                    case "run": {
                        OptionsParser.JobsCommands.RunJobCommand c = optionsParser.getJobsCommands().runJobCommand;

                        long studyId = catalogManager.getStudyId(c.studyId);
                        long outdirId = catalogManager.getFileId(c.outdir);
                        long toolId = catalogManager.getToolId(c.toolId);
                        String toolName;
                        ToolManager toolManager;
                        if (toolId < 0) {
                            toolManager = new ToolManager(c.toolId, c.execution);    //LEGACY MODE, AVOID USING
                            toolName = c.toolId;
                        } else {
                            Tool tool = catalogManager.getTool(toolId, sessionId).getResult().get(0);
                            toolManager = new ToolManager(Paths.get(tool.getPath()).getParent(), tool.getName(), c.execution);
                            toolName = tool.getName();
                        }


                        List<Long> inputFiles = new LinkedList<>();
                        Map<String, List<String>> localParams = new HashMap<>();

                        for (String key : c.params.keySet()) {
                            localParams.put(key, c.params.getAsStringList(key));
                        }


                        Execution ex = toolManager.getExecution();
                        // Set input param
                        for (InputParam inputParam : ex.getInputParams()) {
                            if (c.params.containsKey(inputParam.getName())) {
                                List<String> filePaths = new LinkedList<>();
                                for (String fileId : c.params.getAsStringList(inputParam.getName())) {
                                    File file = catalogManager.getFile(catalogManager.getFileId(fileId), sessionId).getResult().get(0);
                                    filePaths.add(catalogManager.getFileUri(file).getPath());
                                    inputFiles.add(file.getId());
                                }
                                localParams.put(inputParam.getName(), filePaths);
                            }
                        }

                        // Set outdir
                        String outputParam = toolManager.getExecution().getOutputParam();
                        File outdir = catalogManager.getFile(outdirId, sessionId).first();
                        localParams.put(outputParam, Collections.singletonList(catalogManager.getFileUri(outdir).getPath()));


                        QueryResult<Job> jobQueryResult = new JobFactory(catalogManager).createJob(toolManager, localParams, studyId,
                                c.name, c.description, outdir, inputFiles, sessionId, true);

                        System.out.println(createOutput(c.cOpt, jobQueryResult, null));

                        break;
                    }
                    default: {
                        optionsParser.printUsage();
                        break;
                    }
                }
                break;
            }
            case "tools" : {
                switch (optionsParser.getSubCommand()) {
                    case "create": {
                        OptionsParser.ToolCommands.CreateCommand c = optionsParser.getToolCommands().createCommand;

                        Path path = Paths.get(c.path);
                        FileUtils.checkDirectory(path);

                        QueryResult<Tool> tool = catalogManager.createTool(c.alias, c.description, null, null,
                                path.toAbsolutePath().toString(), c.openTool, sessionId);
                        System.out.println(createOutput(c.cOpt, tool, null));

                        break;
                    }
                    case "info": {
                        OptionsParser.ToolCommands.InfoCommand c = optionsParser.getToolCommands().infoCommand;

                        long toolId = catalogManager.getToolId(c.id);
                        ToolManager toolManager;
                        String toolName;
                        if (toolId < 0) {
                            toolManager = new ToolManager(c.id, null);    //LEGACY MODE, AVOID USING
                            toolName = c.id;
                            System.out.println(createOutput(c.cOpt, toolManager.getManifest(), null));
                        } else {
                            Tool tool = catalogManager.getTool(toolId, sessionId).getResult().get(0);
                            toolManager = new ToolManager(Paths.get(tool.getPath()).getParent(), tool.getName(), null);
                            toolName = tool.getName();
                            System.out.println(createOutput(c.cOpt, tool, null));
                        }
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

        logout(sessionId);

        return returnValue;
    }

    private static List<QueryResult<Cohort>> createCohorts(String sessionId, long studyId, String tagmapPath, CatalogManager catalogManager, Logger logger) throws IOException, CatalogException {
        List<QueryResult<Cohort>> queryResults = new ArrayList<>();
        Properties tagmap = new Properties();
        tagmap.load(new FileInputStream(tagmapPath));
        Set<String> catalogCohorts = catalogManager.getAllCohorts(studyId, null, null, sessionId).getResult().stream().map(Cohort::getName).collect(Collectors.toSet());
        for (String cohortName : VariantAggregatedStatsCalculator.getCohorts(tagmap)) {
            if (!catalogCohorts.contains(cohortName)) {
                QueryResult<Cohort> cohort = catalogManager.createCohort(studyId, cohortName, Study.Type.COLLECTION, "", Collections.emptyList(), null, sessionId);
                queryResults.add(cohort);
            } else {
                logger.warn("cohort {} was already created", cohortName);
            }
        }
        return queryResults;
    }


    private Map<File.Bioformat, DataStore> parseBioformatDataStoreMap(OptionsParser.StudyCommands.CreateCommand c) throws Exception {
        Map<File.Bioformat, DataStore> dataStoreMap;
        dataStoreMap = new HashMap<>();
        HashSet<String> storageEnginesSet = new HashSet<>(StorageManagerFactory.get().getDefaultStorageManagerNames());
        if (c.datastores != null) {
            for (String datastore : c.datastores) {
                logger.debug("Parsing datastore {} ", datastore);
                String[] split = datastore.split(":");
                if (split.length != 3) {
                    throw new Exception("Invalid datastore. Expected <bioformat>:<storageEngineName>:<database_name>");
                } else {
                    File.Bioformat bioformat;
                    try {
                        bioformat = File.Bioformat.valueOf(split[0].toUpperCase());
                    } catch (Exception e) {
                        throw new Exception("Unknown Bioformat \"" + split[0] + "\"", e);
                    }
                    String storageEngine = split[1];
                    String dbName = split[2];
                    if (!storageEnginesSet.contains(storageEngine)) {
                        throw new Exception("Unknown StorageEngine \"" + storageEngine + "\"");
                    }
                    dataStoreMap.put(bioformat, new DataStore(storageEngine, dbName));
                }
            }
        }
        return dataStoreMap;
    }

    /* Output Formats */


    private StringBuilder createOutput(OptionsParser.CommonOptions commonOptions, QueryResult qr, StringBuilder sb) throws JsonProcessingException {
        if (commonOptions.metadata) {
            return createOutput(commonOptions, Collections.singletonList(qr), sb);
        } else {
            return createOutput(commonOptions, qr.getResult(), sb);
        }
    }

    private StringBuilder createOutput(OptionsParser.CommonOptions commonOptions, Object obj, StringBuilder sb) throws JsonProcessingException {
        return createOutput(commonOptions, Collections.singletonList(obj), sb);
    }

    private StringBuilder createOutput(OptionsParser.CommonOptions commonOptions, List list, StringBuilder sb) throws JsonProcessingException {
        if (sb == null) {
            sb = new StringBuilder();
        }
        String idSeparator = null;
        switch (commonOptions.outputFormat) {
            case IDS:
                idSeparator = idSeparator == null? "\n" : idSeparator;
            case ID_CSV:
                idSeparator = idSeparator == null? "," : idSeparator;
            case ID_LIST:
                idSeparator = idSeparator == null? "," : idSeparator;
            {
                if (!list.isEmpty()) {
                    try {
                        Iterator iterator = list.iterator();

                        Object next = iterator.next();
                        if (next instanceof QueryResult) {
                            createOutput(commonOptions, (QueryResult) next, sb);
                        } else {
                            sb.append(getId(next));
                        }
                        while (iterator.hasNext()) {
                            next = iterator.next();
                            if (next instanceof QueryResult) {
                                sb.append(idSeparator);
                                createOutput(commonOptions, (QueryResult) next, sb);
                            } else {
                                sb.append(idSeparator).append(getId(next));
                            }
                        }
                    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
                break;
            }
            case NAME_ID_MAP: {
                if (!list.isEmpty()) {
                    try {
                        Iterator iterator = list.iterator();
                        Object object = iterator.next();
                        if (object instanceof QueryResult) {
                            createOutput(commonOptions, (QueryResult) object, sb);
                        } else {
                            sb.append(getName(object)).append(":").append(getId(object));
                        }
                        while (iterator.hasNext()) {
                            object = iterator.next();
                            if (object instanceof QueryResult) {
                                sb.append(",");
                                createOutput(commonOptions, (QueryResult) object, sb);
                            } else {
                                sb.append(",").append(getName(object)).append(":").append(getId(object));
                            }
                        }
                    } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
                break;
            }
            case RAW:
                if (list != null) {
                    for (Object o : list) {
                        sb.append(String.valueOf(o));
                    }
                }
                break;
            default:
                logger.warn("Unsupported output format \"{}\" for that query", commonOptions.outputFormat);
            case PRETTY_JSON:
            case PLAIN_JSON:
                JsonFactory factory = new JsonFactory();
                ObjectMapper objectMapper = new ObjectMapper(factory);
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
                ObjectWriter objectWriter = commonOptions.outputFormat == OptionsParser.OutputFormat.PRETTY_JSON ? objectMapper.writerWithDefaultPrettyPrinter(): objectMapper.writer();

                if (list != null && !list.isEmpty()) {
                    Iterator iterator = list.iterator();
                    sb.append(objectWriter.writeValueAsString(iterator.next()));
                    while (iterator.hasNext()) {
                        sb.append("\n").append(objectWriter.writeValueAsString(iterator.next()));
                    }
                }
                break;
        }
        return sb;
    }

    private Object getId(Object object) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        return object.getClass().getMethod("getId").invoke(object);
    }

    private Object getName(Object object) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        return object.getClass().getMethod("getName").invoke(object);
    }

    private StringBuilder listProjects(List<Project> projects, int level, String indent, boolean showUries, StringBuilder sb, String sessionId) throws CatalogException {
        if (level > 0) {
            for (Iterator<Project> iterator = projects.iterator(); iterator.hasNext(); ) {
                Project project = iterator.next();
                sb.append(String.format("%s (%d) - %s : %s\n", indent + (iterator.hasNext() ? "├──" : "└──"), project.getId(), project.getName(), project.getAlias()));
                listStudies(project.getId(), level - 1, indent + (iterator.hasNext()? "│   " : "    "), showUries, sb, sessionId);
            }
        }
        return sb;
    }

    private StringBuilder listStudies(long projectId, int level, String indent, boolean showUries, StringBuilder sb, String sessionId) throws CatalogException {
        if (level > 0) {
            List<Study> studies = catalogManager.getAllStudiesInProject(projectId,
                    new QueryOptions("include", Arrays.asList("projects.studies.id", "projects.studies.name", "projects.studies.alias")),
                    sessionId).getResult();

            listStudies(studies, level, indent, showUries, sb, sessionId);
        }
        return sb;
    }

    private StringBuilder listStudies(List<Study> studies, int level, String indent, boolean showUries, StringBuilder sb, String sessionId)
            throws CatalogException {
        if (level > 0) {
            for (Iterator<Study> iterator = studies.iterator(); iterator.hasNext(); ) {
                Study study = iterator.next();
                sb.append(String.format("%s (%d) - %s : %s\n", indent.isEmpty()? "" : indent + (iterator.hasNext() ? "├──" : "└──"),
                        study.getId(), study.getName(), study.getAlias()));
                listFiles(study.getId(), ".", level - 1, indent + (iterator.hasNext() ? "│   " : "    "), showUries, sb, sessionId);
            }
        }
        return sb;
    }

    private StringBuilder listFiles(long studyId, String path, int level, String indent, boolean showUries, StringBuilder sb,
                                    String sessionId) throws CatalogException {
        if (level > 0) {
            List<File> files = catalogManager.searchFile(studyId, new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), path),
                    sessionId).getResult();
            listFiles(files, studyId, level, indent, showUries, sb, sessionId);
        }
        return sb;
    }

    private StringBuilder listFiles(List<File> files, long studyId, int level, String indent, boolean showUries, StringBuilder sb,
                                    String sessionId) throws CatalogException {
        if (level > 0) {
            files.sort((file1, file2) -> file1.getName().compareTo(file2.getName()));
//            files.sort((file1, file2) -> file1.getModificationDate().compareTo(file2.getModificationDate()));
            for (Iterator<File> iterator = files.iterator(); iterator.hasNext(); ) {
                File file = iterator.next();
//                System.out.printf("%s%d - %s \t\t[%s]\n", indent + (iterator.hasNext()? "+--" : "L--"), file.getId(), file.getName(), file.getName());
                sb.append(String.format("%s (%d) - %s   [%s, %s]%s\n",
                        indent.isEmpty() ? "" : indent + (iterator.hasNext() ? "├──" : "└──"),
                        file.getId(),
                        file.getName(),
                        file.getStatus().getName(),
                        humanReadableByteCount(file.getDiskUsage(), false),
                        showUries && file.getUri() != null ? " --> " + file.getUri() : ""
                ));
                if (file.getType() == File.Type.DIRECTORY) {
                    listFiles(studyId, file.getPath(), level - 1, indent + (iterator.hasNext()? "│   " : "    "), showUries, sb, sessionId);
//                    listFiles(studyId, file.getPath(), level - 1, indent + (iterator.hasNext()? "| " : "  "), sessionId);
                }
            }
        }
        return sb;
    }

    /**
     * Get Bytes numbers in a human readable string
     * See http://stackoverflow.com/a/3758880
     *
     * @param bytes     Quantity of bytes
     * @param si        Use International System (power of 10) or Binary Units (power of 2)
     * @return
     */
    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    private String login(OptionsParser.UserAndPasswordOptions up) throws CatalogException, IOException {
        String sessionId;
        if (up.hiddenPassword != null && !up.hiddenPassword.isEmpty()) {
            up.password = up.hiddenPassword;
        }
        if(up.user != null && up.password != null) {
            QueryResult<ObjectMap> login = catalogManager.login(up.user, up.password, "localhost");
            sessionId = login.getResult().get(0).getString("sessionId");
//            userId = up.user;
            logoutAtExit = true;
        } else if (up.sessionId != null) {
            sessionId = up.sessionId;
//            userId = up.user;
            logoutAtExit = false;
        } else if (shellSessionId != null && !shellSessionId.isEmpty()){
            sessionId = shellSessionId;
            logoutAtExit = false;
        } else {
            if (sessionFile != null && sessionFile.getSessionId() != null && !sessionFile.getSessionId().isEmpty()) {
                shellSessionId = sessionFile.getSessionId();
                shellUserId = sessionFile.getUserId();
                sessionId = sessionFile.getSessionId();
                logoutAtExit = false;
                sessionIdFromFile = true;
            }
            else {
                sessionId = null;
            }
        }
        return sessionId;

    }

    private void logout(String sessionId) throws CatalogException, IOException {
//        if(sessionId != null && !sessionId.equals(shellSessionId)){
        if(logoutAtExit){
            catalogManager.logout(catalogManager.getUserIdBySessionId(sessionId), sessionId);
        }
    }

    private static void setLogLevel(String logLevel) {
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.cluster").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.connection").setLevel(Level.WARN);
// This small hack allow to configure the appropriate Logger level from the command line, this is done
// by setting the DEFAULT_LOG_LEVEL_KEY before the logger object is created.
//        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel);
        org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();
//        rootLogger.setLevel(Level.toLevel(logLevel));

        ConsoleAppender stderr = (ConsoleAppender) rootLogger.getAppender("stderr");
        stderr.setThreshold(Level.toLevel(logLevel));

        logger = LoggerFactory.getLogger(OpenCGAMainOld.class);
    }

    private static SessionFile loadUserFile() throws IOException {
        java.io.File file = Paths.get(System.getProperty("user.home"), ".opencga", "opencga.yml").toFile();
        if (file.exists()) {
            return new ObjectMapper(new YAMLFactory()).readValue(file, SessionFile.class);
        } else {
            return new SessionFile();
        }
    }

    private static void saveUserFile(SessionFile sessionFile) throws IOException {
        Path opencgaDirectoryPath = Paths.get(System.getProperty("user.home"), ".opencga");
        if (!opencgaDirectoryPath.toFile().exists()) {
            Files.createDirectory(opencgaDirectoryPath);
        }
        FileUtils.checkDirectory(opencgaDirectoryPath, true);
        java.io.File file = opencgaDirectoryPath.resolve("opencga.yml").toFile();
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        objectMapper.writeValue(file, sessionFile);
    }

}
