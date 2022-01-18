package org.opencb.opencga.app.cli.session;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliShellExecutor;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.commons.utils.PrintUtils.*;


public class CliSessionManager {

    public static final String SESSION_FILE_SUFFIX = "_session.json";
    private static final String NO_STUDY = "NO_STUDY";
    private static final String GUEST_USER = "anonymous";
    private static final String NO_TOKEN = "NO_TOKEN";
    private static CliSessionManager instance;
    private static boolean debug = false;
    private static boolean shellMode = false;
    private static OpencgaCliShellExecutor shell;
    private static CliSession session;

    private static final Logger logger = LoggerFactory.getLogger(CliSessionManager.class);

    private CliSessionManager() {
    }

    public static CliSessionManager getInstance() {
        if (instance == null) {
            instance = new CliSessionManager();
        }
        return instance;
    }

    public static void initSession() {
        CommandLineUtils.printDebug("Using: " + getLastHostUsed());
        loadCliSessionFile(getLastHostUsed());
    }

    public static boolean isDebug() {
        return debug;
    }

    public CliSessionManager setDebug(boolean debug) {
        CliSessionManager.debug = debug;
        return this;
    }

    public static boolean isShellMode() {
        return shellMode;
    }

    public CliSessionManager setShellMode(boolean shellMode) {
        CliSessionManager.shellMode = shellMode;
        return this;
    }

    public static OpencgaCliShellExecutor getShell() {
        return shell;
    }

    public static void setShell(OpencgaCliShellExecutor shell) {
        CliSessionManager.shell = shell;
    }


    public static String getLastHostUsed() {
        Path sessionDir = Paths.get(System.getProperty("user.home"), ".opencga");
        if (!Files.exists(sessionDir)) {
            return "";
        }
        Map<String, Long> mapa = new HashMap<>();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(sessionDir)) {
            for (Path path : paths) {
                if (!Files.isDirectory(path)) {
                    if (path.endsWith(SESSION_FILE_SUFFIX)) {
                        CliSession cli = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                                .readValue(path.toFile(), CliSession.class);
                        mapa.put(cli.getCurrentHost(), cli.getTimestamp());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.debug(mapa.toString());

        String res = "";
        Long max = 0L;
        for (Map.Entry<String, Long> entry : mapa.entrySet()) {
            if (entry.getValue() > max) {
                res = String.valueOf(entry.getKey());
            }
        }
        return res;
    }

    public static void loadCliSessionFile(String host) {
        Path sessionDirectory = Paths.get(System.getProperty("user.home")).resolve(".opencga");
        if (!Files.exists(sessionDirectory)) {
            try {
                Files.createDirectory(sessionDirectory);
            } catch (Exception e) {
                CommandLineUtils.printError("Could not create session dir properly", e);
            }
        }
        Path sessionPath = sessionDirectory.resolve(host + SESSION_FILE_SUFFIX);
        CommandLineUtils.printDebug("Loading " + sessionPath);
        // Check if .opencga folder exists
        System.out.println("sessionPath = " + sessionPath);
        if (!Files.exists(sessionPath)) {
            try {
                Files.createFile(sessionPath);
                session = getClearSession();
                updateCliSessionFile(host);
            } catch (Exception e) {
                CommandLineUtils.printError("Could not create session file properly", e);
            }
        } else {
            try {
                session = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                        .readValue(sessionPath.toFile(), CliSession.class);
            } catch (IOException e) {
                CommandLineUtils.printError("Could not parse the session file properly", e);
            }
        }
    }

    private static CliSession getClearSession() {
        CliSession res = new CliSession();
        res.setHost("localhost");
        res.setVersion("");
        res.setUser("anonymous");
        res.setToken(NO_TOKEN);
        res.setRefreshToken("");
        res.setLogin("");
        res.setCurrentStudy("NO_STUDY");
        res.setStudies(Collections.emptyList());
        res.setCurrentHost("localhost");
        res.setTimestamp(System.currentTimeMillis());
        return res;

    }

    public static void updateCliSessionFile(String host) throws IOException {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", host + SESSION_FILE_SUFFIX);
        if (Files.exists(sessionPath)) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), instance);
        }
    }

    public void initSession(OpencgaCommandExecutor executor) throws ClientException {
        initSession();
        if (StringUtils.isEmpty(session.getCurrentHost())) {
            executor.getClientConfiguration().getCurrentHost().setName(session.getCurrentHost());
        } else {
            executor.getClientConfiguration().getCurrentHost().setName(executor.getClientConfiguration().getRest().getHosts().get(0).getName());
        }
        CommandLineUtils.printDebug("Current host:::  " + executor.getClientConfiguration().getCurrentHost().getName());

    }

    public String getToken() {
        return session.getToken();
    }

    public String getUser() {
        return session.getUser();
    }

    public String getRefreshToken() {
        return session.getRefreshToken();
    }

    public void logoutCliSessionFile(OpencgaCommandExecutor executor) throws IOException, ClientException {
        session = getClearSession();
        updateSession(executor);
    }

    public void setValidatedCurrentStudy(String arg, OpencgaCommandExecutor executor) {
        if (!StringUtils.isEmpty(session.getToken())) {
            CommandLineUtils.printDebug("Check study " + arg);
            // FIXME This needs to be refactor
            //TODO Nacho must check the refactorized code
            OpenCGAClient openCGAClient = executor.getOpenCGAClient();
            if (openCGAClient != null) {
                try {
                    RestResponse<Study> res = openCGAClient.getStudyClient().info(arg, new ObjectMap());
                    if (res.allResultsSize() > 0) {
                        CommandLineUtils.printDebug("Validated study " + arg);
                        session.setCurrentStudy(res.response(0).getResults().get(0).getFqn());
                        CommandLineUtils.printDebug("Validated study " + arg);
                        updateSession(executor);
                        println(getKeyValueAsFormattedString("Current study is: ",
                                session.getCurrentStudy()));
                    } else {
                        printWarn("Invalid study");
                    }
                } catch (ClientException e) {
                    CommandLineUtils.printError(e.getMessage(), e);
                }
            } else {
                printError("Client not available");
            }
        } else {
            printWarn("To set a study you must be logged in");
        }
    }

    public String getCurrentFile() {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga",
                getLastHostUsed() + SESSION_FILE_SUFFIX);

        return sessionPath.toString();
    }

    public void updateSessionToken(String token, OpencgaCommandExecutor executor) throws ClientException {
        session.setToken(token);
        session.setHost(executor.getClientConfiguration().getCurrentHost().getUrl());
        session.setCurrentHost(executor.getClientConfiguration().getCurrentHost().getName());
        updateSession(executor);
    }

    public String getCurrentStudy() {
        return session.getCurrentStudy();
    }

    public List<String> getStudies() {
        return session.getStudies();
    }

    public void initUserSession(String token, String user, String refreshToken, List<String> studies, OpencgaCommandExecutor executor) throws IOException {

        session.setToken(token);
        session.setUser(user);
        session.setVersion(GitRepositoryState.get().getBuildVersion());
        session.setRefreshToken(refreshToken);
        session.setStudies(studies);
        session.setLogin(TimeUtils.getTime(new Date()));
        updateSession(executor);
    }

    public void updateSession(OpencgaCommandExecutor executor) {
        try {
            CommandLineUtils.printDebug("Updating session for host " + executor.getClientConfiguration().getCurrentHost().getName());
            saveCliSessionFile(executor.getClientConfiguration().getCurrentHost().getName());
            CommandLineUtils.printDebug("Session updated for ");
        } catch (Exception e) {
            CommandLineUtils.printError(e.getMessage(), e);
        }
    }


    public String getPrompt() {
        String host = format("[" + session.getCurrentHost() + "]", Color.GREEN);
        String study = format("[" + session.getCurrentStudy() + "]", Color.BLUE);
        String user = format("<" + session.getUser() + "/>", Color.YELLOW);
        return host + study + user;
    }

    public void loadSessionStudies(OpencgaCommandExecutor executor) {
        if (!StringUtils.isEmpty(session.getToken())) {
            OpenCGAClient openCGAClient = executor.getOpenCGAClient();
            try {
                RestResponse<Project> res = openCGAClient.getProjectClient().search(new ObjectMap());
                List<String> studies = new ArrayList<>();
                List<OpenCGAResult<Project>> responses = res.getResponses();
                for (OpenCGAResult<Project> p : responses) {
                    for (Project project : p.getResults()) {
                        for (Study study : project.getStudies()) {
                            studies.add(study.getFqn());
                        }
                    }
                }
                if (!studies.isEmpty()) {
                    session.setStudies(studies);
                    if (!studies.contains(session.getCurrentStudy())) {
                        boolean enc = false;
                        for (String study : session.getStudies()) {
                            if (study.startsWith(session.getUser())) {
                                session.setCurrentStudy(study);
                                enc = true;
                                break;
                            }
                        }
                        if (!enc) {
                            session.setCurrentStudy(session.getStudies().get(0));
                        }
                    }
                    updateSession(executor);
                }
            } catch (Exception e) {
                CommandLineUtils.printError("Reloading studies failed ", e);
            }
        }
    }

    public void setDefaultCurrentStudy(OpencgaCommandExecutor executor) throws IOException {
        if ((session.getCurrentStudy().equals(CliSessionManager.NO_STUDY) ||
                !session.getStudies().contains(session.getCurrentStudy()))) {
            if (CollectionUtils.isNotEmpty(session.getStudies())) {
                for (String study : session.getStudies()) {
                    if (study.startsWith(session.getUser())) {
                        session.setCurrentStudy(study);
                    }
                }
                if (session.getCurrentStudy().equals(CliSessionManager.NO_STUDY)) {
                    session.setCurrentStudy(session.getStudies().get(0));
                }
                updateSession(executor);
            } else if (session.getUser().equals(CliSessionManager.GUEST_USER)) {

                OpenCGAClient openCGAClient = executor.getOpenCGAClient();
                try {
                    RestResponse<Project> res = openCGAClient.getProjectClient().search(new ObjectMap());
                    setUserStudies(res, executor);
                } catch (ClientException e) {
                    CommandLineUtils.printError("Reloading projects failed: ", e);
                }
            } else {
                OpenCGAClient openCGAClient = executor.getOpenCGAClient();
                try {
                    RestResponse<Project> res = openCGAClient.getUserClient().projects(session.getUser(), new ObjectMap());
                    setUserStudies(res, executor);
                } catch (ClientException e) {
                    CommandLineUtils.printError("Reloading projects failed ", e);
                }
            }
        }
    }

    private void setUserStudies(RestResponse<Project> res, OpencgaCommandExecutor executor) throws IOException {
        List<String> studies = new ArrayList<>();
        List<OpenCGAResult<Project>> responses = res.getResponses();
        for (OpenCGAResult<Project> p : responses) {
            for (Project project : p.getResults()) {
                for (Study study : project.getStudies()) {
                    studies.add(study.getFqn());
                }
            }
        }
        if (!studies.isEmpty()) {
            session.setStudies(studies);
            setDefaultCurrentStudy(executor);
        }
        updateSession(executor);
    }

    public void saveCliSessionFile(String host) throws IOException {
        // Check the home folder exists
        if (!Files.exists(Paths.get(System.getProperty("user.home")))) {
            System.out.println("WARNING: Could not store token. User home folder '" + System.getProperty("user.home")
                    + "' not found. Please, manually provide the token for any following command lines with '-S {token}'.");
            return;
        }

        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga");
        // check if ~/.opencga folder exists
        if (!Files.exists(sessionPath)) {
            Files.createDirectory(sessionPath);
        }
        logger.debug("Save session: {}", host);
        sessionPath = sessionPath.resolve(host + SESSION_FILE_SUFFIX);

        // we remove the part where the token signature is to avoid key verification
      /*  if (StringUtils.isNotEmpty(token) &&) {
            int i = token.lastIndexOf('.');
            String withoutSignature = token.substring(0, i + 1);
            Date expiration = Jwts.parser().parseClaimsJwt(withoutSignature).getBody().getExpiration();
            instance.setExpirationTime(TimeUtils.getTime(expiration));
        }*/
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), session);
    }

    public ClientConfiguration getShellClientConfiguration() {
        return shell.getClientConfiguration();
    }

    public boolean existsToken() {
        return !session.getToken().equals(NO_TOKEN);
    }


}
