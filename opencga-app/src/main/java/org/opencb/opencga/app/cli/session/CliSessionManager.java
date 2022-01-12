package org.opencb.opencga.app.cli.session;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.Color;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliShellExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class CliSessionManager {

    public static final String DEFAULT_PARAMETER = "--default";
    private static final boolean reloadStudies = false;
    private static OpencgaCliShellExecutor sessionShell;
    private static boolean debug = false;

    public static void updateSession() {
        try {
            if (isShell()) {
                CommandLineUtils.printDebugMessage("Updating session for host " + ClientConfiguration.getInstance().getRest().getCurrentHostname());
                CliSession.getInstance().saveCliSessionFile(ClientConfiguration.getInstance().getRest().getCurrentHostname());
            } else {
                CommandLineUtils.printDebugMessage("Updating session for host " + getLastHostUsed());
                CliSession.getInstance().saveCliSessionFile(getLastHostUsed());
            }
            CommandLineUtils.printDebugMessage("Session updated ");
            if (isReloadStudies()) {
                reloadStudies();
            }
        } catch (Exception e) {
            CommandLineUtils.printError(e.getMessage(), e);
        }
    }

    public static void switchSessionHost(String host) {
        try {
            CliSession.getInstance().loadCliSessionFile(host);
            setCurrentHost(host);
            CliSession.getInstance().setCurrentHost(host);
            PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("The new host has set to: ",
                    CliSession.getInstance().getCurrentHost()));

        } catch (Exception e) {
            CommandLineUtils.printError("Failure to switch hosts", e);
        }
    }

    public static void setCurrentHost(String name) throws Exception {

        if (name == null) {
            throw new Exception("The name of host cannot be null");
        }
        if (!ClientConfiguration.getInstance().getRest().existsName(name)) {
            throw new Exception("Host not found");
        }
        ClientConfiguration.getInstance().getRest().setCurrentHostname(name);
        updateSession();
    }

    public static void setDefaultCurrentStudy() throws IOException {
        if ((CliSession.getInstance().getCurrentStudy().equals(CliSession.NO_STUDY) ||
                !CliSession.getInstance().getStudies().contains(CliSession.getInstance().getCurrentStudy()))) {
            if (CollectionUtils.isNotEmpty(CliSession.getInstance().getStudies())) {
                for (String study : CliSession.getInstance().getStudies()) {
                    if (study.startsWith(CliSession.getInstance().getUser())) {
                        CliSession.getInstance().setCurrentStudy(study);
                    }
                }
                if (CliSession.getInstance().getCurrentStudy().equals(CliSession.NO_STUDY)) {
                    CliSession.getInstance().setCurrentStudy(CliSession.getInstance().getStudies().get(0));
                }

                updateSession();
            } else if (CliSession.getInstance().getUser().equals(CliSession.GUEST_USER)) {

                OpenCGAClient openCGAClient = new OpenCGAClient();
                try {
                    RestResponse<Project> res = openCGAClient.getProjectClient().search(new ObjectMap());
                    setUserStudies(res);
                } catch (ClientException e) {
                    CommandLineUtils.printError("Reloading projects failed: ", e);
                }
            } else {
                OpenCGAClient openCGAClient = new OpenCGAClient();
                try {
                    RestResponse<Project> res = openCGAClient.getUserClient().projects(CliSession.getInstance().getUser(), new ObjectMap());
                    setUserStudies(res);
                } catch (ClientException e) {
                    CommandLineUtils.printError("Reloading projects failed ", e);
                }
            }
        }
    }

    private static void setUserStudies(RestResponse<Project> res) throws IOException {
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
            CliSession.getInstance().setStudies(studies);
            setDefaultCurrentStudy();
        }
        updateSession();
    }

    public static String getLastHostUsed() {
        return CliSession.getInstance().getLastHostUsed();
    }

    public static String getPrompt() {
        String host = PrintUtils.format("[" + CliSession.getInstance().getCurrentHost() + "]", Color.GREEN);
        String study = PrintUtils.format("[" + CliSession.getInstance().getCurrentStudy() + "]", Color.BLUE);
        String user = PrintUtils.format("<" + CliSession.getInstance().getUser() + "/>", Color.YELLOW);
        return host + study + user;
    }

    private static void loadSessionStudies() {
        if (!StringUtils.isEmpty(CliSession.getInstance().getToken())) {
            OpenCGAClient openCGAClient = new OpenCGAClient();
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
                    CliSession.getInstance().setStudies(studies);
                    if (!studies.contains(CliSession.getInstance().getCurrentStudy())) {
                        boolean enc = false;
                        for (String study : CliSession.getInstance().getStudies()) {
                            if (study.startsWith(CliSession.getInstance().getUser())) {
                                CliSession.getInstance().setCurrentStudy(study);
                                enc = true;
                                break;
                            }
                        }
                        if (!enc) {
                            CliSession.getInstance().setCurrentStudy(CliSession.getInstance().getStudies().get(0));
                        }
                    }
                    updateSession();
                }
            } catch (Exception e) {
                CommandLineUtils.printError("Reloading studies failed ", e);
            }
        }
    }

    private static void reloadStudies() {
        setReloadStudies(false);
        CommandLineUtils.printDebugMessage("Reloading studies ");
        if (!StringUtils.isEmpty(CliSession.getInstance().getToken())) {
            loadSessionStudies();
        } else {
            PrintUtils.printWarn("To set a study you must be logged in");
        }
    }

    public static void setValidatedCurrentStudy(String arg) {
        if (!StringUtils.isEmpty(CliSession.getInstance().getToken())) {
            CommandLineUtils.printDebugMessage("Check study " + arg);

            OpenCGAClient openCGAClient = new OpenCGAClient(new AuthenticationResponse(CliSession.getInstance().getToken()));
            if (openCGAClient != null) {
                try {
                    
                    RestResponse<Study> res = openCGAClient.getStudyClient().info(arg, new ObjectMap());
                    if (res.allResultsSize() > 0) {
                        CommandLineUtils.printDebugMessage("Validated study " + arg);

                        CliSession.getInstance().setCurrentStudy(res.response(0).getResults().get(0).getFqn());
                        CommandLineUtils.printDebugMessage("Validated study " + arg);
                        updateSession();
                        PrintUtils.println(PrintUtils.getKeyValueAsFormattedString("Current study is: ",
                                CliSession.getInstance().getCurrentStudy()));
                    } else {
                        PrintUtils.printWarn("Invalid study");
                    }
                } catch (ClientException e) {
                    CommandLineUtils.printError(e.getMessage(), e);
                }
            } else {
                PrintUtils.printError("Client not available");
            }
        } else {
            PrintUtils.printWarn("To set a study you must be logged in");
        }
    }

    public static String getToken() {
        return CliSession.getInstance().getToken();
    }

    public static String getUser() {
        return CliSession.getInstance().getUser();
    }

    public static String getCurrentFile() {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga",
                getLastHostUsed() + CliSession.SESSION_FILE_SUFFIX);

        return sessionPath.toString();
    }

    public static void updateSessionToken(String token) {
        CliSession.getInstance().setToken(token);
        CliSession.getInstance().setHost(ClientConfiguration.getInstance().getRest().getCurrentUrl());
        CliSession.getInstance().setCurrentHost(ClientConfiguration.getInstance().getRest().getCurrentHostname());
        updateSession();
    }

    public static String getRefreshToken() {
        return CliSession.getInstance().getRefreshToken();
    }

    public static void updateTokens(String token, String refreshToken) {
        CliSession.getInstance().setToken(token);
        CliSession.getInstance().setRefreshToken(refreshToken);
        try {
            CliSession.getInstance().updateCliSessionFile(ClientConfiguration.getInstance().getRest().getCurrentHostname());
        } catch (IOException e) {
            CommandLineUtils.printError(e.getMessage(), e);
        }
    }

    public static String getCurrentStudy() {
        return CliSession.getInstance().getCurrentStudy();
    }

    public static OpencgaCliShellExecutor getShell() {
        return sessionShell;
    }

    public static boolean isShell() {
        return sessionShell != null;
    }

    public static void setShell(OpencgaCliShellExecutor shell) {
        sessionShell = shell;
    }

    public static List<String> getStudies() {
        return CliSession.getInstance().getStudies();
    }

    public static void initUserSession(String token, String user, String refreshToken, List<String> studies) throws IOException {

        setReloadStudies(true);
        CliSession.getInstance().setToken(token);
        CliSession.getInstance().setUser(user);
        CliSession.getInstance().setVersion(GitRepositoryState.get().getBuildVersion());
        CliSession.getInstance().setRefreshToken(refreshToken);
        CliSession.getInstance().setStudies(studies);
        CliSession.getInstance().setLogin(TimeUtils.getTime(new Date()));
        //CliSession.getInstance().setCurrentHost();
        updateSession();
    }

    public static void logoutCliSessionFile() throws IOException {
        if (isShell()) {
            CliSession.getInstance().logoutCliSessionFile(ClientConfiguration.getInstance().getRest().getCurrentHostname());
        } else {
            CliSession.getInstance().logoutCliSessionFile(getLastHostUsed());
        }
    }

    public static void switchDefaultSessionHost() {
        switchSessionHost(getDefaultHost());
    }

    private static String getDefaultHost() {
        return ClientConfiguration.getInstance().getRest().getHosts().get(0).getName();
    }

    public static boolean isDebug() {
        return debug;
    }

    public static void setDebug(boolean debug) {
        CliSessionManager.debug = debug;
    }

    public static void initShell() {
        try {
            OpencgaCliShellExecutor shell = new OpencgaCliShellExecutor();
            setShell(shell);
            setDefaultCurrentStudy();
        } catch (IOException e) {
            CommandLineUtils.printError(e.getMessage(), e);
        } catch (CatalogAuthenticationException e) {
            e.printStackTrace();
        }
    }

    public static void init(String[] args) {
        CliSession.getInstance();
        if (StringUtils.isEmpty(CliSession.getInstance().getCurrentHost())) {
            ClientConfiguration.getInstance().getRest().setCurrentHostname(CliSession.getInstance().getCurrentHost());
        } else {
            ClientConfiguration.getInstance().getRest().setCurrentHostname(
                    ClientConfiguration.getInstance().getRest().getHosts().get(0).getName());
        }

        setDebug(isDebugModeSet(args));
        loadSessionStudies();
    }

    private static boolean isDebugModeSet(String[] args) {
        for (String s : args) {
            if ("--debug".equals(s)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isReloadStudies() {
        return reloadStudies;
    }

    public static void setReloadStudies(boolean reloadStudies) {
        reloadStudies = reloadStudies;
    }
}
