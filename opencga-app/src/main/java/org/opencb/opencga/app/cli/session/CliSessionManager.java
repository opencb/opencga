package org.opencb.opencga.app.cli.session;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.CommandExecutor;
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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.opencb.commons.utils.PrintUtils.*;


public class CliSessionManager {

    public static final String DEFAULT_PARAMETER = "--default";
    private static CliSessionManager instance;
    private static boolean debug = false;
    private static OpencgaCliShellExecutor shell;

    private CliSessionManager() {

    }

    public static CliSessionManager getInstance() {
        if (instance == null) {
            instance = new CliSessionManager();
        }
        return instance;
    }

    public static boolean isDebug() {
        return debug;
    }

    public CliSessionManager setDebug(boolean debug) {
        CliSessionManager.debug = debug;
        return this;
    }

    public static boolean isShell() {
        return shell != null;
    }

    public static OpencgaCliShellExecutor getShell() {
        return shell;
    }

    public static void setShell(OpencgaCliShellExecutor shell) {
        CliSessionManager.shell = shell;
    }

    public static ClientConfiguration getClientConfiguration() {
        return shell.getClientConfiguration();
    }

    public static OpenCGAClient getClient() {
        return shell.getOpenCGAClient();
    }

    public void init(String[] args, CommandExecutor executor) {
        if (StringUtils.isEmpty(CliSession.getInstance().getCurrentHost())) {
            executor.getClientConfiguration().getRest().setCurrentHostname(CliSession.getInstance().getCurrentHost());
        } else {
            executor.getClientConfiguration().getRest().setCurrentHostname(
                    executor.getClientConfiguration().getRest().getHosts().get(0).getName());
        }
    }

    public void switchDefaultSessionHost() {
        switchSessionHost(getDefaultHost());
    }

    private String getDefaultHost() {
        return shell.getClientConfiguration().getRest().getHosts().get(0).getName();
    }

    public String getToken() {
        return CliSession.getInstance().getToken();
    }

    public String getUser() {
        return CliSession.getInstance().getUser();
    }

    public String getRefreshToken() {
        return CliSession.getInstance().getRefreshToken();
    }

    public void logoutCliSessionFile() throws IOException {
        if (isShell()) {
            CliSession.getInstance().logoutCliSessionFile(shell.getClientConfiguration().getRest().getCurrentHostname());
        } else {
            CliSession.getInstance().logoutCliSessionFile(getLastHostUsed());
        }
    }

    public void setValidatedCurrentStudy(String arg) {
        if (!StringUtils.isEmpty(CliSession.getInstance().getToken())) {
            CommandLineUtils.printDebug("Check study " + arg);
            // FIXME This needs to be refactor
            //TODO Nacho must check the refactorized code
            OpenCGAClient openCGAClient = shell.getOpenCGAClient();
            if (openCGAClient != null) {
                try {
                    RestResponse<Study> res = openCGAClient.getStudyClient().info(arg, new ObjectMap());
                    if (res.allResultsSize() > 0) {
                        CommandLineUtils.printDebug("Validated study " + arg);
                        CliSession.getInstance().setCurrentStudy(res.response(0).getResults().get(0).getFqn());
                        CommandLineUtils.printDebug("Validated study " + arg);
                        updateSession();
                        println(getKeyValueAsFormattedString("Current study is: ",
                                CliSession.getInstance().getCurrentStudy()));
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
                getLastHostUsed() + CliSession.SESSION_FILE_SUFFIX);

        return sessionPath.toString();
    }

    public void updateSessionToken(String token) {
        CliSession.getInstance().setToken(token);
        CliSession.getInstance().setHost(shell.getClientConfiguration().getRest().getCurrentUrl());
        CliSession.getInstance().setCurrentHost(shell.getClientConfiguration().getRest().getCurrentHostname());
        updateSession();
    }

    public String getCurrentStudy() {
        return CliSession.getInstance().getCurrentStudy();
    }

    public List<String> getStudies() {
        return CliSession.getInstance().getStudies();
    }

    public void initUserSession(String token, String user, String refreshToken, List<String> studies) throws IOException {

        CliSession.getInstance().setToken(token);
        CliSession.getInstance().setUser(user);
        CliSession.getInstance().setVersion(GitRepositoryState.get().getBuildVersion());
        CliSession.getInstance().setRefreshToken(refreshToken);
        CliSession.getInstance().setStudies(studies);
        CliSession.getInstance().setLogin(TimeUtils.getTime(new Date()));
        updateSession();
    }

    public void updateSession() {
        try {
            if (isShell()) {
                CommandLineUtils.printDebug("Updating session for host " + shell.getClientConfiguration().getRest().getCurrentHostname());
                CliSession.getInstance().saveCliSessionFile(shell.getClientConfiguration().getRest().getCurrentHostname());
            } else {
                CommandLineUtils.printDebug("Updating session for host " + getLastHostUsed());
                CliSession.getInstance().saveCliSessionFile(getLastHostUsed());
            }
            CommandLineUtils.printDebug("Session updated ");

        } catch (Exception e) {
            CommandLineUtils.printError(e.getMessage(), e);
        }
    }

    public String getLastHostUsed() {
        return CliSession.getInstance().getLastHostUsed();
    }

    public String getPrompt() {
        String host = format("[" + CliSession.getInstance().getCurrentHost() + "]", Color.GREEN);
        String study = format("[" + CliSession.getInstance().getCurrentStudy() + "]", Color.BLUE);
        String user = format("<" + CliSession.getInstance().getUser() + "/>", Color.YELLOW);
        return host + study + user;
    }

    public void loadSessionStudies(OpencgaCommandExecutor executor) {
        if (!StringUtils.isEmpty(CliSession.getInstance().getToken())) {
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

    public void setCurrentHost(String name) throws Exception {
        if (name == null) {
            throw new Exception("The name of host cannot be null");
        }
        if (!shell.getClientConfiguration().getRest().existsName(name)) {
            throw new Exception("Host not found");
        }
        shell.getClientConfiguration().getRest().setCurrentHostname(name);
        CliSessionManager.getInstance().updateSession();
    }

    public void switchSessionHost(String host) {
        try {
            CliSession.getInstance().loadCliSessionFile(host);
            setCurrentHost(host);
            CliSession.getInstance().setCurrentHost(host);
            println(getKeyValueAsFormattedString("The new host has set to: ",
                    CliSession.getInstance().getCurrentHost()));
        } catch (Exception e) {
            CommandLineUtils.printError("Failure to switch hosts", e);
        }
    }

    public void setDefaultCurrentStudy() throws IOException {
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

                OpenCGAClient openCGAClient = shell.getOpenCGAClient();
                try {
                    RestResponse<Project> res = openCGAClient.getProjectClient().search(new ObjectMap());
                    setUserStudies(res);
                } catch (ClientException e) {
                    CommandLineUtils.printError("Reloading projects failed: ", e);
                }
            } else {
                OpenCGAClient openCGAClient = shell.getOpenCGAClient();
                try {
                    RestResponse<Project> res = openCGAClient.getUserClient().projects(CliSession.getInstance().getUser(), new ObjectMap());
                    setUserStudies(res);
                } catch (ClientException e) {
                    CommandLineUtils.printError("Reloading projects failed ", e);
                }
            }
        }
    }

    private void setUserStudies(RestResponse<Project> res) throws IOException {
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

}
