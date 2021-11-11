package org.opencb.opencga.app.cli.session;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.OpencgaCliShellExecutor;
import org.opencb.opencga.app.cli.main.OpencgaMain;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.config.Host;
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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.fusesource.jansi.Ansi.Color.*;
import static org.fusesource.jansi.Ansi.ansi;

public class CliSessionManager {

    public static Map<String, String> hosts = new HashMap<>();
    private static OpencgaCliShellExecutor sessionShell;

    static {
        List<Host> availableHosts = ClientConfiguration.getInstance().getRest().getHosts();
        availableHosts.add(new Host(ClientConfiguration.getInstance().getRest().getHostname(),
                ClientConfiguration.getInstance().getRest().getUrl(), true));
        for (Host h : availableHosts) {
            hosts.put(h.getName(), h.getUrl());
        }
    }

    public static void switchSessionHost(String host) {
        try {
            CliSession.getInstance().loadCliSessionFile(host);
            setCurrentHost(host);
            CliSession.getInstance().setCurrentHost(host);
            try {
                CliSession.getInstance().saveCliSessionFile(host);
            } catch (Exception e) {
                OpencgaMain.printErrorMessage("Error updating session file", e);
            }
            OpencgaCliShellExecutor.printlnGreen("The new host has set to " + CliSession.getInstance().getCurrentHost());
        } catch (Exception e) {
            OpencgaMain.printErrorMessage(e.getMessage(), e);
        }
    }

    public static void setCurrentHost(String name) throws Exception {

        if (name == null) {
            throw new Exception("The name of host cannot be null");
        }
        if (!MapUtils.isEmpty(hosts)) {
            String url = hosts.get(name);
            if (url == null) {
                throw new Exception("Host not found");
            }
            ClientConfiguration.getInstance().getRest().setHostname(name);
            ClientConfiguration.getInstance().getRest().setUrl(url);
        } else {
            throw new Exception("There are no servers configured");
        }
    }

    public static void setDefaultCurrentStudy() throws IOException {
        if ((CliSession.getInstance().getCurrentStudy().equals(CliSession.NO_STUDY) || !CliSession.getInstance().getStudies().contains(CliSession.getInstance().getCurrentStudy()))) {

            if (CollectionUtils.isNotEmpty(CliSession.getInstance().getStudies())) {
                for (String study : CliSession.getInstance().getStudies()) {
                    if (study.startsWith(CliSession.getInstance().getUser())) {
                        CliSession.getInstance().setCurrentStudy(study);
                    }
                }
                if (CliSession.getInstance().getCurrentStudy().equals(CliSession.NO_STUDY)) {
                    CliSession.getInstance().setCurrentStudy(CliSession.getInstance().getStudies().get(0));
                }
                String host = "";
                if (isShell()) {
                    host = ClientConfiguration.getInstance().getRest().getHostname();
                } else {
                    host = getLastHostUsed();
                }
                CliSession.getInstance().saveCliSessionFile(host);
            } else if (CliSession.getInstance().getUser().equals(CliSession.GUEST_USER)) {
                List<String> studies = new ArrayList<>();
                OpenCGAClient openCGAClient = new OpenCGAClient();
                try {
                    RestResponse<Project> res = openCGAClient.getProjectClient().search(new ObjectMap());
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
                } catch (ClientException e) {
                    e.printStackTrace();
                }
            }
            try {
                CliSession.getInstance().saveCliSessionFile(ClientConfiguration.getInstance().getRest().getHostname());
            } catch (IOException e) {
                OpencgaMain.printErrorMessage("Error updating session file", e);
            }
        }
    }

    public static String getLastHostUsed() {
        Path sessionDir = Paths.get(System.getProperty("user.home"), ".opencga");
        if (!Files.exists(sessionDir)) {
            return ClientConfiguration.getInstance().getRest().getHostname();
        }
        Map<String, Long> mapa = new HashMap<String, Long>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sessionDir)) {
            for (Path path : stream) {
                if (!Files.isDirectory(path)) {
                    if (path.endsWith(CliSession.SESSION_FILENAME)) {
                        CliSession cli = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                                .readValue(path.toFile(), CliSession.class);
                        mapa.put(cli.getCurrentHost(), cli.getTimestamp());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String res = "";
        Long max = new Long(0);
        for (Map.Entry entry : mapa.entrySet()) {
            if (((Long) entry.getValue()) > max) {
                res = String.valueOf(entry.getKey());
            }
        }
        return res;
    }

    public static String getPrompt() {
        return String.valueOf(ansi().fg(GREEN).a(
                "[" + CliSession.getInstance().getCurrentHost() + "]").fg(BLUE).a("[" + CliSession.getInstance().getCurrentStudy() + "]").fg(YELLOW).a("<"
                + CliSession.getInstance().getUser() + "/>").reset());
    }

    public static void setCurrentStudy(String arg) {
        if (!StringUtils.isEmpty(CliSession.getInstance().getToken())) {
            OpenCGAClient openCGAClient = new OpenCGAClient(new AuthenticationResponse(CliSession.getInstance().getToken()));
            if (openCGAClient != null) {
                try {
                    RestResponse<Study> res = openCGAClient.getStudyClient().info(arg, new ObjectMap());
                    if (res.allResultsSize() > 0) {
                        CliSession.getInstance().setCurrentStudy(res.response(0).getResults().get(0).getFqn());
                        //String user, String token, String refreshToken, List<String> studies,String currentStudy
                        try {
                            CliSession.getInstance().saveCliSessionFile(ClientConfiguration.getInstance().getRest().getHostname());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        OpencgaMain.printInfoMessage("Current study is " + CliSession.getInstance().getCurrentStudy());
                    } else {
                        OpencgaMain.printWarningMessage("Invalid study");
                    }
                } catch (ClientException e) {
                    OpencgaMain.printErrorMessage(e.getMessage(), e);
                    e.printStackTrace();
                }
            } else {
                OpencgaMain.printErrorMessage("Client not available");
            }
        } else {
            OpencgaMain.printErrorMessage("To set a study you must be logged in");
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
                getLastHostUsed() + CliSession.SESSION_FILENAME);
        return sessionPath.toString();
    }

    public static void updateSession(String token) {
        CliSession.getInstance().setToken(token);
        CliSession.getInstance().setHost(ClientConfiguration.getInstance().getRest().getUrl());
        CliSession.getInstance().setCurrentHost(ClientConfiguration.getInstance().getRest().getUrl());
        try {
            CliSession.getInstance().saveCliSessionFile(ClientConfiguration.getInstance().getRest().getHostname());
        } catch (IOException e) {
            OpencgaMain.printErrorMessage(e.getMessage(), e);
        }
    }

    public static String getRefreshToken() {
        return CliSession.getInstance().getRefreshToken();
    }

    public static void updateTokens(String token, String refreshToken) {
        CliSession.getInstance().setToken(token);
        CliSession.getInstance().setRefreshToken(refreshToken);
        try {
            CliSession.getInstance().updateCliSessionFile(ClientConfiguration.getInstance().getRest().getHostname());
        } catch (IOException e) {
            OpencgaMain.printErrorMessage(e.getMessage(), e);
        }
    }

    public static String getCurrentStudy() {
        return CliSession.getInstance().getCurrentStudy();
    }

    public static OpencgaCliShellExecutor getShell() {
        return sessionShell;
    }

    public static void setShell(OpencgaCliShellExecutor shell) {
        sessionShell = shell;
    }

    public static boolean isShell() {
        return sessionShell != null;
    }

    public static List<String> getStudies() {
        return CliSession.getInstance().getStudies();
    }

    public static void initUserSession(String token, String user, String refreshToken, List<String> studies) throws IOException {
        CliSession.getInstance().setToken(token);
        CliSession.getInstance().setUser(user);
        CliSession.getInstance().setVersion(GitRepositoryState.get().getBuildVersion());
        CliSession.getInstance().setRefreshToken(refreshToken);
        CliSession.getInstance().setStudies(studies);
        CliSession.getInstance().setLogin(TimeUtils.getTime(new Date()));
        CliSession.getInstance().saveCliSessionFile(ClientConfiguration.getInstance().getRest().getHostname());
    }

    public static void logoutCliSessionFile() throws IOException {
        if (isShell()) {
            CliSession.getInstance().logoutCliSessionFile(ClientConfiguration.getInstance().getRest().getHostname());
        } else {
            CliSession.getInstance().logoutCliSessionFile(getLastHostUsed());
        }
    }
}
