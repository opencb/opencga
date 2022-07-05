package org.opencb.opencga.app.cli.main.impl;

import com.beust.jcommander.ParameterException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.app.cli.CliOptionsParser;
import org.opencb.commons.app.cli.main.processors.AbstractCommandProcessor;
import org.opencb.commons.app.cli.main.utils.CommandLineUtils;
import org.opencb.commons.app.cli.session.Session;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.GitRepositoryState;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.main.OpencgaCommandLine;
import org.opencb.opencga.app.cli.main.executors.ExecutorProvider;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.parser.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.utils.LoginUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.utils.JwtUtils;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.opencb.commons.utils.PrintUtils.printWarn;
import static org.opencb.commons.utils.PrintUtils.println;

public class CommandProcessorImpl extends AbstractCommandProcessor {

    private static final Logger logger = LoggerFactory.getLogger(CommandProcessorImpl.class);

    public void process(String[] args, CliOptionsParser parser) {
        OpencgaCliOptionsParser cliOptionsParser = (OpencgaCliOptionsParser) parser;
        try {
            if (!ArrayUtils.isEmpty(args)) {
                //2. Parse params of options files
                cliOptionsParser.parse(args);
                if (!ArrayUtils.isEmpty(args)) {
                    logger.debug("PARSED OPTIONS ::: " + CommandLineUtils.argsToString(args));
                    try {
                        // 3. Check if a command has been provided is valid
                        String parsedCommand = cliOptionsParser.getCommand();
                        logger.debug("COMMAND TO EXECUTE ::: " + CommandLineUtils.argsToString(args));
                        if (cliOptionsParser.isValid(parsedCommand)) {
                            // 4. Get command executor from ExecutorProvider
                            logger.debug("COMMAND AND SUBCOMMAND ARE VALID");
                            String parsedSubCommand = cliOptionsParser.getSubCommand();

                            OpencgaCommandExecutor commandExecutor = ExecutorProvider.getOpencgaCommandExecutor(cliOptionsParser, parsedCommand);
                            // 5. Execute parsed command with executor provided using CommandProcessor Implementation
                            logger.debug("EXECUTING ::: " + CommandLineUtils.argsToString(args));

                            if (commandExecutor != null) {
                                try {
                                    if (StringUtils.isNotEmpty(commandExecutor.getSessionManager().getSession().getRefreshToken())) {
                                        Date expirationDate = JwtUtils.getExpirationDate(commandExecutor.getSessionManager().getSession().getRefreshToken());
                                        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                                        logger.debug("Token expiration date -> " + sdf.format(expirationDate));
                                    }

                                    if (checkAutoRefresh(commandExecutor)) {
                                        logger.debug("Refreshing token..." + commandExecutor.getSessionManager().getSession().getRefreshToken());
                                        refreshToken(commandExecutor);
                                        logger.debug("New refresh token..." + commandExecutor.getSessionManager().getSession().getRefreshToken());

                                    }
                                    commandExecutor.execute();
                                    commandExecutor.getSessionManager().saveSession();
                                    loadSessionStudies(commandExecutor);
                                } catch (Exception ex) {
                                    CommandLineUtils.error("Execution error", ex);
                                    logger.error("Execution error", ex);
                                }
                            } else {
                                cliOptionsParser.printUsage();
                                logger.error("Command Executor NULL");
                                System.exit(1);
                            }
                        } else {
                            cliOptionsParser.printUsage();
                        }
                    } catch (ParameterException e) {
                        printWarn("\n" + e.getMessage());
                        cliOptionsParser.printUsage();
                        logger.error("Parameter error: " + e.getMessage(), e);
                    } catch (CatalogAuthenticationException e) {
                        printWarn("\n" + e.getMessage());
                        logger.error(e.getMessage(), e);

                    }
                }
            }

        } catch (Exception e) {
            CommandLineUtils.error(e);
            cliOptionsParser.printUsage();
            logger.error(e.getMessage(), e);

        }

    }

    public String[] processShortCuts(String[] args) {
        OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();
        switch (CommandLineUtils.getShortcut(args)) {
            case "login":
                return LoginUtils.parseLoginCommand(args);
            case "--help":
            case "help":
            case "-h":
            case "?":
                if (ArrayUtils.contains(args, "help")) {
                    for (int i = 0; i < args.length; i++) {
                        if (args[i].equals("help") || args[i].equals("?") || args[i].equals("-h")) {
                            args[i] = "--help";
                        }
                    }
                }
                try {
                    cliOptionsParser.printUsage(args);
                } catch (Exception e) {
                    // malformed command
                    return args;
                }
                break;
            case "--version":
            case "version":
                println(CommandLineUtils.getVersionString());
                break;
            case "--build-version":
            case "build-version":
                println(GitRepositoryState.get().getBuildVersion());
                break;
            case "logout":
                return ArrayUtils.addAll(new String[]{"users"}, args);
            case "list":
                if (OpencgaCommandLine.isShellMode()) {
                    if (args.length > 1 && args[1].equals("studies")) {
                        println(String.join(", ", OpencgaCommandLine.getCommandLine().getShell().getSessionManager().getSession().getStudies()), PrintUtils.Color.GREEN);
                    } else {
                        printWarn("Opencga version " + GitRepositoryState.get().getBuildVersion() + " can only list studies");
                    }
                } else {
                    printWarn("List studies is only available in Shell mode");
                }
                break;
            default:
                return args;
        }
        return null;
    }


    private void refreshToken(OpencgaCommandExecutor commandExecutor) throws ClientException, IOException {
        AuthenticationResponse response = commandExecutor.getOpenCGAClient().
                refresh(commandExecutor.getSessionManager().getSession().getRefreshToken());
        commandExecutor.refreshToken(response);

    }

    private boolean checkAutoRefresh(OpencgaCommandExecutor commandExecutor) {
        if (StringUtils.isEmpty(commandExecutor.getSessionManager().getSession().getRefreshToken())) {
            return false;
        }
        if (commandExecutor.getClientConfiguration().getRest().isTokenAutoRefresh()) {
            Date expirationDate = JwtUtils.getExpirationDate(commandExecutor.getSessionManager().getSession().getRefreshToken());
            Date now = new Date();
            Calendar cal = Calendar.getInstance();
            cal.setTime(now);
            cal.add(Calendar.MINUTE, 10);
            Date refreshDate = cal.getTime();
            return expirationDate.getTime() >= now.getTime() && expirationDate.getTime() <= refreshDate.getTime();
        }
        return false;
    }

    public void loadSessionStudies(OpencgaCommandExecutor commandExecutor) {
        Session session = commandExecutor.getSessionManager().getSession();
        if (!StringUtils.isEmpty(session.getToken()) && !SessionManagerImpl.NO_TOKEN.equals(session.getToken())) {
            logger.debug("Loading session studies using token: "
                    + session.getToken());

            OpenCGAClient openCGAClient = commandExecutor.getOpenCGAClient();
            try {
                // Query the server to retrieve the studies of user projects
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

                if (CollectionUtils.isNotEmpty(studies)) {
                    //Save all the user studies in session
                    logger.debug("Retrieved studies: " + studies);

                    session.setStudies(studies);

                    logger.debug("Session studies: " + session.getStudies());

                    //If the List of studies retrieved doesn't contain the current study, must select the new one
                    if (!studies.contains(session.getCurrentStudy())) {
                        boolean find = false;
                        for (String study : studies) {
                            // If it has found any study of which the user is the
                            // owner saves it as the current study
                            if (study.startsWith(session.getUser())) {
                                session.setCurrentStudy(study);

                                find = true;
                                break;
                            }
                        }
                        logger.debug("Current study: " + session.getCurrentStudy());

                        // If none is found, save the first one as the current study
                        if (!find) {
                            session.setCurrentStudy(studies.get(0));
                        }
                    }
                    // The last step is to save the session in file
                    commandExecutor.getSessionManager().saveSession(session);
                }
            } catch (Exception e) {
                CommandLineUtils.error("Failure reloading studies ", e);
                logger.error("Failure reloading studies ", e);
            }
            logger.debug("Session studies: " + commandExecutor.getSessionManager().getSession().getStudies().toString());
            logger.debug("Current study: " + commandExecutor.getSessionManager().getSession().getCurrentStudy());
        }
    }


}
