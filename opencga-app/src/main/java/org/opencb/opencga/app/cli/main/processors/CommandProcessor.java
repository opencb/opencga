package org.opencb.opencga.app.cli.main.processors;

import com.beust.jcommander.ParameterException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.executors.ExecutorProvider;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.session.Session;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;

import java.util.ArrayList;
import java.util.List;

import static org.opencb.commons.utils.PrintUtils.printWarn;

public class CommandProcessor {


    public void process(String[] args) {
        OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();
        try {
            if (!ArrayUtils.isEmpty(args)) {
                //2. Parse params of options files
                cliOptionsParser.parse(args);
                if (!ArrayUtils.isEmpty(args)) {
                    CommandLineUtils.debug("PARSED OPTIONS ::: " + ArrayUtils.toString(args));
                    try {
                        // 3. Check if a command has been provided is valid
                        String parsedCommand = cliOptionsParser.getCommand();
                        CommandLineUtils.debug("COMMAND TO EXECUTE ::: " + ArrayUtils.toString(args));
                        if (cliOptionsParser.isValid(parsedCommand)) {
                            // 4. Get command executor from ExecutorProvider
                            CommandLineUtils.debug("COMMAND AND SUBCOMMAND ARE VALID");
                            String parsedSubCommand = cliOptionsParser.getSubCommand();

                            OpencgaCommandExecutor commandExecutor = ExecutorProvider.getOpencgaCommandExecutor(cliOptionsParser, parsedCommand);
                            // 5. Execute parsed command with executor provided using CommandProcessor Implementation
                            CommandLineUtils.debug("EXECUTING ::: " + ArrayUtils.toString(args));

                            if (commandExecutor != null) {
                                try {
                                    commandExecutor.execute();
                                    commandExecutor.getSessionManager().saveSession();
                                    loadSessionStudies(commandExecutor);
                                } catch (Exception ex) {
                                    CommandLineUtils.error("Execution error: " + ex.getMessage(), ex);
                                    //System.exit(1);
                                }
                            } else {
                                cliOptionsParser.printUsage();
                                System.exit(1);
                            }
                        } else {
                            cliOptionsParser.printUsage();
                        }
                    } catch (ParameterException e) {
                        printWarn("\n" + e.getMessage());
                        cliOptionsParser.printUsage();
                    } catch (CatalogAuthenticationException e) {
                        printWarn("\n" + e.getMessage());
                    }
                }
            }

        } catch (Exception e) {
            CommandLineUtils.error(e);
            cliOptionsParser.printUsage();
        }

    }


    public void loadSessionStudies(OpencgaCommandExecutor commandExecutor) {
        Session session = commandExecutor.getSessionManager().getSession();
        if (!StringUtils.isEmpty(session.getToken()) && !SessionManager.NO_TOKEN.equals(session.getToken())) {
            CommandLineUtils.debug("Loading session studies using token: "
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
                    CommandLineUtils.debug("Retrieved studies: " + studies);

                    session.setStudies(studies);

                    CommandLineUtils.debug("Session studies: " + session.getStudies());

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
                        CommandLineUtils.debug("Current study: " + session.getCurrentStudy());

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
                e.printStackTrace();
                CommandLineUtils.error("Session studies: " + commandExecutor.getSessionManager().getSession().getStudies().toString(), e);
                CommandLineUtils.error("Current study: " + commandExecutor.getSessionManager().getSession().getCurrentStudy(), e);
            }
            CommandLineUtils.debug("Session studies: " + commandExecutor.getSessionManager().getSession().getStudies().toString());
            CommandLineUtils.debug("Current study: " + commandExecutor.getSessionManager().getSession().getCurrentStudy());
        }
    }


}
