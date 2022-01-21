package org.opencb.opencga.app.cli.main.processors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.parser.ParamParser;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ShellCommandProcessor extends AbstractCommandProcessor {


    public ShellCommandProcessor(ParamParser parser) {
        super(parser);
    }

    public void processCommand(OpencgaCommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser) {
        if (commandExecutor != null) {
            try {
                commandExecutor.execute();
                commandExecutor.getSessionManager().saveSession();
                // After executing the command, the studies must be reloaded in case they have changed
                loadSessionStudies(commandExecutor);
            } catch (IOException e) {
                CommandLineUtils.error("Could not set the default study", e);
                System.exit(1);
            } catch (Exception ex) {
                CommandLineUtils.error("Execution error: " + ex.getMessage(), ex);

                System.exit(1);
            }
        } else {
            cliOptionsParser.printUsage();
            System.exit(1);
        }
    }


    public void loadSessionStudies(OpencgaCommandExecutor commandExecutor) {
        if (commandExecutor.getSessionManager().hasSessionToken()) {
            CommandLineUtils.debug("Loading session studies using token: "
                    + commandExecutor.getSessionManager().getToken());

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

                if (!studies.isEmpty()) {
                    //Save all the user studies in session
                    commandExecutor.getSessionManager().getSession().setStudies(studies);
                    if (!studies.contains(commandExecutor.getSessionManager().getCurrentStudy())) {
                        boolean find = false;
                        for (String study : commandExecutor.getSessionManager().getStudies()) {
                            // If it has found any study of which the user is the
                            // owner saves it as the current study
                            if (study.startsWith(commandExecutor.getSessionManager().getUser())) {
                                commandExecutor.getSessionManager().getSession().setCurrentStudy(study);
                                find = true;
                                break;
                            }
                        }
                        // If none is found, save the first one as the current study
                        if (!find) {
                            commandExecutor.getSessionManager().getSession().setCurrentStudy(commandExecutor.getSessionManager().getStudies().get(0));
                        }
                    }
                    // The last step is to save the session in file
                    commandExecutor.getSessionManager().saveSession();
                }
            } catch (Exception e) {
                CommandLineUtils.error("Reloading studies failed ", e);

            }
            CommandLineUtils.debug("Session studies: " + commandExecutor.getSessionManager().getStudies().toString());
            CommandLineUtils.debug("Current study: " + commandExecutor.getSessionManager().getSession().getCurrentStudy());
        }


    }


}
