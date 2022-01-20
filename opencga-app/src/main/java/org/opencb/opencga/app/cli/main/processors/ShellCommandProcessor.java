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


public class ShellCommandProcessor extends CommandProcessor {


    public ShellCommandProcessor(ParamParser parser) {
        super(parser);
    }

    public void processCommand(OpencgaCommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser) {
        if (commandExecutor != null) {
            try {
                commandExecutor.execute();
                commandExecutor.getSessionManager().saveCliSession();
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
                    commandExecutor.getSessionManager().getCliSession().setStudies(studies);
                    if (!studies.contains(commandExecutor.getSessionManager().getCurrentStudy())) {
                        boolean enc = false;
                        for (String study : commandExecutor.getSessionManager().getStudies()) {
                            if (study.startsWith(commandExecutor.getSessionManager().getUser())) {
                                commandExecutor.getSessionManager().getCliSession().setCurrentStudy(study);
                                enc = true;
                                break;
                            }
                        }
                        if (!enc) {
                            commandExecutor.getSessionManager().getCliSession().setCurrentStudy(commandExecutor.getSessionManager().getStudies().get(0));
                        }
                    }
                    commandExecutor.getSessionManager().saveCliSession();
                }
            } catch (Exception e) {
                CommandLineUtils.error("Reloading studies failed ", e);

            }
            CommandLineUtils.debug("Session studies: " + commandExecutor.getSessionManager().getStudies().toString());
            CommandLineUtils.debug("Current study: " + commandExecutor.getSessionManager().getCliSession().getCurrentStudy());
        }


    }


}
