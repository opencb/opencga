package org.opencb.opencga.app.cli.main.processors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaMain;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.commons.utils.PrintUtils.*;


public class ShellProcessor extends AbstractProcessor {

    public ShellProcessor() {
        super();
    }

    public boolean parseParams(String[] args) throws CatalogAuthenticationException {
        CommandLineUtils.printDebug("Executing " + String.join(" ", args));
        if (ArrayUtils.contains(args, "--host")) {
            printDebug("To change host you must exit the shell and launch it again with the --host parameter.");
            return false;
        }

        if (args.length == 1 && "exit".equals(args[0].trim())) {
            println("\nThanks for using OpenCGA. See you soon.\n\n", Color.YELLOW);
            System.exit(0);
        }

        if (args.length == 3 && "use".equals(args[0]) && "study".equals(args[1])) {
            setValidatedCurrentStudy(args[2], OpencgaMain.getShell());
            return false;
        }

        //Is for scripting login method
        if (isNotHelpCommand(args)) {
            if (args.length > 3 && "users".equals(args[0]) && "login".equals(args[1]) && ArrayUtils.contains(args, "--user-password")) {
                char[] passwordArray =
                        console.readPassword(format("\nEnter your password: ", Color.GREEN));
                args = ArrayUtils.addAll(args, "--password", new String(passwordArray));

            }
        }

        return true;
    }


    public void executeCommand(OpencgaCommandExecutor commandExecutor, OpencgaCliOptionsParser cliOptionsParser) {
        if (commandExecutor != null) {
            try {
                commandExecutor.execute();
                commandExecutor.getSessionManager().saveCliSession();
                loadSessionStudies(commandExecutor);
            } catch (IOException e) {
                CommandLineUtils.printError("Could not set the default study", e);
                System.exit(1);
            } catch (Exception ex) {
                CommandLineUtils.printError("Execution error: " + ex.getMessage(), ex);
                System.exit(1);
            }
        } else {
            cliOptionsParser.printUsage();
            System.exit(1);
        }
    }


    public void loadSessionStudies(OpencgaCommandExecutor commandExecutor) {
        if (isValidToken(commandExecutor.getSessionManager().getToken())) {
            CommandLineUtils.printDebug("Loading session studies using token: " + commandExecutor.getSessionManager().getToken());
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
                CommandLineUtils.printError("Reloading studies failed ", e);
            }
        }
    }

    private boolean isValidToken(String token) {
        return !StringUtils.isEmpty(token) && !SessionManager.NO_TOKEN.equals(token);
    }

    public void setValidatedCurrentStudy(String arg, OpencgaCommandExecutor commandExecutor) {
        if (!StringUtils.isEmpty(commandExecutor.getSessionManager().getToken())) {
            CommandLineUtils.printDebug("Check study " + arg);
            // FIXME This needs to be refactor
            //TODO Nacho must check the refactorized code
            OpenCGAClient openCGAClient = commandExecutor.getOpenCGAClient();
            if (openCGAClient != null) {
                try {
                    RestResponse<Study> res = openCGAClient.getStudyClient().info(arg, new ObjectMap());
                    if (res.allResultsSize() > 0) {
                        CommandLineUtils.printDebug("Validated study " + arg);
                        commandExecutor.getSessionManager().getCliSession().setCurrentStudy(res.response(0).getResults().get(0).getFqn());
                        CommandLineUtils.printDebug("Validated study " + arg);
                        commandExecutor.getSessionManager().saveCliSession();
                        println(getKeyValueAsFormattedString("Current study is: ",
                                commandExecutor.getSessionManager().getCliSession().getCurrentStudy()));
                    } else {
                        printWarn("Invalid study");
                    }
                } catch (ClientException e) {
                    CommandLineUtils.printError(e.getMessage(), e);
                } catch (IOException e) {
                    CommandLineUtils.printError(e.getMessage(), e);
                }
            } else {
                printError("Client not available");
            }
        } else {
            printWarn("To set a study you must be logged in");
        }
    }
}
