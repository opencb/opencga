package org.opencb.opencga.app.cli.main.parser;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.OpencgaMain;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;

import static org.opencb.commons.utils.PrintUtils.*;


public class ShellParamParser implements ParamParser {


    public String[] parseParams(String[] args) throws CatalogAuthenticationException {
        CommandLineUtils.printLog("Executing " + String.join(" ", args));
        if (ArrayUtils.contains(args, "--host")) {
            printDebug("To change host you must exit the shell and launch it again with the --host parameter.");
            return null;
        }

        if (args.length == 1 && "exit".equals(args[0].trim())) {
            println("\nThanks for using OpenCGA. See you soon.\n\n", Color.YELLOW);
            System.exit(0);
        }

        if (args.length == 3 && "use".equals(args[0]) && "study".equals(args[1])) {
            setValidatedCurrentStudy(args[2], OpencgaMain.getShell());
            return null;
        }

        //Is for scripting login method
        if (isNotHelpCommand(args)) {
            if (ArrayUtils.contains(args, "--user-password")) {
                char[] passwordArray =
                        System.console().readPassword(format("\nEnter your password: ", Color.GREEN));
                args = ArrayUtils.addAll(args, "--password", new String(passwordArray));
                return args;
            }
        }
        CommandLineUtils.printLog("PARSED::: " + String.join(" ", args));

        return args;
    }


    public void setValidatedCurrentStudy(String arg, OpencgaCommandExecutor commandExecutor) {
        if (!StringUtils.isEmpty(commandExecutor.getSessionManager().getToken())) {
            CommandLineUtils.debug("Check study " + arg);
            OpenCGAClient openCGAClient = commandExecutor.getOpenCGAClient();
            if (openCGAClient != null) {
                try {
                    RestResponse<Study> res = openCGAClient.getStudyClient().info(arg, new ObjectMap());
                    if (res.allResultsSize() > 0) {
                        CommandLineUtils.printLog("Validated study " + arg);
                        commandExecutor.getSessionManager().getSession().setCurrentStudy(res.response(0).getResults().get(0).getFqn());
                        CommandLineUtils.printLog("Validated study " + arg);
                        commandExecutor.getSessionManager().saveSession();
                        println(getKeyValueAsFormattedString("Current study is: ",
                                commandExecutor.getSessionManager().getSession().getCurrentStudy()));
                    } else {
                        printWarn("Invalid study");
                    }
                } catch (ClientException e) {
                    CommandLineUtils.error(e);
                } catch (IOException e) {
                    CommandLineUtils.error(e);
                }
            } else {
                printError("Client not available");
            }
        } else {
            printWarn("To set a study you must be logged in");
        }
    }

    protected boolean isNotHelpCommand(String[] args) {
        return !ArrayUtils.contains(args, "--help") && !ArrayUtils.contains(args, "-h");
    }
}
