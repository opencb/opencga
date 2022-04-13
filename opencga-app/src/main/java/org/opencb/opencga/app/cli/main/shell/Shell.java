package org.opencb.opencga.app.cli.main.shell;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.*;
import org.jline.reader.impl.DefaultHighlighter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.OpenCgaCompleterImpl;
import org.opencb.opencga.app.cli.main.OpencgaMain;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.processors.CommandProcessor;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;
import java.util.logging.Level;

import static org.opencb.commons.utils.PrintUtils.*;

public class Shell extends OpencgaCommandExecutor {


    private LineReader lineReader = null;
    private Terminal terminal = null;
    private String host = null;

    public Shell(GeneralCliOptions.CommonCommandOptions options) throws CatalogAuthenticationException {
        super(options);
        if (options.host != null) {
            host = options.host;
        }
    }

    private LineReader getTerminal() {
        LineReader reader = null;
        try {
            if (terminal == null) {
                terminal = TerminalBuilder.builder()
                        .system(true).nativeSignals(true)
                        .build();

                System.out.print(eraseScreen());
                printShellHeaderMessage();
            }
            History defaultHistory = new DefaultHistory();

            // Register a shutdown-hook per JLine documentation to save history
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    defaultHistory.save();
                } catch (IOException e) {
                    CommandLineUtils.error("Failed to save terminal history", e);
                    logger.error("Failed to save terminal history", e);
                }
            }));
            reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .highlighter(new DefaultHighlighter())
                    .history(defaultHistory).completer(new OpenCgaCompleterImpl())
                    .build();
        } catch (Exception e) {
            CommandLineUtils.error("Failed to create terminal ", e);
            logger.error("Failed to create terminal ", e);

        }

        return reader;
    }

    //    @Override
    public void execute() throws Exception {
        try {
            if (lineReader == null) {
                lineReader = getTerminal();
            }
            String PROMPT;
            // Create a command processor to process all the shell commands
            CommandProcessor processor = new CommandProcessor();
            while (true) {
                // Read and sanitize the input
                String line;

                // Renew the prompt for set the current study, host and user
                PROMPT = getPrompt();

                // Read the shell command line for next execution
                try {
                    line = lineReader.readLine(PROMPT);
                } catch (UserInterruptException e) {
                    printWarn("If you want to close OpenCGA. Type \"exit\"");
                    continue;
                } catch (EndOfFileException e) {
                    break;
                }
                if (line == null) {
                    continue;
                }
                line = line.trim();

                // Send the line read to the processor for process
                if (!line.equals("")) {
                    String[] args = line.split(" ");
                    args = parseParams(args);
                    if (!ArrayUtils.isEmpty(args)) {
                        ArrayUtils.addAll(args, "--host", this.host);
                        processor.process(args);
                    }
                }
            }
            terminal.writer().flush();
        } catch (Exception e) {
            CommandLineUtils.error("OpenCGA execution error ", e);
            CommandLineUtils.debug("sessionManager:" + sessionManager);
            CommandLineUtils.debug("getCliSession:" + sessionManager.getSession());
            logger.error("OpenCGA execution error ", e);
            logger.debug("sessionManager:" + sessionManager);
            logger.debug("getCliSession:" + sessionManager.getSession());

        }
    }

    public String getPrompt() {
        String host = format("[" + sessionManager.getSession().getHost() + "]", PrintUtils.Color.GREEN);
        String study = format("[" + sessionManager.getSession().getCurrentStudy() + "]", PrintUtils.Color.BLUE);
        String user = format("<" + sessionManager.getSession().getUser() + "/>", PrintUtils.Color.YELLOW);
        return host + study + user;
    }


    private void printShellHeaderMessage() {

        println("     ███████                                    █████████    █████████    █████████  ", Color.GREEN);
        println("   ███░░░░░███                                 ███░░░░░███  ███░░░░░███  ███░░░░░███ ", Color.GREEN);
        println("  ███     ░░███ ████████   ██████  ████████   ███     ░░░  ███     ░░░  ░███    ░███ ", Color.GREEN);
        println("  ███      ░███░░███░░███ ███░░███░░███░░███ ░███         ░███          ░███████████ ", Color.GREEN);
        println("  ███      ░███ ░███ ░███░███████  ░███ ░███ ░███         ░███    █████ ░███░░░░░███ ", Color.GREEN);
        println("  ░███     ███  ░███ ░███░███░░░   ░███ ░███ ░░███     ███░░███  ░░███  ░███    ░███ ", Color.GREEN);
        println("  ░░░███████░   ░███████ ░░██████  ████ █████ ░░█████████  ░░█████████  █████   █████", Color.GREEN);
        println("    ░░░░░░░     ░███░░░   ░░░░░░  ░░░░ ░░░░░   ░░░░░░░░░    ░░░░░░░░░  ░░░░░   ░░░░░ ", Color.GREEN);
        println("                ░███                                                                 ", Color.GREEN);
        println("                █████                                                                ", Color.GREEN);
        println("               ░░░░░                                                                 ", Color.GREEN);

        println("");
        System.out.println(CommandLineUtils.getVersionString());
        println("");
        println("\nTo close the application type \"exit\"", Color.BLUE);
        println("");
        println("");
        println("");
        println("");
        CommandLineUtils.debug("Opencga is running in " + OpencgaMain.getLogLevel() + " mode");
        println("");
        println("");
        println("");
        println("");
        println("");
        println("");
        println("");
        println("");
    }


    public String[] parseParams(String[] args) throws CatalogAuthenticationException {
        CommandLineUtils.debug("Executing " + String.join(" ", args));
        if (ArrayUtils.contains(args, "--host")) {
            printDebug("To change host you must exit the shell and launch it again with the --host parameter.");
            return null;
        }

        if (args.length == 1 && "exit".equals(args[0].trim())) {
            println("\nThanks for using OpenCGA. See you soon.\n\n", Color.YELLOW);
            System.exit(0);
        }

        if (args.length == 3 && "use".equals(args[0]) && "study".equals(args[1])) {
            setValidatedCurrentStudy(args[2]);
            return null;
        }

        //Is for scripting login method
        if (isNotHelpCommand(args)) {
            if (ArrayUtils.contains(args, "--user-password")) {
                char[] passwordArray =
                        System.console().readPassword(format("\nEnter your password: ", Color.GREEN));
                ArrayUtils.addAll(args, "--password", new String(passwordArray));
                return args;
            }
        }
        return CommandLineUtils.processShortCuts(args);

    }


    public void setValidatedCurrentStudy(String arg) {
        if (!StringUtils.isEmpty(getSessionManager().getSession().getToken())) {
            CommandLineUtils.debug("Check study " + arg);
            OpenCGAClient openCGAClient = getOpenCGAClient();
            if (openCGAClient != null) {
                try {
                    RestResponse<Study> res = openCGAClient.getStudyClient().info(arg, new ObjectMap());
                    if (res.allResultsSize() > 0) {
                        CommandLineUtils.debug("Validated study " + arg);
                        getSessionManager().getSession().setCurrentStudy(res.response(0).getResults().get(0).getFqn());
                        CommandLineUtils.debug("Validated study " + arg);
                        getSessionManager().saveSession();
                        println(getKeyValueAsFormattedString("Current study is: ",
                                getSessionManager().getSession().getCurrentStudy()));
                    } else {
                        printWarn("Invalid study");
                    }
                } catch (ClientException e) {
                    CommandLineUtils.error(e);
                    logger.error(e.getMessage(), e);

                } catch (IOException e) {
                    CommandLineUtils.error(e);
                    logger.error(e.getMessage(), e);
                }
            } else {
                printError("Client not available");
                logger.error("Client not available");
            }
        } else {
            printWarn("To set a study you must be logged in");
        }
    }

    protected boolean isNotHelpCommand(String[] args) {
        return !ArrayUtils.contains(args, "--help") && !ArrayUtils.contains(args, "-h");
    }
}
