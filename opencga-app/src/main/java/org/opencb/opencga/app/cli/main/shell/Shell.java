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
import org.opencb.opencga.app.cli.session.Session;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opencb.commons.utils.PrintUtils.*;

public class Shell extends OpencgaCommandExecutor {


    // Create a command processor to process all the shell commands
    private final CommandProcessor processor = new CommandProcessor();
    private LineReader lineReader = null;
    private Terminal terminal = null;
    private String host = null;

    public Shell(GeneralCliOptions.CommonCommandOptions options) throws CatalogAuthenticationException {
        super(options);
        if (options.host != null) {
            host = options.host;
        }
    }

    public static void printShellHeaderMessage() {
        System.out.print(eraseScreen());
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
        println(CommandLineUtils.getVersionString());
        println("");
        println("\nTo close the application type \"exit\"", Color.BLUE);
        println("");
        println("");
        println("");
        println("");
        println("Opencga is running in " + OpencgaMain.getLogLevel() + " mode");
        println("");
        println("");
        println("");
        println("");
        println("");
        println("");
        println("");
        println("");
    }

    private LineReader getTerminal() {
        LineReader reader = null;
        logger = LoggerFactory.getLogger(Shell.class);
        try {
            if (terminal == null) {
                terminal = TerminalBuilder.builder()
                        .system(true).nativeSignals(true)
                        .build();
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
                    String[] args = splitLine(line);
                    logger.debug("Command: " + line);

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
            logger.error("OpenCGA execution error ", e);
            logger.debug("sessionManager:" + sessionManager);
            logger.debug("getCliSession:" + sessionManager.getSession());

        }
    }

    private String[] splitLine(String line) {
        List<String> list = new ArrayList<String>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(line);
        while (m.find()) {
            list.add(m.group(1).replace("\"", ""));
        }
        return list.toArray(new String[list.size()]);
    }

    public String getPrompt() {
        String host = format("[" + sessionManager.getSession().getHost() + "]", PrintUtils.Color.GREEN);
        String study = format("[" + sessionManager.getSession().getCurrentStudy() + "]", PrintUtils.Color.BLUE);
        String user = format("<" + sessionManager.getSession().getUser() + "/>", PrintUtils.Color.YELLOW);
        return host + study + user + " ";
    }

    public String[] parseParams(String[] args) throws CatalogAuthenticationException {
        logger.debug("Executing " + String.join(" ", args));
        if (ArrayUtils.contains(args, "--host")) {
            printDebug("To change host you must exit the shell and launch it again with the --host parameter.");
            return null;
        }

        if (args.length == 1 && "exit".equals(args[0].trim())) {
            println("\nThanks for using OpenCGA. See you soon.\n\n", Color.YELLOW);
            System.exit(0);
        }

        if (args.length == 3 && "use".equals(args[0]) && "study".equals(args[1])) {
            logger.debug("Validated study " + StringUtils.join(args, " "));

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
        Session session = getSessionManager().getSession();
        if (!StringUtils.isEmpty(session.getToken())) {
            logger.debug("Check study " + arg);
            OpenCGAClient openCGAClient = new OpenCGAClient(new AuthenticationResponse(session.getToken())
                    , clientConfiguration);
            if (openCGAClient != null) {
                try {
                    RestResponse<Study> res = openCGAClient.getStudyClient().info(arg, new ObjectMap());
                    if (res.allResultsSize() > 0) {
                        session.setCurrentStudy(res.response(0).getResults().get(0).getFqn());
                        logger.debug("Info study results: " + res.response(0).getResults().get(0).getFqn());
                        logger.debug("Validated study " + arg);
                        getSessionManager().saveSession(session);

                        logger.debug("Current study is: " +
                                session.getCurrentStudy());
                        println(getKeyValueAsFormattedString("Current study is: ",
                                session.getCurrentStudy()));
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
