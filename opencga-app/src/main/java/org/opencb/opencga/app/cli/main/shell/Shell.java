package org.opencb.opencga.app.cli.main.shell;

import org.jline.reader.*;
import org.jline.reader.impl.DefaultHighlighter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.OpenCgaCompleterImpl;
import org.opencb.opencga.app.cli.main.OpencgaMain;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.parser.ShellParamParser;
import org.opencb.opencga.app.cli.main.processors.ShellCommandProcessor;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.session.LogLevel;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import java.io.IOException;

import static org.opencb.commons.utils.PrintUtils.*;

public class Shell extends OpencgaCommandExecutor {


    private LineReader lineReader = null;
    private Terminal terminal = null;

    public Shell(GeneralCliOptions.CommonCommandOptions options) throws CatalogAuthenticationException {
        super(options);
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
                }
            }));
            reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .highlighter(new DefaultHighlighter())
                    .history(defaultHistory).completer(new OpenCgaCompleterImpl())
                    .build();
        } catch (Exception e) {
            CommandLineUtils.error("Failed to create terminal ", e);

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
            ShellCommandProcessor processor = new ShellCommandProcessor(new ShellParamParser());
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
                    processor.process(line.split(" "));

                }
            }
            terminal.writer().flush();
        } catch (Exception e) {
            CommandLineUtils.error("OpenCGA execution error ", e);
            CommandLineUtils.debug("sessionManager:" + sessionManager);
            CommandLineUtils.debug("getCliSession:" + sessionManager.getSession());

        }
    }

    public String getPrompt() {
        String host = format("[" + sessionManager.getHost() + "]", PrintUtils.Color.GREEN);
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
        if (!OpencgaMain.getLogLevel().equals(LogLevel.OFF)) {
            CommandLineUtils.printLog("Opencga is running in " + OpencgaMain.getLogLevel() + " mode");
        }
        println("");
        println("");
        println("");
        println("");
        println("");
        println("");
        println("");
        println("");
    }

}
