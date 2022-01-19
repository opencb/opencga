package org.opencb.opencga.app.cli.main;

import org.jline.reader.*;
import org.jline.reader.impl.DefaultHighlighter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.processors.ShellProcessor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import java.io.IOException;

import static org.opencb.commons.utils.PrintUtils.*;


public class OpencgaCliShellExecutor extends OpencgaCommandExecutor {

    private LineReader lineReader = null;
    private Terminal terminal = null;

    public OpencgaCliShellExecutor(GeneralCliOptions.CommonCommandOptions options) throws CatalogAuthenticationException {
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
                    CommandLineUtils.printLog("Failed to save terminal history", e);
                }
            }));
            reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .highlighter(new DefaultHighlighter())
                    .history(defaultHistory).completer(new OpenCgaCompleterImpl())
                    .build();
        } catch (Exception e) {
            CommandLineUtils.printLog("Failed to create terminal ", e);

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
            ShellProcessor processor = new ShellProcessor();
            while (true) {
                // Read and sanitize the input
                String line;
                PROMPT = getPrompt();
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
                if (!line.equals("")) {

                    processor.execute(line.split(" "));

                }
                // Construct the Command and args to pass to that command
            }
            terminal.writer().flush();
        } catch (Exception e) {
            CommandLineUtils.printLog("OpenCGA execution error ", e);
            e.printStackTrace();
            CommandLineUtils.printLog("sessionManager:" + sessionManager, null);
            CommandLineUtils.printLog("getCliSession:" + sessionManager.getCliSession(), null);

        }
    }

    public String getPrompt() {
        String host = format("[" + sessionManager.getHost() + "]", PrintUtils.Color.GREEN);
        String study = format("[" + sessionManager.getCliSession().getCurrentStudy() + "]", PrintUtils.Color.BLUE);
        String user = format("<" + sessionManager.getCliSession().getUser() + "/>", PrintUtils.Color.YELLOW);
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

        System.out.println();
        System.out.println(CommandLineUtils.getVersionString());
        System.out.println();
        println("\nTo close the application type \"exit\"", Color.BLUE);
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        CommandLineUtils.printLog("Opencga is running in DEBUG mode");
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
    }

}
