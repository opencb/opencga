package org.opencb.opencga.app.cli.main;

import org.jline.reader.*;
import org.jline.reader.impl.DefaultHighlighter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.io.IOException;

import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.ansi;

public class OpencgaCliShellExecutor { // extends CommandExecutor {

    private LineReader lineReader = null;
    private Terminal terminal = null;

    public OpencgaCliShellExecutor(GeneralCliOptions.CommonCommandOptions options) throws CatalogAuthenticationException {
//        super(options);
    }

    private LineReader getTerminal() {
        LineReader reader = null;
        try {
            if (terminal == null) {
                terminal = TerminalBuilder.builder()
                        .system(true).nativeSignals(true)
                        .build();

                System.out.print(ansi().eraseScreen());
                printShellHeaderMessage();
            }
            History defaultHistory = new DefaultHistory();

            // Register a shutdown-hook per JLine documentation to save history
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    defaultHistory.save();
                } catch (IOException e) {
                    CommandLineUtils.printColor("RED", "Failed to save terminal history" + e.getMessage());
                }
            }));
            reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .highlighter(new DefaultHighlighter())
                    .history(defaultHistory).completer(new OpenCgaCompleterImpl())
                    .build();
        } catch (Exception e) {
            CommandLineUtils.printColor("RED", "Failed to create terminal " + e.getMessage());
        }

        return reader;
    }

    //    @Override
    public void execute() throws Exception {
        try {
            if (lineReader == null) {
                lineReader = getTerminal();
            }
            String PROMPT = String.valueOf(ansi().fg(GREEN).a("\n[OpenCGA]/>").reset());

            while (true) {
                // Read and sanitize the input
                String line;
                PROMPT = CliSessionManager.getPrompt();
                try {
                    line = lineReader.readLine(PROMPT);
                } catch (UserInterruptException e) {
                    CommandLineUtils.printColor("YELLOW", "If you want to close OpenCGA. Type \"exit\"");
                    continue;
                } catch (EndOfFileException e) {
                    break;
                }
                if (line == null) {
                    continue;
                }
                line = line.trim();
                if (!line.equals("")) {
                    OpencgaCliProcessor.execute(line.split(" "));
                }
                // Construct the Command and args to pass to that command

            }
            terminal.writer().flush();
        } catch (Exception e) {
            if (CliSessionManager.isDebug()) {
                e.printStackTrace();
            }
            OpencgaMain.printErrorMessage("OpenCGA execution error ", e);
        }
    }

    private void printShellHeaderMessage() {

       /* printlnBlue("    ██████╗ ██████╗ ███████╗███╗   ██╗ ██████╗ ██████╗  █████╗ ");
        printlnBlue("   ██╔═══██╗██╔══██╗██╔════╝████╗  ██║██╔════╝██╔════╝ ██╔══██╗");
        printlnBlue("   ██║   ██║██████╔╝█████╗  ██╔██╗ ██║██║     ██║  ███╗███████║");
        printlnBlue("   ██║   ██║██╔═══╝ ██╔══╝  ██║╚██╗██║██║     ██║   ██║██╔══██║");
        printlnBlue("   ╚██████╔╝██║     ███████╗██║ ╚████║╚██████╗╚██████╔╝██║  ██║");
        printlnBlue("    ╚═════╝ ╚═╝     ╚══════╝╚═╝  ╚═══╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═╝");
        */
        CommandLineUtils.printColor("GREEN", "     ███████                                    █████████    █████████    █████████  ");
        CommandLineUtils.printColor("GREEN", "   ███░░░░░███                                 ███░░░░░███  ███░░░░░███  ███░░░░░███ ");
        CommandLineUtils.printColor("GREEN", "  ███     ░░███ ████████   ██████  ████████   ███     ░░░  ███     ░░░  ░███    ░███ ");
        CommandLineUtils.printColor("GREEN", "  ███      ░███░░███░░███ ███░░███░░███░░███ ░███         ░███          ░███████████ ");
        CommandLineUtils.printColor("GREEN", "  ███      ░███ ░███ ░███░███████  ░███ ░███ ░███         ░███    █████ ░███░░░░░███ ");
        CommandLineUtils.printColor("GREEN", "  ░███     ███  ░███ ░███░███░░░   ░███ ░███ ░░███     ███░░███  ░░███  ░███    ░███ ");
        CommandLineUtils.printColor("GREEN", "  ░░░███████░   ░███████ ░░██████  ████ █████ ░░█████████  ░░█████████  █████   █████");
        CommandLineUtils.printColor("GREEN", "    ░░░░░░░     ░███░░░   ░░░░░░  ░░░░ ░░░░░   ░░░░░░░░░    ░░░░░░░░░  ░░░░░   ░░░░░ ");
        CommandLineUtils.printColor("GREEN", "                ░███                                                                 ");
        CommandLineUtils.printColor("GREEN", "                █████                                                                ");
        CommandLineUtils.printColor("GREEN", "               ░░░░░                                                                 ");

        System.out.println();
        CommandLineUtils.printColor("GREEN", "\tOpenCGA CLI version: ", false);
        CommandLineUtils.printColor("YELLOW", "\t" + GitRepositoryState.get().getBuildVersion());
        CommandLineUtils.printColor("GREEN", "\tGit version:", false);
        CommandLineUtils.printColor("YELLOW", "\t\t" + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId());
        CommandLineUtils.printColor("GREEN", "\tProgram:", false);
        CommandLineUtils.printColor("YELLOW", "\t\tOpenCGA (OpenCB)");
        CommandLineUtils.printColor("GREEN", "\tDescription: ", false);
        CommandLineUtils.printColor("YELLOW", "\t\tBig Data platform for processing and analysing NGS data");
        System.out.println();
        System.out.println("\nTo close the application type \"exit\"");
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        OpencgaMain.printDebugMessage("Opencga is running in DEBUG mode");
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
