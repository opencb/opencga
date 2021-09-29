package org.opencb.opencga.app.cli.main;

import org.jline.reader.*;
import org.jline.reader.impl.DefaultHighlighter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.CliSession;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;

import static org.fusesource.jansi.Ansi.Color.*;
import static org.fusesource.jansi.Ansi.ansi;

class OpencgaCliShellExecutor extends CommandExecutor {



    private LineReader lineReader=null;
    private  Terminal terminal =null;

    public OpencgaCliShellExecutor(GeneralCliOptions.CommonCommandOptions options){
        super(new GeneralCliOptions.CommonCommandOptions(),true);
    }
    public void setCurrentStudy(String arg) {
        OpenCGAClient openCGAClient = new OpenCGAClient(new AuthenticationResponse(CliSession.getInstance().getToken()), clientConfiguration);
        if(openCGAClient!=null) {
            try {
                RestResponse<Study> res = openCGAClient.getStudyClient().info(arg, new ObjectMap());
                if (res.allResultsSize() > 0) {
                    CliSession.getInstance().setCurrentStudy(res.response(0).getResults().get(0).getFqn());
                    //String user, String token, String refreshToken, List<String> studies,String currentStudy
                    try {
                        CliSession.getInstance().saveCliSessionFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    printlnGreen("Current study is " + arg);
                } else {
                    printlnYellow("Invalid study");
                }
            } catch (ClientException e) {
                printlnRed(e.getMessage());
               // e.printStackTrace();
            }
        }else{
            printlnRed("Client not available");
        }
    }



    private LineReader getTerminal(){
        LineReader reader =null;
        try {
            if(terminal == null) {
                terminal = TerminalBuilder.builder()
                        .system(true)
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
                    printlnRed("Failed to save terminal history" + e.getMessage());
                }
            }));
            reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .highlighter(new DefaultHighlighter())
                    .history(defaultHistory)
                    .build();
        }catch (Exception e) {
            printlnRed("Failed to create terminal " + e.getMessage());
        }


        return reader;
    }


    @Override
    public void execute() throws Exception {
        try {

            if(lineReader == null){
                lineReader=getTerminal();
            }
            String PROMPT = String.valueOf(ansi().fg(GREEN).a("\n[OpenCGA]/>").reset());


            while (true) {
                // Read and sanitize the input
                String line;
                if(!CliSession.getInstance().isValid()){
                    OpencgaCliProcessor.forceLogin();
                }
                if(cliSession != null && cliSession.getUser() != null && CliSession.getInstance().isValid()){
                    PROMPT = getPrompt();
                }
                try {
                    line = lineReader.readLine(PROMPT);
                } catch (UserInterruptException e) {
                    printlnYellow("If you want to close OpenCGA. Type \"exit\"");
                    continue;
                } catch (EndOfFileException e) {
                    break;
                }
                if (line == null) {
                    continue;
                }
                line = line.trim();
                if (!line.equals("")) {
                    OpencgaCliProcessor.process(line.split(" "));
                }
                // Construct the Command and args to pass to that command

                // Flush a newline to the screen.
                terminal.writer().flush();
            }

        } catch (Exception e) {
            printlnRed(e.getMessage());
            e.printStackTrace();
           // execute();
        }
    }



    private String getPrompt() {

        return String.valueOf(ansi().fg(BLUE).a("["+cliSession.getCurrentStudy()+"]").fg(YELLOW).a("<"+cliSession.getUser()+">").fg(GREEN).a(
                "[OpenCGA]/>").reset());
    }

    private void printShellHeaderMessage() {

       /* printlnBlue("    ██████╗ ██████╗ ███████╗███╗   ██╗ ██████╗ ██████╗  █████╗ ");
        printlnBlue("   ██╔═══██╗██╔══██╗██╔════╝████╗  ██║██╔════╝██╔════╝ ██╔══██╗");
        printlnBlue("   ██║   ██║██████╔╝█████╗  ██╔██╗ ██║██║     ██║  ███╗███████║");
        printlnBlue("   ██║   ██║██╔═══╝ ██╔══╝  ██║╚██╗██║██║     ██║   ██║██╔══██║");
        printlnBlue("   ╚██████╔╝██║     ███████╗██║ ╚████║╚██████╗╚██████╔╝██║  ██║");
        printlnBlue("    ╚═════╝ ╚═╝     ╚══════╝╚═╝  ╚═══╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═╝");
        */
        printlnGreen("     ███████                                    █████████    █████████    █████████  ");
        printlnGreen("   ███░░░░░███                                 ███░░░░░███  ███░░░░░███  ███░░░░░███ ");
        printlnGreen("  ███     ░░███ ████████   ██████  ████████   ███     ░░░  ███     ░░░  ░███    ░███ ");
        printlnGreen("  ███      ░███░░███░░███ ███░░███░░███░░███ ░███         ░███          ░███████████ ");
        printlnGreen("  ███      ░███ ░███ ░███░███████  ░███ ░███ ░███         ░███    █████ ░███░░░░░███ ");
        printlnGreen("  ░███     ███  ░███ ░███░███░░░   ░███ ░███ ░░███     ███░░███  ░░███  ░███    ░███ ");
        printlnGreen("  ░░░███████░   ░███████ ░░██████  ████ █████ ░░█████████  ░░█████████  █████   █████");
        printlnGreen("    ░░░░░░░     ░███░░░   ░░░░░░  ░░░░ ░░░░░   ░░░░░░░░░    ░░░░░░░░░  ░░░░░   ░░░░░ ");
        printlnGreen("                ░███                                                                 ");
        printlnGreen("                █████                                                                ");
        printlnGreen("               ░░░░░                                                                 ");


        System.out.println("");
        printGreen("\tOpenCGA CLI version: ");
        printlnYellow("\t"+GitRepositoryState.get().getBuildVersion());
        printGreen("\tGit version:");
        printlnYellow("\t\t"+GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId());
        printGreen("\tProgram:");
        printlnYellow("\t\tOpenCGA (OpenCB)");
        printGreen("\tDescription: ");
        printlnYellow("\t\tBig Data platform for processing and analysing NGS data");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
    }

    public static void printlnBlue(String message) {
        System.out.println(ansi().fg(BLUE).a(message).reset());
    }

    public static void printBlue(String message) {
        System.out.print(ansi().fg(BLUE).a(message).reset());
    }

    public static void printlnGreen(String message) {
        System.out.println(ansi().fg(GREEN).a(message).reset());
    }

    public static void printGreen(String message) {
        System.out.print(ansi().fg(GREEN).a(message).reset());
    }

    public static void printlnYellow(String message) {
        System.out.println(ansi().fg(YELLOW).a(message).reset());
    }

    public static void printYellow(String message) {
        System.out.print(ansi().fg(YELLOW).a(message).reset());
    }

    public static void printlnRed(String message) {
        System.out.println(ansi().fg(RED).a(message).reset());
    }

    public static void printRed(String message) {
        System.out.print(ansi().fg(RED).a(message).reset());
    }
}
