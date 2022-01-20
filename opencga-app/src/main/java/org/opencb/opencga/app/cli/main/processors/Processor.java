package org.opencb.opencga.app.cli.main.processors;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.parser.ParamParser;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.main.utils.LoginUtils;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.core.common.GitRepositoryState;

import static org.opencb.commons.utils.PrintUtils.println;

public abstract class Processor {

    private final ParamParser parser;

    public Processor(ParamParser parser) {
        this.parser = parser;
    }

    abstract protected void processCommandOptions(OpencgaCliOptionsParser cliOptionsParser);

    public void process(String[] args) {
        OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();
        try {
            //Process the shortcuts login, help, version, logout...
            args = processShortCuts(args, cliOptionsParser);
            if (!ArrayUtils.isEmpty(args)) {
                //Parse params differently if it is the shell or the cli
                args = parser.parseParams(args);
                if (args != null) {
                    cliOptionsParser.parse(args);
                    CommandLineUtils.debug("PARSED OPTIONS ::: " + ArrayUtils.toString(args));
                    //execute command with parsed options
                    processCommandOptions(cliOptionsParser);
                }
            }
        } catch (Exception e) {
            CommandLineUtils.error(e);
            cliOptionsParser.printUsage();
        }

    }


    public String[] processShortCuts(String[] args, OpencgaCliOptionsParser cliOptionsParser) throws CatalogAuthenticationException {
        switch (args[0]) {
            case "login":
                return LoginUtils.parseLoginCommand(args);
            case "--help":
            case "help":
            case "-h":
            case "?":
                cliOptionsParser.printUsage();
                return new String[0];
            case "--version":
            case "version":
                println(CommandLineUtils.getVersionString());
                return new String[0];
            case "--build-version":
            case "build-version":
                println(GitRepositoryState.get().getBuildVersion());
                return new String[0];
            case "logout":
                return ArrayUtils.addAll(new String[]{"users"}, args);
            default:
                return args;
        }
    }
}
