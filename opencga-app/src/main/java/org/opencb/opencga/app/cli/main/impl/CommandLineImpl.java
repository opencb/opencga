package org.opencb.opencga.app.cli.main.impl;

import org.apache.commons.lang3.ArrayUtils;
import org.opencb.commons.app.cli.CliOptionsParser;
import org.opencb.commons.app.cli.GeneralCliOptions;
import org.opencb.commons.app.cli.main.CommandLine;
import org.opencb.commons.app.cli.main.processors.AbstractCommandProcessor;
import org.opencb.commons.app.cli.main.shell.Shell;
import org.opencb.opencga.app.cli.main.parser.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.shell.ShellExecutor;

public class CommandLineImpl extends CommandLine {


    public CommandLineImpl(String appName) {
        super(appName);
    }

    public AbstractCommandProcessor getProcessor() {
        return new CommandProcessorImpl();
    }

    public CliOptionsParser getOptionParser() {
        return new OpencgaCliOptionsParser();
    }

    public Shell getShell(String[] args) throws Exception {
        GeneralCliOptions.CommonCommandOptions options = new GeneralCliOptions.CommonCommandOptions();
        if (ArrayUtils.contains(args, "--host")) {
            options.host = args[ArrayUtils.indexOf(args, "--host") + 1];
        }
        return new ShellImpl(options, new ShellExecutor(options, true, logLevel));
    }
}
