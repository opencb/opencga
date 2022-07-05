package org.opencb.opencga.app.cli.main.impl;


import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.Completer;
import org.opencb.commons.app.cli.AbstractCommandExecutor;
import org.opencb.commons.app.cli.CliOptionsParser;
import org.opencb.commons.app.cli.GeneralCliOptions;
import org.opencb.commons.app.cli.main.processors.AbstractCommandProcessor;
import org.opencb.commons.app.cli.main.shell.Shell;
import org.opencb.commons.app.cli.session.AbstractSessionManager;
import org.opencb.opencga.app.cli.main.completer.OpencgaCompleterImpl;
import org.opencb.opencga.app.cli.main.parser.OpencgaCliOptionsParser;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import static org.opencb.commons.utils.PrintUtils.*;

public class ShellImpl extends Shell {


    // Create a command processor to process all the shell commands

    public ShellImpl(GeneralCliOptions.CommonCommandOptions options, AbstractCommandExecutor executor) throws CatalogAuthenticationException {
        super(options, executor);
        if (options.host != null) {
            executor.getSessionManager().getSession(options.host);
        }
    }

    public String getPrompt() {
        String host = format("[" + executor.getSessionManager().getSession().getHost() + "]", Color.GREEN);
        String study = format("[" + executor.getSessionManager().getSession().getCurrentStudy() + "]", Color.BLUE);
        String user = format("<" + executor.getSessionManager().getSession().getUser() + "/>", Color.YELLOW);
        return host + study + user;
    }

    public String[] parseCustomParams(String[] args) {
        if (ArrayUtils.contains(args, "--host")) {
            printDebug("To change host you must exit the shell and launch it again with the --host parameter.");
            return null;
        }

        if (args.length == 3 && "use".equals(args[0]) && "study".equals(args[1])) {
            logger.debug("Validated study " + StringUtils.join(args, " "));
            ((SessionManagerImpl) executor.getSessionManager()).setValidatedCurrentStudy(args[2]);
            return null;
        }
        return args;
    }


    @Override
    public Completer getCompleter() {
        return new OpencgaCompleterImpl();
    }

    @Override
    public CliOptionsParser getCliOptionsParser() {
        return new OpencgaCliOptionsParser();
    }

    @Override
    public AbstractCommandProcessor getProcessor() {
        return new CommandProcessorImpl();
    }


    public AbstractSessionManager getSessionManager() {
        return executor.getSessionManager();
    }
}
