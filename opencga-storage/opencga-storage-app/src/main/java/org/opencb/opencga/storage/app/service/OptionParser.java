package org.opencb.opencga.storage.app.service;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jacobo on 23/10/14.
 */
public class OptionParser {

    private final JCommander jcommander;
    private GeneralOptions generalOptions = new GeneralOptions();

    OptionParser() {
        this.jcommander = new JCommander();

        jcommander.addObject(generalOptions);
    }

    String usage() {
        StringBuilder out = new StringBuilder();
        jcommander.usage(out);
        return out.toString();
    }

    String parse(String args[]) {
        jcommander.parse(args);
        return jcommander.getParsedCommand() != null? jcommander.getParsedCommand() : "";
    }

    GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    //@Parameters(commandNames = {"daemon"}, commandDescription = "")
    class GeneralOptions {

        @Parameter(names = { "-h", "--help" }, description = "Print this help", help = true)
        boolean help;

        @Parameter(names = {"-V", "--version"}, arity = 0)
        boolean version = false;

        @Parameter(names = {"-C", "--conf"}, description = "Specify the configuration file", required = false, arity = 1)
        String conf = "";

        @Parameter(names = {"-v", "--verbose"}, description = "This parameter set the level of the logging", required = false, arity = 0)
        boolean verbose = false;

        @Parameter(names = {"-L", "--log-level"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        int logLevel;

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        Map<String, String> params = new HashMap<String, String>();


        //---------------------


        @Parameter(names = {"-P", "--port"}, description = "Port", required = false, arity = 1)
        int port = 0;


    }

}
