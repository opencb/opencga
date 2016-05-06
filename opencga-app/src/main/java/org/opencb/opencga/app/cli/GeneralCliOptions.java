package org.opencb.opencga.app.cli;

import com.beust.jcommander.Parameter;

/**
 * Created on 03/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GeneralCliOptions {

    public static class GeneralOptions {

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"--version"})
        public boolean version;
    }

    /**
     * This class contains all those common parameters available for all 'subcommands'
     */
    public static class CommonCommandOptions {

        @Parameter(names = {"-h", "--help"}, description = "Print this help", help = true)
        public boolean help;

        @Parameter(names = {"-L", "--log-level"}, description = "One of the following: 'error', 'warn', 'info', 'debug', 'trace'")
        public String logLevel = "info";

        @Parameter(names = {"--log-file"}, description = "Set the file to write the log")
        public String logFile;

        @Parameter(names = {"-v", "--verbose"}, description = "Increase the verbosity of logs")
        public boolean verbose = false;

        @Parameter(names = {"-C", "--conf"}, description = "Configuration folder path, this folder must contain opencga.yml, catalog-configuration.yaml and storage-configuration.yml files.")
        public String configFile;

    }
}
