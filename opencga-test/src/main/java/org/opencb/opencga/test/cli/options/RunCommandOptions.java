package org.opencb.opencga.test.cli.options;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandNames = {"run"}, commandDescription = "Execute commands")
public class RunCommandOptions {

    @Parameter(names = "--conf", description = "Config file (configuration.yml).", required = true)
    public static String configFile = "configuration.yml";

    @Parameter(names = "--simulate", description = "Show current version information.")
    public static boolean simulate = false;

    @Parameter(names = "--output", description = "Output directory.")
    public static String output = "./";

}
