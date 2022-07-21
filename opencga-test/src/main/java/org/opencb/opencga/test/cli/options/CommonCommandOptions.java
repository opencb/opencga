package org.opencb.opencga.test.cli.options;

import com.beust.jcommander.Parameter;

public class CommonCommandOptions {

    public static String logLevel_DEFAULT_VALUE = "off";

    @Parameter(names = "--log-level", description = "Show current version information.")
    public static String logLevel = logLevel_DEFAULT_VALUE;


}
