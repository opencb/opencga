/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.app.cli;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 03/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GeneralCliOptions {

    private final JCommander jCommander;

    public GeneralCliOptions(JCommander jCommander) {
        this.jCommander = jCommander;
    }

    public String getSubCommand() {
        return CliOptionsParser.getSubCommand(jCommander);
    }

    public static class GeneralOptions {

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"--version"})
        public boolean version;
    }

    /**
     * This class contains all those common parameters available for all 'subcommands'
     */
    public static class CommonCommandOptions extends BasicCommonCommandOptions {
        @Parameter(names = {"--of", "--output-format"}, description = "Output format. one of {JSON, JSON_PRETTY, TEXT, YAML}", arity = 1)
        public String outputFormat = "TEXT";
    }

    public static class BasicCommonCommandOptions {
        @Parameter(names = {"-h", "--help"}, description = "Print this help", help = true)
        public boolean help;

        @Parameter(names = {"-L", "--log-level"}, description = "One of the following: 'error', 'warn', 'info', 'debug', 'trace' [info]")
        public String logLevel;

        @Parameter(names = {"--log-file"}, description = "Set the file to write the log")
        public String logFile;

        @Parameter(names = {"-C", "--conf"}, description = "Configuration folder that contains configuration.yml, "
                + "storage-configuration.yml and client-configuration.yml files.")
        public String conf;

        @Deprecated
        @Parameter(names = {"-v", "--verbose"}, description = "Increase the verbosity of logs")
        public boolean verbose = false;

        @Parameter(names = {"-S", "--sid", "--session-id"}, description = "Token session id. Deprecated, use --token instead", arity = 1)
        public void setToken(String token) {
            this.token = token;
        }

        @Parameter(names = {"--token"}, description = "Token session id", arity = 1)
        public String token;

        @Parameter(names = {"-M", "--metadata"}, description = "Include metadata information", required = false, arity = 0)
        public boolean metadata = false;

        @Parameter(names = {"--no-header"}, description = "Not include headers in the output (not applicable to json output-format)",
                required = false, arity = 0)
        public boolean noHeader = false;

        @DynamicParameter(names = "-D", description = "Storage engine specific parameters go here comma separated, ie. -Dmongodb" +
                ".compression=snappy", hidden = true)
        public Map<String, String> params = new HashMap<>(); //Dynamic parameters must be initialized
    }

    public static class StudyOption {

        @Parameter(names = {"-s", "--study"}, description = "Study [[user@]project:]study.", arity = 1)
        public String study;

    }

    public static class StudyListOption {

        @Parameter(names = {"-s", "--study"}, description = "Study list [[user@]project:]study where study and project can be either the id"
                + " or alias.", arity = 1)
        public String study;

    }

    public static class DataModelOptions {

        @Parameter(names = {"-I", "--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"-E", "--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

    }

    public static class NumericOptions {

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public int skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public int limit;

        @Parameter(names = {"--count"}, description = "Total number of results. Default = false", arity = 0)
        public boolean count;

    }

}
