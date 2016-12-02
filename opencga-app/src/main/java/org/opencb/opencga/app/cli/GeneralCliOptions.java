/*
 * Copyright 2015-2016 OpenCB
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
import com.beust.jcommander.Parameter;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;

import java.util.HashMap;
import java.util.Map;

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

        @Parameter(names = {"-C", "--conf"}, description = "Configuration folder that contains opencga.yml, catalog-configuration.yaml, "
                + "storage-configuration.yml and client-configuration.yaml files.")
        public String conf;

        @Deprecated
        @Parameter(names = {"-v", "--verbose"}, description = "Increase the verbosity of logs")
        public boolean verbose = false;

        @Parameter(names = {"--of", "--output-format"}, description = "Output format. one of {JSON, JSON_PRETTY, TEXT, YAML}", arity = 1)
        public String outputFormat = "TEXT";

        @Parameter(names = {"-S", "--sid", "--session-id"}, description = "Token session id", arity = 1)
        public String sessionId;

        @Parameter(names = {"-c", "--cache"}, description = "use cache ", arity = 1)
        public boolean cache = false;

        @Parameter(names = {"-M", "--metadata"}, description = "Include metadata information", required = false, arity = 0)
        public boolean metadata = false;

        @Parameter(names = {"--no-header"}, description = "Not include headers in the output (not applicable to json output-format)",
                required = false, arity = 0)
        public boolean noHeader = false;

        @DynamicParameter(names = "-D", description = "Storage engine specific parameters go here comma separated, ie. -Dmongodb" +
                ".compression=snappy", hidden = false)
        public Map<String, String> params = new HashMap<>(); //Dynamic parameters must be initialized
    }
}
