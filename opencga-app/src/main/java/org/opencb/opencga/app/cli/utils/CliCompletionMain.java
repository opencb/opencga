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

package org.opencb.opencga.app.cli.utils;

import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;

import java.io.IOException;
import java.util.Collections;

public class CliCompletionMain {

    public static void main(String[] args) {
        try {
            String output = System.getProperty("app.home") + "/bin/utils";
            if (args != null && args.length > 0) {
                output = args[0];
            }

            OpencgaCliOptionsParser opencgaCliOptionsParser = new OpencgaCliOptionsParser();
            CommandLineUtils.generateBashAutoComplete(opencgaCliOptionsParser.getJCommander(), output + "/opencga",
                    "opencga", Collections.singletonList("-D"));

            AdminCliOptionsParser adminCliOptionsParser = new AdminCliOptionsParser();
            CommandLineUtils.generateBashAutoComplete(adminCliOptionsParser.getJCommander(), output + "/opencga-admin",
                    "opencga-admin", Collections.singletonList("-D"));

            InternalCliOptionsParser internalCliOptionsParser = new InternalCliOptionsParser();
            CommandLineUtils.generateBashAutoComplete(internalCliOptionsParser.getJCommander(), output + "/opencga-internal",
                    "opencga-internal", Collections.singletonList("-D"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
