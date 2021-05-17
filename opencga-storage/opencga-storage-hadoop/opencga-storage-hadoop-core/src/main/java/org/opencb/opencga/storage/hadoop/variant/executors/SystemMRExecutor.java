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

package org.opencb.opencga.storage.hadoop.variant.executors;

import org.apache.tools.ant.types.Commandline;
import org.opencb.commons.exec.Command;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.MR_EXECUTOR_SSH_PASSWORD;

/**
 * Created on 18/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SystemMRExecutor extends MRExecutor {

    @Override
    public int run(String executable, String[] args) {
        return run(buildCommandLine(executable, args));
    }

    public int run(String commandLine) {
        Command command = new Command(commandLine, getEnv());
        command.run();
        return command.getExitValue();
    }

    private String buildCommandLine(String executable, String[] args) {
        redactSecureString(args, MR_EXECUTOR_SSH_PASSWORD.key());
        redactSecureString(args, "token");
        return executable + " " + Commandline.toString(args);
    }
}
