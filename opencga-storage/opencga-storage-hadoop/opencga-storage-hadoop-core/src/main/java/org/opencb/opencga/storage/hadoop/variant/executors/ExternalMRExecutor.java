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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;

import java.util.List;

/**
 * Created on 18/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ExternalMRExecutor implements MRExecutor {
    private final List<String> env;

    public ExternalMRExecutor(ObjectMap options) {
        env = options.getAsStringList(HadoopVariantStorageEngine.HADOOP_ENV);
    }

    @Override
    public int run(String executable, String args) {
        return run(executable + " " + args);
    }

    public int run(String commandLine) {
//        if (env != null) {
//            for (String s : env) {
//                System.out.println("env = " + s);
//            }
//        }
        Command command = new Command(commandLine, env);
        command.run();
        return command.getExitValue();
    }
}
