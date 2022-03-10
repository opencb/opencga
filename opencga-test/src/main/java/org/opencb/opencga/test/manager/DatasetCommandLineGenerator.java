/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.test.manager;

import org.opencb.opencga.test.config.Configuration;
import org.opencb.opencga.test.config.Env;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DatasetCommandLineGenerator {

    private Configuration configuration;

    public DatasetCommandLineGenerator(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Allows to only process a list of given environments.
     * @param environments
     * @return
     * @throws IOException
     */
    public Map<String, List<String>> generateCommandLines(List<String> environments) throws IOException {
        // logger.debug("Processing the following environments: {}", environments.toString())

        Map<String, List<String>> commandLinesMap = new HashMap<>();
        for (String environment : environments) {
            List<String> commandLines = this.generateCommandLines(environment);
            commandLinesMap.put(environment, commandLines);
        }
        return commandLinesMap;
    }

    /**
     * Generate the CLIs for a single environment.
     * @param environment
     * @return
     */
    public List<String> generateCommandLines(String environment) {
        // logger.debug("Processing the environment: {}", environment)

        // Search the environment object to be processed
        List<Env> environmentList = configuration.getEnvs().stream()
                .filter(env -> env.getId().equals(environment)).collect(Collectors.toList());

        List<String> commandLines = new ArrayList<>();
        if (environmentList.size() == 1) {
            // There should be ONLY one result
            Env env = environmentList.get(0);
            // TODO Juanfe: create CLIs here
        } else {
            // logger.error(...)
        }

        return commandLines;
    }

}
