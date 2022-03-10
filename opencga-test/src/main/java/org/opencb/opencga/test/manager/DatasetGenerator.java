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

import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.config.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DatasetGenerator {


    private Configuration configuration;

    public DatasetGenerator(Configuration configuration) {
        this.configuration = configuration;
    }


    /**
     * Process a list of given environments.
     *
     * @return
     * @throws IOException
     */
    public Map<String, List<String>> generateCommandLines() throws IOException {
        DatasetCommandLineGenerator datasetCommandLineGenerator = new DatasetCommandLineGenerator(configuration);
        return datasetCommandLineGenerator.generateCommandLines();
    }

    /**
     * Executes (creates) the VCF, ...
     *
     * @throws IOException
     */
    public void execute() throws IOException {
        Map<String, List<String>> commandLinesMap = this.generateCommandLines();
        DatasetCommandLineExecutor executor = new DatasetCommandLineExecutor(commandLinesMap, configuration);
        executor.execute();
    }

    /**
     * Dry-run of all CLIs.
     *
     * @throws IOException
     */
    public void simulate() throws IOException {
        Map<String, List<String>> commandLinesMap = this.generateCommandLines();
        for (String env : commandLinesMap.keySet()) {
            PrintUtils.println(env, PrintUtils.Color.YELLOW);
            for (String line : commandLinesMap.get(env)) {
                PrintUtils.println("    " + line, PrintUtils.Color.GREEN);
            }
        }
    }

}
