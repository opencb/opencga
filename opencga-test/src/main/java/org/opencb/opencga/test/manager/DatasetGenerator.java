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

import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.test.config.Configuration;
import org.opencb.opencga.test.config.Env;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class DatasetGenerator {

    private List<String> environments;
    private Configuration configuration;

    public DatasetGenerator(Configuration configuration) {
        this(configuration, null);
    }

    public DatasetGenerator(Configuration configuration, List<String> environments) {
        this.configuration = configuration;
        this.environments = environments;

        this.init();
    }

    private void init() {
        // TODO Juanfe: Create logger

        // Init environments
        if (environments == null || environments.size() == 0) {
            environments = configuration.getEnvs().stream().map(Env::getId).collect(Collectors.toList());
        }
    }

    /**
     * Checks Configuration file is correct.
     * @throws IOException
     */
    public void check() throws IOException {
        for (Env env : configuration.getEnvs()) {
            FileUtils.checkFile(Paths.get(env.getReference().getPath()));
            FileUtils.checkFile(Paths.get(env.getDataset().getPath()));
            // TODO Juanfe: add other checks
        }
    }

    /**
     * Process a list of given environments.
     * @return
     * @throws IOException
     */
    public Map<String, List<String>> generateCommandLines() throws IOException {
        // Check configuration file is correct
        this.check();

        DatasetCommandLineGenerator datasetCommandLineGenerator = new DatasetCommandLineGenerator(configuration);
        return datasetCommandLineGenerator.generateCommandLines(environments);
    }

    /**
     * Executes (creates) the VCF, ...
     * @throws IOException
     */
    public void execute() throws IOException {
        Map<String, List<String>> commandLinesMap = this.generateCommandLines();
        DatasetCommandLineExecutor executor = new DatasetCommandLineExecutor(commandLinesMap, configuration);
        executor.execute();
    }

    /**
     * Dry-run of all CLIs.
     * @throws IOException
     */
    public void simulate() throws IOException {
        Map<String, List<String>> commandLinesMap = this.generateCommandLines();
        // TODO Juanfe: print all CLIs
    }

}
