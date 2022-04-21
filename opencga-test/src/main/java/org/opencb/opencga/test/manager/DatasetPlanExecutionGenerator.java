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
import org.opencb.opencga.test.execution.LocalDatasetExecutor;
import org.opencb.opencga.test.execution.models.DatasetExecutionPlan;

import java.io.IOException;
import java.util.List;

public class DatasetPlanExecutionGenerator {


    private Configuration configuration;

    public DatasetPlanExecutionGenerator(Configuration configuration) {
        this.configuration = configuration;
    }


    /**
     * Process a list of given environments.
     *
     * @param
     * @return
     * @throws IOException
     */
    public List<DatasetExecutionPlan> generateCommandLines(boolean resume) throws IOException {
        DatasetCommandLineGenerator datasetCommandLineGenerator = new DatasetCommandLineGenerator(configuration);
        return datasetCommandLineGenerator.generateCommandLines(resume);
    }

    /**
     * Executes (creates) the VCF, must check execution configuration...
     *
     * @throws IOException
     */
    public void execute() throws IOException {
        List<DatasetExecutionPlan> commandLinesMap = this.generateCommandLines(false);
        LocalDatasetExecutor executor = new LocalDatasetExecutor(configuration);
        executor.execute(commandLinesMap);
    }

    /**
     * Dry-run of all CLIs.
     *
     * @throws IOException
     */
    public void simulate() throws IOException {
        List<DatasetExecutionPlan> datasetPlanExecutions = this.generateCommandLines(false);
        for (DatasetExecutionPlan datasetExecutionPlan : datasetPlanExecutions) {
            datasetExecutionPlan.simulate();
        }
    }

    public void resume() throws IOException {
        List<DatasetExecutionPlan> commandLinesMap = this.generateCommandLines(true);
        LocalDatasetExecutor executor = new LocalDatasetExecutor(configuration);
        executor.execute(commandLinesMap);
    }

    public void mutate() throws IOException {
        // List<DatasetExecutionPlan> commandLinesMap = this.generateCommandLines(true);
        LocalDatasetExecutor executor = new LocalDatasetExecutor(configuration);
        executor.mutate();
    }
}
