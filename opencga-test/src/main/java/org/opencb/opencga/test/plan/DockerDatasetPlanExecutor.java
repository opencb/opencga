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

package org.opencb.opencga.test.plan;

import org.opencb.commons.utils.DockerUtils;
import org.opencb.commons.utils.PrintUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DockerDatasetPlanExecutor implements PlanExecutor {


    public void execute(List<DatasetPlanExecution> datasetPlanExecutionList) {

        for (DatasetPlanExecution datasetPlanExecution : datasetPlanExecutionList) {
            Map<String, List<String>> result = new HashMap<>();
            for (String filename : datasetPlanExecution.getCommands().keySet()) {
                List<String> dockerCommands = new ArrayList<>();
                int i = 0;
                for (CommandDataSet command : datasetPlanExecution.getCommands().get(filename)) {
                    dockerCommands.add(DockerUtils.buildMountPathsCommandLine(command.getImage(), command.getCommandLine()));
                }
                result.put(filename, dockerCommands);
            }
            PrintUtils.println(datasetPlanExecution.getEnvironment().getId(), PrintUtils.Color.YELLOW);
            for (String filename : result.keySet()) {
                PrintUtils.println("    " + filename, PrintUtils.Color.GREEN);
                for (String command : result.get(filename)) {
                    PrintUtils.println("    " + command.replaceAll("//", "/"), PrintUtils.Color.WHITE);
                }
            }
        }
   /*  for (DatasetPlanExecution datasetPlanExecution : datasetPlanExecutionList) {
            PrintUtils.println(datasetPlanExecutionList.getId(), PrintUtils.Color.YELLOW);
            for (String filename : commands.keySet()) {
                PrintUtils.println("    " + filename, PrintUtils.Color.GREEN);
                for (String command : commands.get(filename)) {
                    PrintUtils.println("    " + command, PrintUtils.Color.WHITE);
                }
            }
        }*/
    }
}
