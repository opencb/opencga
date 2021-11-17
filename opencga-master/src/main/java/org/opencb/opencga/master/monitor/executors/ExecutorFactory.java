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

package org.opencb.opencga.master.monitor.executors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.Execution;

/**
 * Created by pfurio on 22/08/16.
 */
public class ExecutorFactory {

    // TODO: Change for a map
    private BatchExecutor executor;

    public ExecutorFactory(Configuration configuration) {
        Execution execution = configuration.getAnalysis().getExecution();
        String mode = StringUtils.isEmpty(execution.getId())
                ? "local" : execution.getId().toLowerCase();

        switch (mode) {
            case "local":
                this.executor = new LocalExecutor(execution);
                break;
            case "sge":
                this.executor = new SGEExecutor(execution);
                break;
            case "azure":
            case "azure-batch":
                this.executor = new AzureBatchExecutor(execution);
                break;
            case "k8s":
            case "kubernetes":
                this.executor = new K8SExecutor(configuration);
                break;
            default:
                throw new UnsupportedOperationException("nonsoupported execution mode { " + mode
                        + " }, accepted modes are : local, sge, azure");
        }
    }

    public BatchExecutor getExecutor() {
        return this.executor;
    }

    // TODO: CreateCommandLine (static method)
}
