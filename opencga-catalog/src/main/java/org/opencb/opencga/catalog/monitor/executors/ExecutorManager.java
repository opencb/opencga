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

package org.opencb.opencga.catalog.monitor.executors;

import org.opencb.opencga.catalog.config.CatalogConfiguration;

/**
 * Created by pfurio on 22/08/16.
 */
public class ExecutorManager {

    // TODO: Change for a map
    private AbstractExecutor executor;

    public ExecutorManager(CatalogConfiguration catalogConfiguration) {
        if (catalogConfiguration != null) {
            if (catalogConfiguration.getExecution().getMode().equalsIgnoreCase("local")) {
                this.executor = new LocalExecutor();
            } else if (catalogConfiguration.getExecution().getMode().equalsIgnoreCase("sge")) {
                // init sge executor
                this.executor = new SGEExecutor(catalogConfiguration);
                System.out.println("SGE not ready");
            }
        }

        if (executor == null) {
            // Load default executor
            this.executor = new LocalExecutor();
        }
    }

    public AbstractExecutor getExecutor() {
        return this.executor;
    }

    // TODO: CreateCommandLine (static method)
}
