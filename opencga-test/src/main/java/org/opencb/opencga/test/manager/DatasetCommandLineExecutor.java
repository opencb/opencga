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

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DatasetCommandLineExecutor {

    private Map<String, List<String>> commandLines;
    private Configuration configuration;

    public DatasetCommandLineExecutor(Map<String, List<String>> commandLines, Configuration configuration) {
        this.commandLines = commandLines;
        this.configuration = configuration;
    }

    public void check() throws IOException {
        // TODO Juanfe: check images, datasets, ...

    }

    public void execute() {

    }
}
