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

package org.opencb.opencga.analysis.family;

import org.opencb.opencga.core.config.ConfigurationUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.PedigreeGraphUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.family.PedigreeGraphAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@ToolExecutor(id="opencga-local", tool = PedigreeGraphAnalysisExecutor.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class PedigreeGraphLocalAnalysisExecutor extends PedigreeGraphAnalysisExecutor {

    private Path opencgaHome;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException, CatalogException, IOException, StorageEngineException {
        opencgaHome = Paths.get(getExecutorParams().getString("opencgaHome"));
        Configuration configuration = ConfigurationUtils.loadConfiguration(opencgaHome.toAbsolutePath().toString());

         // Run R script for fitting signature
        PedigreeGraphUtils.createPedigreeGraph(getFamily(), opencgaHome.resolve("analysis/" + PedigreeGraphAnalysisExecutor.ID),
                getOutDir(), configuration);
    }
}
