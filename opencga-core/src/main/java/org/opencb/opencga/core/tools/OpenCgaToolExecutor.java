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

package org.opencb.opencga.core.tools;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.ConfigurationUtils;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class OpenCgaToolExecutor {

    public static final String EXECUTOR_ID = "executorId";

    private ObjectMap executorParams;
    private Path outDir;
    private ExecutionResultManager arm;

    protected OpenCgaToolExecutor() {
    }

    public final String getToolId() {
        return this.getClass().getAnnotation(ToolExecutor.class).tool();
    }

    public final String getId() {
        return this.getClass().getAnnotation(ToolExecutor.class).id();
    }

    public final ToolExecutor.Framework getFramework() {
        return this.getClass().getAnnotation(ToolExecutor.class).framework();
    }

    public final ToolExecutor.Source getSource() {
        return this.getClass().getAnnotation(ToolExecutor.class).source();
    }

    public final void setUp(ExecutionResultManager arm, ObjectMap executorParams, Path outDir) {
        this.arm = arm;
        this.executorParams = executorParams;
        this.outDir = outDir;
    }

    public final void execute() throws ToolException {
        try {
            run();
        } catch (ToolException e) {
            throw e;
        } catch (Exception e) {
            throw ToolExecutorException.exceptionAtToolExecutor(getId(), getToolId(), e);
        }
    }

    protected abstract void run() throws Exception;

    public final ObjectMap getExecutorParams() {
        return executorParams;
    }

    public final Path getOutDir() {
        return outDir;
    }

    protected final String getToken() {
        return getExecutorParams().getString(ParamConstants.TOKEN);
    }

    protected final String getAnalysisResourceUrl() throws ToolExecutorException {
        String opencgaHome = getExecutorParams().getString(ParamConstants.OPENCGA_HOME);
        if (StringUtils.isEmpty(opencgaHome)) {
            throw new ToolExecutorException("Missing OpenCGA home within the executor parameters");
        }
        try {
            Configuration configuration = ConfigurationUtils.loadConfiguration(opencgaHome);
            if (StringUtils.isEmpty(configuration.getAnalysis().getResources().getUrl())) {
                return ParamConstants.OPENCGA_ANALYSIS_RESOURCE_URL;
            }
            return configuration.getAnalysis().getResources().getUrl();
        } catch (IOException e) {
            throw new ToolExecutorException(e);
        }
    }

    protected final void addWarning(String warning) throws ToolException {
        arm.addWarning(warning);
    }

    protected final void addAttribute(String key, Object value) throws ToolException {
        arm.addStepAttribute(key, value);
    }

}
