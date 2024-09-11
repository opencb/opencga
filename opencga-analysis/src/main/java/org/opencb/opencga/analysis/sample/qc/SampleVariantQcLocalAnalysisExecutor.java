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

package org.opencb.opencga.analysis.sample.qc;

import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.individual.qc.IndividualVariantQcAnalysis;
import org.opencb.opencga.analysis.utils.VariantQcAnalysisExecutorUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.variant.SampleQcAnalysisParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.SampleVariantQcAnalysisExecutor;

import java.io.IOException;
import java.nio.file.Path;

import static org.opencb.opencga.analysis.utils.VariantQcAnalysisExecutorUtils.CONFIG_FILENAME;
import static org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis.SAMPLE_QC_TYPE;

@ToolExecutor(id="opencga-local", tool = IndividualVariantQcAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class SampleVariantQcLocalAnalysisExecutor extends SampleVariantQcAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolExecutorException {
        Path configPath = getOutDir().resolve(CONFIG_FILENAME);
        ObjectWriter objectWriter = JacksonUtils.getDefaultObjectMapper().writerFor(SampleQcAnalysisParams.class);
        try {
            objectWriter.writeValue(configPath.toFile(), getQcParams());
        } catch (IOException e) {
            throw new ToolExecutorException(e);
        }

        VariantQcAnalysisExecutorUtils.run(SAMPLE_QC_TYPE, getVcfPaths(), getJsonPaths(), configPath, getOutDir(), getOpencgaHome());
    }
}
