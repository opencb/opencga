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

package org.opencb.opencga.analysis.variant.genomePlot;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.GenomePlot;
import org.opencb.biodata.models.clinical.qc.GenomePlotConfig;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.variant.GenomePlotAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.GenomePlotAnalysisExecutor;

import java.nio.file.Paths;

@Tool(id = GenomePlotAnalysis.ID, resource = Enums.Resource.VARIANT)
public class GenomePlotAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "genome-plot";
    public static final String DESCRIPTION = "Generate a genome plot for a given sample.";

    public final static String SUFFIX_FILENAME = ".genomePlot.png";

    @ToolParams
    private GenomePlotAnalysisParams genomePlotParams = new GenomePlotAnalysisParams();

    private java.io.File configFile;

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        if (StringUtils.isEmpty(genomePlotParams.getConfigFile())) {
            throw new ToolException("Missing Genome plot configuration file");
        }

        File catalogFile = AnalysisUtils.getCatalogFile(genomePlotParams.getConfigFile(), getStudy(), getCatalogManager().getFileManager(),
                getToken());
        configFile = Paths.get(catalogFile.getUri().getPath()).toFile();
        if (!configFile.exists()) {
            throw new ToolException("Invalid parameters: genome plot configuration file does not exist (" + configFile + ")");
        }
    }

    @Override
    protected void run() throws ToolException {
        step(getId(), () -> {
            getToolExecutor(GenomePlotAnalysisExecutor.class)
                    .setStudy(study)
                    .setConfigFile(configFile)
                    .execute();


            // Update quality control for the catalog sample
            if (StringUtils.isNotEmpty(genomePlotParams.getSample())) {
                OpenCGAResult<Sample> sampleResult = getCatalogManager().getSampleManager().get(getStudy(), genomePlotParams.getSample(),
                        QueryOptions.empty(), getToken());
                Sample sample = sampleResult.first();
                if (sample != null) {
                    GenomePlot genomePlot = null;
                    // Parse configuration file
                    GenomePlotConfig plotConfig = JacksonUtils.getDefaultObjectMapper().readerFor(GenomePlotConfig.class)
                            .readValue(configFile);
                    // Get image file
                    for (java.io.File imgFile : getOutDir().toFile().listFiles()) {
                        if (imgFile.getName().endsWith(GenomePlotAnalysis.SUFFIX_FILENAME)) {
                            int index = imgFile.getAbsolutePath().indexOf("JOBS/");
                            String relativeFilePath = (index == -1 ? imgFile.getName() : imgFile.getAbsolutePath().substring(index));
                            genomePlot = new GenomePlot("", getGenomePlotParams().getDescription(), plotConfig, relativeFilePath);
                            break;
                        }
                    }
                    if (genomePlot !=  null) {
                        SampleQualityControl qc = sampleResult.first().getQualityControl();
                        if (qc == null) {
                            qc = new SampleQualityControl();
                        }
                        qc.getVariantMetrics().getGenomePlots().add(genomePlot);

                        catalogManager.getSampleManager().update(getStudy(), sample.getId(), new SampleUpdateParams().setQualityControl(qc),
                                QueryOptions.empty(), getToken());
                    }
                }
            }
        });
    }

    public String getStudy() {
        return study;
    }

    public GenomePlotAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    public GenomePlotAnalysisParams getGenomePlotParams() {
        return genomePlotParams;
    }

    public GenomePlotAnalysis setGenomePlotParams(GenomePlotAnalysisParams genomePlotParams) {
        this.genomePlotParams = genomePlotParams;
        return this;
    }
}
