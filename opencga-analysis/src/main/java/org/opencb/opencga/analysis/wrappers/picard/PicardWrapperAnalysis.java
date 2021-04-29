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

package org.opencb.opencga.analysis.wrappers.picard;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.PicardWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import static org.opencb.opencga.core.api.ParamConstants.PICARD_COMMANDS_SUPPORTED;
import static org.opencb.opencga.core.api.ParamConstants.PICARD_COMMAND_DESCRIPTION;

@Tool(id = PicardWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = PicardWrapperAnalysis.DESCRIPTION)
public class PicardWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "picard";
    public final static String DESCRIPTION = "Picard is a set of command line tools (in Java) for manipulating high-throughput sequencing"
            + " (HTS) data and formats such as SAM/BAM/CRAM and VCF. " + PICARD_COMMAND_DESCRIPTION;

    @ToolParams
    protected final PicardWrapperParams analysisParams = new PicardWrapperParams();

    private String bamFilePath = null;
    private String bedFilePath = null;
    private String baitIntervalsFilePath = null;
    private String targetIntervalsFilePath = null;
    private String dictFilePath = null;
    private String refSeqFilePath = null;

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(analysisParams.getCommand())) {
            throw new ToolException("Missig Picard command.");
        }

        if (!AnalysisUtils.isSupportedCommand(PICARD_COMMANDS_SUPPORTED)) {
            throw new ToolException("Picard command '" + analysisParams.getCommand() + "' is not available. Supported commands are "
                    + PICARD_COMMANDS_SUPPORTED);
        }

        // Get files from catalog
        FileManager fileManager = catalogManager.getFileManager();
        if (StringUtils.isNotEmpty(analysisParams.getBamFile())) {
            bamFilePath = AnalysisUtils.getCatalogFile(analysisParams.getBamFile(), study, fileManager, token).getUri().getPath();
        }
        if (StringUtils.isNotEmpty(analysisParams.getBedFile())) {
            bedFilePath = AnalysisUtils.getCatalogFile(analysisParams.getBedFile(), study, fileManager, token).getUri().getPath();
        }
        if (StringUtils.isNotEmpty(analysisParams.getBaitIntervalsFile())) {
            baitIntervalsFilePath = AnalysisUtils.getCatalogFile(analysisParams.getBaitIntervalsFile(), study, fileManager, token).getUri()
                    .getPath();
        }
        if (StringUtils.isNotEmpty(analysisParams.getTargetIntervalsFile())) {
            targetIntervalsFilePath = AnalysisUtils.getCatalogFile(analysisParams.getTargetIntervalsFile(), study, fileManager,
                    token).getUri().getPath();
        }
        if (StringUtils.isNotEmpty(analysisParams.getDictFile())) {
            dictFilePath = AnalysisUtils.getCatalogFile(analysisParams.getDictFile(), study, fileManager, token)
                    .getUri().getPath();
        }
        if (StringUtils.isNotEmpty(analysisParams.getRefSeqFile())) {
            refSeqFilePath = AnalysisUtils.getCatalogFile(analysisParams.getRefSeqFile(), study, fileManager, token)
                    .getUri().getPath();
        }
    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        step(() -> {
            if (MapUtils.isNotEmpty(analysisParams.getPicardParams())) {
                executorParams.appendAll(analysisParams.getPicardParams());
            }

            getToolExecutor(PicardWrapperAnalysisExecutor.class)
                    .setCommand(analysisParams.getCommand())
                    .setBamFile(bamFilePath)
                    .setBedFile(bedFilePath)
                    .setBaitIntervalsFile(baitIntervalsFilePath)
                    .setTargetIntervalsFile(targetIntervalsFilePath)
                    .setDictFile(dictFilePath)
                    .setRefSeqFile(refSeqFilePath)
                    .setOutFilename(analysisParams.getOutFilename())
                    .execute();
        });
    }
}
