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

package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.operations.variant.VariantScoreIndexParams;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;

import java.net.URI;

import static org.opencb.opencga.core.api.ParamConstants.STUDY_PARAM;

@Tool(id= VariantScoreIndexOperationTool.ID, description = VariantScoreIndexOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantScoreIndexOperationTool extends OperationTool {

    public static final String ID = "variant-score-index";
    public static final String DESCRIPTION = "Index a variant score in the database.";


    private VariantScoreFormatDescriptor descriptor;
    private VariantScoreIndexParams indexParams = new VariantScoreIndexParams();
    private URI scoreFile;

    public VariantScoreIndexOperationTool setInput(URI scoreFile) {
        indexParams.setInput(scoreFile.toString());
        return this;
    }

    public VariantScoreIndexOperationTool setScoreName(String scoreName) {
        indexParams.setScoreName(scoreName);
        return this;
    }

    public VariantScoreIndexOperationTool setCohort1(String cohort1) {
        indexParams.setCohort1(cohort1);
        return this;
    }

    public VariantScoreIndexOperationTool setCohort2(String cohort2) {
        indexParams.setCohort2(cohort2);
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();

        indexParams.updateParams(params);

        scoreFile = UriUtils.createUri(indexParams.getInput());

        descriptor = new VariantScoreFormatDescriptor();
        for (String column : indexParams.getInputColumns().split(",")) {
            String[] split = column.split("=");
            if (split.length != 2) {
                throw new IllegalArgumentException("Malformed value '" + column + "'. Please, use COLUMN=INDEX");
            }
            int columnIdx = Integer.parseInt(split[1]);
            switch (split[0].toUpperCase()) {
                case "SCORE":
                    descriptor.setScoreColumnIdx(columnIdx);
                    break;
                case "PVALUE":
                    descriptor.setPvalueColumnIdx(columnIdx);
                    break;
                case "VAR":
                    descriptor.setVariantColumnIdx(columnIdx);
                    break;
                case "CHR":
                case "CHROM":
                    descriptor.setChrColumnIdx(columnIdx);
                    break;
                case "POS":
                    descriptor.setPosColumnIdx(columnIdx);
                    break;
                case "REF":
                    descriptor.setRefColumnIdx(columnIdx);
                    break;
                case "ALT":
                    descriptor.setAltColumnIdx(columnIdx);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown column " + split[0].toUpperCase() + ". "
                            + "Known columns are: ['SCORE','PVALUE','VAR','CHROM','POS','REF','ALT']");
            }
        }
        descriptor.checkValid();
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            variantStorageManager.variantScoreLoad(
                    getStudyFqn(),
                    scoreFile,
                    indexParams.getScoreName(),
                    indexParams.getCohort1(),
                    indexParams.getCohort2(),
                    descriptor,
                    params,
                    token);
        });
    }
}
