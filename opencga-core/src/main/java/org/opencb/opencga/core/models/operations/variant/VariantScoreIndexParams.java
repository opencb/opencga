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

package org.opencb.opencga.core.models.operations.variant;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.opencga.core.tools.ToolParams;

public class VariantScoreIndexParams extends ToolParams {

    public static final String SCORE_NAME = "Unique name of the score within the study";
    public static final String COHORT1 = "Cohort used to compute the score. "
            + "Use the cohort '" + StudyEntry.DEFAULT_COHORT + "' if all samples from the study where used to compute the score";
    public static final String COHORT2 = "Second cohort used to compute the score, typically to compare against the first cohort. "
            + "If only one cohort was used to compute the score, leave empty";
    public static final String INPUT_COLUMNS = "Indicate which columns to load from the input file. "
            + "Provide the column position (starting in 0) for the column with the score with 'SCORE=n'. "
            + "Optionally, the PValue column with 'PVALUE=n'. "
            + "The, to indicate the variant associated with the score, provide either the columns ['CHROM', 'POS', 'REF', 'ALT'], "
            + "or the column 'VAR' containing a variant representation with format 'chr:start:ref:alt'. "
            + "e.g. 'CHROM=0,POS=1,REF=3,ALT=4,SCORE=5,PVALUE=6' or 'VAR=0,SCORE=1,PVALUE=2'";
    public static final String RESUME = "Resume a previously failed indexation";

    public static final String DESCRIPTION = "Variant score index params. "
            + "scoreName: " + SCORE_NAME + ". "
            + "cohort1: " + COHORT1 + ". "
            + "cohort2: " + COHORT2 + ". "
            + "inputColumns: " + INPUT_COLUMNS + ". "
            + "resume: " + RESUME;

    private String scoreName;
    private String cohort1;
    private String cohort2;
    private String input;
    private String inputColumns;
    private boolean resume;

    public VariantScoreIndexParams() {
    }

    public VariantScoreIndexParams(String scoreName, String cohort1, String cohort2, String input, String inputColumns,
                                   boolean resume) {
        this.scoreName = scoreName;
        this.cohort1 = cohort1;
        this.cohort2 = cohort2;
        this.input = input;
        this.inputColumns = inputColumns;
        this.resume = resume;
    }

    public String getScoreName() {
        return scoreName;
    }

    public VariantScoreIndexParams setScoreName(String scoreName) {
        this.scoreName = scoreName;
        return this;
    }

    public String getCohort1() {
        return cohort1;
    }

    public VariantScoreIndexParams setCohort1(String cohort1) {
        this.cohort1 = cohort1;
        return this;
    }

    public String getCohort2() {
        return cohort2;
    }

    public VariantScoreIndexParams setCohort2(String cohort2) {
        this.cohort2 = cohort2;
        return this;
    }

    public String getInput() {
        return input;
    }

    public VariantScoreIndexParams setInput(String input) {
        this.input = input;
        return this;
    }

    public String getInputColumns() {
        return inputColumns;
    }

    public VariantScoreIndexParams setInputColumns(String inputColumns) {
        this.inputColumns = inputColumns;
        return this;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantScoreIndexParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }
}
