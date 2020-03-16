package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.models.variant.MendelianErrorReport;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.HashMap;
import java.util.Map;

public abstract class MendelianErrorAnalysisExecutor extends OpenCgaToolExecutor {

    private String studyId;
    private String familyId;

    private MendelianErrorReport mendelianErrorReport;

    public MendelianErrorAnalysisExecutor() {
    }

    public String getStudyId() {
        return studyId;
    }

    public MendelianErrorAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public MendelianErrorAnalysisExecutor setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public MendelianErrorReport getMendelianErrorReport() {
        return mendelianErrorReport;
    }

    public MendelianErrorAnalysisExecutor setMendelianErrorReport(MendelianErrorReport mendelianErrorReport) {
        this.mendelianErrorReport = mendelianErrorReport;
        return this;
    }
}
