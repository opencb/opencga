package org.opencb.opencga.analysis.variant.mendelianError;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.variant.geneticChecks.GeneticChecksUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.variant.MendelianErrorReport;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.MendelianErrorAnalysisExecutor;

import java.io.IOException;

@Tool(id = MendelianErrorAnalysis.ID, resource = Enums.Resource.VARIANT, description = MendelianErrorAnalysis.DESCRIPTION)
public class MendelianErrorAnalysis extends OpenCgaTool {

    public static final String ID = "mendelian-error";
    public static final String DESCRIPTION = "Run mendelian error analysis to infer uniparental disomy regions.";

    private String studyId;
    private String familyId;
    private String individualId;
    private String sampleId;

    public MendelianErrorAnalysis() {
    }

    /**
     * Study of the samples.
     * @param studyId Study id
     * @return this
     */
    public MendelianErrorAnalysis setStudy(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public MendelianErrorAnalysis setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public MendelianErrorAnalysis setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public MendelianErrorAnalysis setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(studyId);

        if (StringUtils.isEmpty(studyId)) {
            throw new ToolException("Missing study ID.");
        }

        try {
            studyId = catalogManager.getStudyManager().get(studyId, null, token).first().getFqn();
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Sanity check
        if (StringUtils.isNotEmpty(familyId) && StringUtils.isNotEmpty(individualId) && StringUtils.isNotEmpty(sampleId)) {
            throw new ToolException("Incorrect parameters: please, provide only a family ID, a individual ID or a sample ID.");
        }

        // Get family by ID
        Family family;
        if (StringUtils.isNotEmpty(familyId)) {
            // Get family ID by individual ID
            family = GeneticChecksUtils.getFamilyById(studyId, familyId, catalogManager, token);
        } else if (StringUtils.isNotEmpty(individualId)) {
            // Get family ID by individual ID
            family = GeneticChecksUtils.getFamilyByIndividualId(studyId, individualId, catalogManager, token);
        } else if (StringUtils.isNotEmpty(sampleId)) {
            // Get family ID by sample ID
            family = GeneticChecksUtils.getFamilyBySampleId(studyId, sampleId, catalogManager, token);
        } else {
            throw new ToolException("Missing a family ID, a individual ID or a sample ID.");
        }
        if (family == null) {
            throw new ToolException("Members not found to execute genetic checks analysis.");
        }

        familyId = family.getId();
    }


    @Override
    protected void run() throws ToolException {

        step(ID, () -> {
            MendelianErrorAnalysisExecutor mendelianErrorExecutor = getToolExecutor(MendelianErrorAnalysisExecutor.class);

            mendelianErrorExecutor.setStudyId(studyId)
                    .setFamilyId(familyId)
                    .execute();

            try {
                // Save inferred sex report
                MendelianErrorReport report = mendelianErrorExecutor.getMendelianErrorReport();
                JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve(ID + ".report.json").toFile(), report);
            } catch (IOException e) {
                throw new ToolException(e);
            }
        });
    }
}
