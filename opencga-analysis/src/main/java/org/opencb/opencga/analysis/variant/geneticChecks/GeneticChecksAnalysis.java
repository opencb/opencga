package org.opencb.opencga.analysis.variant.geneticChecks;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.GeneticChecksReport;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.GeneticChecksAnalysisExecutor;

import java.io.IOException;
import java.util.*;

@Tool(id = GeneticChecksAnalysis.ID, resource = Enums.Resource.VARIANT, description = GeneticChecksAnalysis.DESCRIPTION)
public class GeneticChecksAnalysis extends OpenCgaTool {

    public static final String ID = "genetic-checks";
    public static final String DESCRIPTION = "Run genetic checks for sex, relatedness and mendelian errors (UDP).";
    public static final String VARIABLE_SET_ID = "opencga_family_genetic_checks";

    private String studyId;
    private String familyId;
    private String individualId;
    private String sampleId;
    private String minorAlleleFreq;
    private String relatednessMethod;

    // Internal members
    private List<Individual> individuals;
    private GeneticChecksReport report;

    public GeneticChecksAnalysis() {
    }

    /**
     * Study of the samples.
     * @param studyId Study id
     * @return this
     */
    public GeneticChecksAnalysis setStudy(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public GeneticChecksAnalysis setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public GeneticChecksAnalysis setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public GeneticChecksAnalysis setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public GeneticChecksAnalysis setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public GeneticChecksAnalysis setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
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

        // Get relatives, i.e., members of a family
        if (StringUtils.isNotEmpty(familyId)) {
            // Check and get the individuals from that family ID
            individuals = GeneticChecksUtils.getIndividualsByFamilyId(studyId, familyId, catalogManager, token);
        } else if (StringUtils.isNotEmpty(individualId)) {
            // Get father, mother and siblings from that individual
            individuals = GeneticChecksUtils.getRelativesByIndividualId(studyId, individualId, catalogManager, token);
        } else if (StringUtils.isNotEmpty(sampleId)) {
            // Get father, mother and siblings from that sample
            individuals = GeneticChecksUtils.getRelativesBySampleId(studyId, individualId, catalogManager, token);
        } else {
            throw new ToolException("Missing a family ID, a individual ID or a sample ID.");
        }

        if (CollectionUtils.isEmpty(individuals)) {
            throw new ToolException("Members not found to execute genetic checks analysis.");
        }
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add("sex");
        steps.add("relatedness");
        steps.add("mendelian-errors");
        steps.add("index-variable-set");
        return steps;
    }

    @Override
    protected void run() throws ToolException {

        GeneticChecksAnalysisExecutor executor = getToolExecutor(GeneticChecksAnalysisExecutor.class);

        executor.setStudyId(studyId)
                .setIndividuals(individuals)
                .setMinorAlleleFreq(minorAlleleFreq)
                .setRelatednessMethod(relatednessMethod);

        step("sex", () -> {
            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.INFERRED_SEX).execute();
        });

        step("relatedness", () -> {
            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.RELATEDNESS).execute();
        });

        step("mendelian-errors", () -> {
            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.MENDELIAN_ERRORS).execute();
        });

        // Save results
        try {
            JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve(ID + ".report.json").toFile(),
                    executor.getReport());
        } catch (IOException e) {
            throw new ToolException(e);
        }

        // Index as a variable set
        step("index-variable-set", () -> {
            indexResults(executor.getReport());
        });
    }

    private void indexResults(GeneticChecksReport result) throws ToolException {
//        try {
//            Map<String, Object> annotations = buildAnnotations(result);
//
//            AnnotationSet annotationSet = new AnnotationSet(VARIABLE_SET_ID, VARIABLE_SET_ID, annotations);
//
//            FamilyUpdateParams updateParams = new FamilyUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));
//
//            catalogManager.getFamilyManager().update(study, family, updateParams, QueryOptions.empty(), token);
//        } catch (CatalogException e) {
//            throw new ToolException(e);
//        }
    }

    private Map<String, Object> buildAnnotations(GeneticChecksReport result) {
        Map<String, Object> annot = new HashMap<>();
        annot.put("familyId", result.getFamilyId());

        annot.put("fatherId", result.getFatherId());
        annot.put("motherId", result.getMotherId());
        annot.put("childrenIds", result.getChildrenIds());

        annot.put("sexReport", result.getInferredSexReport());
        annot.put("relatednessReport", result.getRelatednessReport());
        annot.put("mendelianErrorsReport", result.getMendelianErrorsReport());

        return annot;
    }
}
