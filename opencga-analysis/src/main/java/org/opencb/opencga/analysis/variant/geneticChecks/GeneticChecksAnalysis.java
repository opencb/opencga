package org.opencb.opencga.analysis.variant.geneticChecks;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.FamilyUpdateParams;
import org.opencb.opencga.core.models.variant.GeneticChecksReport;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.GeneticChecksAnalysisExecutor;

import java.io.IOException;
import java.util.*;

@Tool(id = GeneticChecksAnalysis.ID, resource = Enums.Resource.VARIANT, description = GeneticChecksAnalysis.DESCRIPTION)
public class GeneticChecksAnalysis extends OpenCgaTool {

    public static final String ID = "genetic-checks";
    public static final String DESCRIPTION = "Run genetic checks for sex, relatedness and mendelian errors (UDP).";
    public static final String VARIABLE_SET_ID = "opencga_family_genetic_checks";

    private String study;
    private String family;
    private String individual;
    private String sample;
    private String minorAlleleFreq;
    private String relatednessMethod;

    private List<String> samples;
    private GeneticChecksReport output;

    public GeneticChecksAnalysis() {
    }

    /**
     * Study of the samples.
     * @param study Study id
     * @return this
     */
    public GeneticChecksAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public GeneticChecksAnalysis setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getIndividual() {
        return individual;
    }

    public GeneticChecksAnalysis setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public GeneticChecksAnalysis setSample(String sample) {
        this.sample = sample;
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
        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(study)) {
            throw new ToolException("Missing study!");
        }

        try {
            study = catalogManager.getStudyManager().get(study, null, token).first().getFqn();
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Get sample IDs from family ID
        samples = GeneticChecksUtils.getSamples(study, family, catalogManager, token);
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

        executor.setStudy(study)
                .setFamily(family)
                .setSamples(samples)
                .setMinorAlleleFreq(minorAlleleFreq)
                .setRelatednessMethod(relatednessMethod);

        step("sex", () -> {
            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.SEX).execute();
        });

        step("relatedness", () -> {
            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.RELATEDNESS).execute();
        });

        step("mendelian-errors", () -> {
            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.MENDELIAN_ERRORS).execute();
        });

        // Save results
        try {
            JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve("genetic_checks.json").toFile(),
                    executor.getOutput());
        } catch (IOException e) {
            throw new ToolException(e);
        }

        // Index as a variable set
        step("index-variable-set", () -> {
            indexResults(executor.getOutput());
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

        annot.put("sexReport", result.getSexReport());
        annot.put("relatednessReport", result.getRelatednessReport());
        annot.put("mendelianErrorsReport", result.getMendelianErrorsReport());

        return annot;
    }
}
