package org.opencb.opencga.analysis.variant.geneticChecks;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.GeneticChecksAnalysisExecutor;
import org.opencb.opencga.core.tools.variant.RelatednessAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.ArrayList;
import java.util.List;

@Tool(id = GeneticChecksAnalysis.ID, resource = Enums.Resource.VARIANT, description = GeneticChecksAnalysis.DESCRIPTION)
public class GeneticChecksAnalysis extends OpenCgaTool {

    public static final String ID = "genetic-checks";
    public static final String DESCRIPTION = "Run genetic checks for sex, relatedness and mendelian errors (UDP).";

    private String study;
    private List<String> samples;

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

    public List<String> getSamples() {
        return samples;
    }

    public GeneticChecksAnalysis setSamples(List<String> samples) {
        this.samples = samples;
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

        // check read permission
        try {
            List<String> allSamples = new ArrayList<>();
            allSamples.addAll(samples);
            variantStorageManager.checkQueryPermissions(
                    new Query()
                            .append(VariantQueryParam.STUDY.key(), study)
                            .append(VariantQueryParam.INCLUDE_SAMPLE.key(), allSamples),
                    new QueryOptions(),
                    token);
        } catch (CatalogException | StorageEngineException e) {
            throw new ToolException(e);
        }
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add("sex");
        steps.add("relatedness");
        steps.add("mendelian-errors");
        return steps;
    }

    @Override
    protected void run() throws ToolException {

        GeneticChecksAnalysisExecutor executor = getToolExecutor(GeneticChecksAnalysisExecutor.class);

        executor.setStudy(study)
                .setSamples(samples);

        step("sex", () -> {
//            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.SEX).execute();
        });

        step("relatedness", () -> {
//            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.RELATEDNESS).execute();
        });

        step("mendelian-errors", () -> {
            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.MENDELIAN_ERRORS).execute();
        });
    }
}
