package org.opencb.opencga.analysis.variant.mutationalSignature;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

@Tool(id = MutationalSignatureAnalysis.ID, resource = Enums.Resource.VARIANT)
public class MutationalSignatureAnalysis extends OpenCgaTool {

    public static final String ID = "mutational-signature";
    public static final String DESCRIPTION = "Run mutational signature analysis for a given sample.";

    private String study;
    private String sampleName;

//    private List<String> checkedSamplesList;
    private Path outputFile;

    /**
     * Study of the sample.
     * @param study Study id
     * @return this
     */
    public MutationalSignatureAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    /**
     * Sample.
     * @param sampleName Sample name
     * @return this
     */
    public MutationalSignatureAnalysis setSampleName(String sampleName) {
        this.sampleName = sampleName;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (study == null || study.isEmpty()) {
            throw new ToolException("Missing study");
        }
        try {
            study = catalogManager.getStudyManager().get(study, null, token).first().getFqn();

            if (StringUtils.isNotEmpty(sampleName)) {
                OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(study, sampleName, new QueryOptions(), token);
                if (sampleResult.getNumResults() != 1) {
                    throw new ToolException("Unable to compute mutational signature analysis. Sample '" + sampleName + "' not found");
                }
            }

//            // Remove non-indexed samples
//            Set<String> indexedSamples = variantStorageManager.getIndexedSamples(study, token);
//            allSamples.removeIf(s -> !indexedSamples.contains(s));

        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        addAttribute("sampleName", sampleName);
        outputFile = getOutDir().resolve("mutational_signature_analysis.json");
    }

    @Override
    protected void run() throws ToolException {
        step(getId(), () -> {
            getToolExecutor(MutationalSignatureAnalysisExecutor.class)
                    .setStudy(study)
                    .setOutputFile(outputFile)
                    .setSampleName(sampleName)
                    .execute();
        });
    }
}
