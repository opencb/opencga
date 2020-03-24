package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.HashMap;
import java.util.Map;

public abstract class InferredSexAnalysisExecutor extends OpenCgaToolExecutor {

    private String studyId;
    private String sampleId;

    private InferredSexReport inferredSexReport;

    public InferredSexAnalysisExecutor() {
    }

    public String getStudyId() {
        return studyId;
    }

    public InferredSexAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public InferredSexAnalysisExecutor setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public InferredSexReport getInferredSexReport() {
        return inferredSexReport;
    }

    public InferredSexAnalysisExecutor setInferredSexReport(InferredSexReport inferredSexReport) {
        this.inferredSexReport = inferredSexReport;
        return this;
    }

    // TODO use cellbase
    public static final Map<String, Integer> GRCH37_CHROMOSOMES = new HashMap<String, Integer>() {{
        put("1", 249250621);
        put("2", 243199373);
        put("3", 198022430);
        put("4", 191154276);
        put("5", 180915260);
        put("6", 171115067);
        put("7", 159138663);
        put("8", 146364022);
        put("9", 141213431);
        put("10", 135534747);
        put("11", 135006516);
        put("12", 133851895);
        put("13", 115169878);
        put("14", 107349540);
        put("15", 102531392);
        put("16", 90354753);
        put("17", 81195210);
        put("18", 78077248);
        put("19", 59128983);
        put("20", 63025520);
        put("21", 48129895);
        put("22", 51304566);
        put("X", 155270560);
        put("Y", 59373566);
    }};

    public static final Map<String, Integer> GRCH38_CHROMOSOMES = new HashMap<String, Integer>() {{
        put("1", 248956422);
        put("2", 242193529);
        put("3", 198295559);
        put("4", 190214555);
        put("5", 181538259);
        put("6", 170805979);
        put("7", 159345973);
        put("8", 145138636);
        put("9", 138394717);
        put("10", 133797422);
        put("11", 135086622);
        put("12", 133275309);
        put("13", 114364328);
        put("14", 107043718);
        put("15", 101991189);
        put("16", 90338345);
        put("17", 83257441);
        put("18", 80373285);
        put("19", 58617616);
        put("20", 64444167);
        put("21", 46709983);
        put("22", 50818468);
        put("X", 156040895);
        put("Y", 57227415);
    }};
}
