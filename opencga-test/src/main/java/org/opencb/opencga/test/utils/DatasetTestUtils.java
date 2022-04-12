package org.opencb.opencga.test.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.opencga.test.config.Caller;
import org.opencb.opencga.test.config.Environment;
import org.opencb.opencga.test.execution.models.DatasetExecutionPlan;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DatasetTestUtils {


    private static final String FILE_REFERENCE = "{INPUT_FILE}";
    private static final String SAMTOOLS_BAM = "samtools view -bS " + FILE_REFERENCE + ".sam > " + FILE_REFERENCE + ".bam";
    private static final String SAMTOOLS_INDEX = "samtools index " + FILE_REFERENCE + ".sorted.bam";
    private static final String SAMTOOLS_SORT = "samtools sort " + FILE_REFERENCE + ".bam > " + FILE_REFERENCE + ".sorted.bam";

    public static String getInputEnvironmentDirPath(Environment environment) {
        String output = environment.getDataset().getPath();
        String separator = "";
        if (!output.endsWith(File.separator)) {
            separator = File.separator;
        }
        return output + separator;
    }

    public static String getInputFastqDirPath(Environment environment) {
        return getInputEnvironmentDirPath(environment) + "fastq/";
    }

    public static String getInputVCFDirPath(Environment environment) {
        return getInputEnvironmentDirPath(environment) + "vcf/";
    }

    public static String getInputBamDirPath(Environment environment) {
        return getInputEnvironmentDirPath(environment) + "bam/";
    }

    public static String getInputTemplatesDirPath(Environment environment) {
        return getInputEnvironmentDirPath(environment) + "templates/";

    }

    public static String getEnvironmentDirPath(Environment environment) {

        return getInputEnvironmentDirPath(environment) + environment.getId() + "/";
    }

    public static String getEnvironmentOutputDirPath(Environment environment) {
        return getEnvironmentDirPath(environment) + "output/";
    }

    public static String getOutputBamDirPath(Environment environment) {
        return getEnvironmentOutputDirPath(environment) + "bam/";
    }

    public static String getOutputTemplatesDirPath(Environment environment) {
        return getEnvironmentOutputDirPath(environment) + "templates/";
    }

    public static String getVCFOutputDirPath(Environment environment) {
        return getEnvironmentOutputDirPath(environment) + "vcf/";
    }

    public static String getExecutionDirPath(Environment environment) {
        return DatasetTestUtils.getEnvironmentDirPath(environment) + "execution/";
    }

    public static List<String> getSamtoolsCommands(String filename) {
        List<String> res = new ArrayList<>();
        res.add(SAMTOOLS_BAM.replace(FILE_REFERENCE, filename));
        res.add(SAMTOOLS_SORT.replace(FILE_REFERENCE, filename));
        res.add(SAMTOOLS_INDEX.replace(FILE_REFERENCE, filename));
        return res;
    }

    public static boolean areSkippedAllCallers(Environment env) {
        return areSkippedAllCallers(env.getCallers());
    }

    public static boolean areSkippedAllCallers(List<Caller> callers) {
        if (CollectionUtils.isEmpty(callers)) {
            return true;
        }
        for (Caller caller : callers) {
            if (!caller.isSkip()) {
                return false;
            }
        }
        return true;
    }

    public static boolean areSkippedAllExecutionPlans(List<DatasetExecutionPlan> datasetPlanExecutionList) {
        if (CollectionUtils.isEmpty(datasetPlanExecutionList)) {
            return true;
        }
        for (DatasetExecutionPlan datasetExecutionPlan : datasetPlanExecutionList) {
            if (!areSkippedAllCallers(datasetExecutionPlan.getEnvironment())) {
                return false;
            }
        }
        return false;
    }
}
