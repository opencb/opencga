package org.opencb.opencga.test.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.cli.options.DatasetCommandOptions;
import org.opencb.opencga.test.config.Caller;
import org.opencb.opencga.test.config.Environment;
import org.opencb.opencga.test.execution.models.DatasetExecutionPlan;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        return output + separator + "envs/" + environment.getId() + "/";
    }

    public static String getInputDataDirPath(Environment environment) {
        String output = environment.getData().getPath();
        String separator = "";
        if (!output.endsWith(File.separator)) {
            separator = File.separator;
        }
        return output + separator;
    }

    public static String getInputFastqDirPath(Environment environment) {
        return getInputDataDirPath(environment) + "fastq/";
    }

    public static String getInputVCFDirPath(Environment environment) {
        return getInputDataDirPath(environment) + "vcf/";
    }

    public static String getInputBamDirPath(Environment environment) {
        return getInputDataDirPath(environment) + "bam/";
    }

    public static String getInputTemplatesDirPath(Environment environment) {
        return getInputEnvironmentDirPath(environment) + "templates/";

    }

    public static String getOutputDirPath(Environment environment) {

        if (StringUtils.isNotEmpty(DatasetCommandOptions.output)) {
            String output = DatasetCommandOptions.output;
            String separator = "";
            if (!output.endsWith(File.separator)) {
                separator = File.separator;
            }
            Path path = Paths.get(output);
            if (Files.exists(path)) {
                return output + separator;
            } else {
                PrintUtils.printError("The path " + path + " is not present.");
                System.exit(0);
            }
        }

        String output = environment.getDataset().getPath();
        String separator = "";
        if (!output.endsWith(File.separator)) {
            separator = File.separator;
        }
        return output + separator + "output/";
    }

    public static String getEnvironmentOutputDirPath(Environment environment) {

        return getOutputDirPath(environment) + environment.getId() + "/";
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

    public static String getEnvironmentExecutionDirPath(Environment environment) {
        return DatasetTestUtils.getMetadataDirPath(environment) + "execution/" + environment.getId() + "/";
    }

    public static String getExecutionDirPath(Environment environment) {
        return DatasetTestUtils.getMetadataDirPath(environment) + "execution/";
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

    public static String getMetadataDirPath(Environment environment) {
        return getOutputDirPath(environment) + "metadata/";
    }
}
