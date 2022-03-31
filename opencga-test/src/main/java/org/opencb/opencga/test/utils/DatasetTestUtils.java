package org.opencb.opencga.test.utils;

import org.opencb.opencga.test.config.Environment;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DatasetTestUtils {


    private static final String FILE_REFERENCE = "{INPUT_FILE}";
    private static final String SAMTOOLS_BAM = "samtools view -bS " + FILE_REFERENCE + ".sam > " + FILE_REFERENCE + ".bam";
    private static final String SAMTOOLS_INDEX = "samtools index " + FILE_REFERENCE + ".sorted.bam";
    private static final String SAMTOOLS_SORT = "samtools view -bS " + FILE_REFERENCE + ".sam > {FILE_OUTPUT}.bam";


    public static String getEnvironmentOutputDir(Environment environment) {

        String output = environment.getDataset().getPath();
        String separator = "";
        if (!output.endsWith(File.separator)) {
            separator = File.separator;
        }
        return output + separator + environment.getId() + "/";

    }


    public static List<String> getSamtoolsCommands(String filename) {

        List<String> res = new ArrayList<>();
        res.add(SAMTOOLS_BAM.replace(FILE_REFERENCE, filename));
        res.add(SAMTOOLS_SORT.replace(FILE_REFERENCE, filename));
        res.add(SAMTOOLS_INDEX.replace(FILE_REFERENCE, filename));
        return res;
    }
}
