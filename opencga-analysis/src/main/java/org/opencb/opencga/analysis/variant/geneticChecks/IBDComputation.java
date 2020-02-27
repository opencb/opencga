package org.opencb.opencga.analysis.variant.geneticChecks;

import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.PlinkWrapperAnalysis;
import org.opencb.opencga.core.exceptions.ToolException;

import java.io.*;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.List;
import java.util.Scanner;

public class IBDComputation {

    public static File compute(String study, List<String> samples, String population, Path outDir, VariantStorageManager storageManager, String token)
            throws ToolException {
        String basename = "variants";

        // Select markers
        if (!outDir.resolve(basename + ".tped").toFile().exists() || !outDir.resolve(basename + ".tfam").toFile().exists()) {
            GeneticChecksUtils.selectMarkers(basename, study, samples, population, outDir, storageManager, token);
        }

        // run IBD and return the result file (now autosome-file comprises X chromosome too)
        return runIBD(basename, outDir);
    }

    private static File runIBD(String basename, Path outDir) throws ToolException {
        String outFilename = "relatedness";
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                "/data/output");

        // Calculate allele frequencies to pass to PLINK: --read-freq <.freq filename>
        String readFreq = "";
        String mafFilename = "maf.freq";
        calculateMAF(outputBinding.getKey() + "/" + basename + ".tped", outputBinding.getKey() + "/" + mafFilename);
        if (new File(outputBinding.getKey() + "/" + mafFilename).exists()) {
            readFreq = " --read-freq /data/output/" + mafFilename;
        }

        // Run IBD using PLINK in docker
        String exclude = "";
        File pruneOutFile = new File(outputBinding.getKey() + "/" + basename + ".prune.out");
        if (pruneOutFile.exists() && pruneOutFile.length() > 0) {
            exclude = " --exclude /data/output/" + basename + ".prune.out";
        }
        String plinkParams = "plink --tfile /data/output/" + basename + " --genome rel-check " + readFreq + exclude + " --out /data/output/"
                + outFilename;
        try {
            DockerUtils.run(PlinkWrapperAnalysis.PLINK_DOCKER_IMAGE, null, outputBinding, plinkParams, null);
        } catch (IOException e) {
            throw new ToolException(e);
        }

        // Check output file
        File outFile = new File(outputBinding.getKey() + "/" + outFilename + ".genome");
        if (!outFile.exists()) {
            throw new ToolException("Something wrong executing relatedness analysis (i.e., IBD/IBS computation) in PLINK docker.");
        }

        return outFile;
    }

    private static void calculateMAF(String tpedFilename, String mafFilename) throws ToolException {
        try (FileWriter f = new FileWriter(mafFilename, true);
             PrintWriter p = new PrintWriter(new BufferedWriter(f))) {

            p.println("CHR\tSNP\tA1\tA2\tMAF\tNCHROBS");

            FileInputStream fis = new FileInputStream(tpedFilename);
            Scanner sc = new Scanner(fis);
            while(sc.hasNextLine()) {
                String[] splits = sc.nextLine().split("\t");
                String[] varSplits = splits[1].split(":");

                int a1 = 0;
                int a2 = 0;
                for (int i = 4; i < splits.length; i++) {
                    if (splits[i].equals(varSplits[2])) {
                        a1++;
                    } else if (splits[i].equals(varSplits[3])) {
                        a2++;
                    }
                }
                p.println(splits[0] + "\t" + splits[1] + "\t" + varSplits[2] + "\t" + varSplits[3] + "\t" + (1.0d * a2 / (a1 + a2)) + "\t"
                        + (a1 + a2));
            }
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }
}
