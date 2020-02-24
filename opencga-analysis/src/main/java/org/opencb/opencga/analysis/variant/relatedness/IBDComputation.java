package org.opencb.opencga.analysis.variant.relatedness;

import org.apache.commons.io.FileUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.PlinkWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.*;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.TPED;

public class IBDComputation {

    public static File compute(String study, List<String> samples, Path outDir, VariantStorageManager storageManager, String token)
            throws ToolException {
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                "/data/output");

        // Apply filter and export variants in format .tped and .tfam to run plink
        Query query = new Query()
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.SAMPLE.key(), samples)
                .append(VariantQueryParam.TYPE.key(), "SNV");
        //.append(VariantQueryParam.FILTER.key(), "PASS")

        // First, autosomal chromosomes
        File tpedAutosomeFile = outDir.resolve("autosome.tped").toFile();
        File tfamAutosomeFile = outDir.resolve("autosome.tfam").toFile();
        query.put(VariantQueryParam.REGION.key(), Arrays.asList("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22".split(",")));
        query.put(VariantQueryParam.STATS_MAF.key(), "ALL>0.3");
        exportData(tpedAutosomeFile, tfamAutosomeFile, query, storageManager, token);
        if (tpedAutosomeFile.exists() && tpedAutosomeFile.length() > 0) {
            pruneVariants("autosome", outputBinding);
        }

        // First, X chromosome
        File tpedXFile = outDir.resolve("x.tped").toFile();
        File tfamXFile = outDir.resolve("x.tfam").toFile();
        query.put(VariantQueryParam.REGION.key(), "X");
        query.put(VariantQueryParam.STATS_MAF.key(), "ALL>0.05");
        exportData(tpedXFile, tfamXFile, query, storageManager, token);
        if (tpedXFile.exists() && tpedXFile.length() > 0) {
            pruneVariants("x", outputBinding);
        }

        // Append files:
        //   - the x.tped file to autosome.tped file (since tfam files contain the same sample information)
        //   - the x.prune.out file to autosome.prune.out file
        appendFile(tpedXFile.getAbsolutePath(), tpedAutosomeFile.getAbsolutePath());
        appendFile(outDir.resolve("x.prune.out").toString(), outDir.resolve("autosome.prune.out").toString());

        // runIBD and return the result file (now autosome-file comprises X chromosome too)
        return runIBD("autosome", outputBinding);
    }


    private static void exportData(File tpedFile, File tfamFile, Query query, VariantStorageManager storageManager,
                                   String token) throws ToolException {
        try {
            storageManager.exportData(tpedFile.getAbsolutePath(), TPED, null, query, QueryOptions.empty(), token);
        } catch(CatalogException |IOException | StorageEngineException e) {
            throw new ToolException(e);
        }

        if (!tpedFile.exists() || !tfamFile.exists()) {
            throw new ToolException("Something wrong exporting data to TPED/TFAM format");
        }
    }

    private static void pruneVariants(String basename, AbstractMap.SimpleEntry<String, String> outputBinding) throws ToolException {
        // Variant pruning using PLINK in docker
        String plinkParams = "plink --tfile /data/output/" + basename + " --indep 50 5 2 --out /data/output/" + basename;
        try {
            DockerUtils.run(PlinkWrapperAnalysis.PLINK_DOCKER_IMAGE, null, outputBinding, plinkParams, null);
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    private static void appendFile(String srcFilename, String destFilename) throws ToolException {
        if (new File(srcFilename).exists()) {
            try (FileWriter f = new FileWriter(destFilename, true);
                 PrintWriter p = new PrintWriter(new BufferedWriter(f))) {
                FileInputStream fis = new FileInputStream(srcFilename);
                Scanner sc = new Scanner(fis);
                while (sc.hasNextLine()) {
                    p.println(sc.nextLine());
                }
            } catch (IOException e) {
                throw new ToolException(e);
            }
        }
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

    private static File runIBD(String basename, AbstractMap.SimpleEntry<String, String> outputBinding) throws ToolException {
        String outFilename = "relatedness";

        // Calculate allele frequencies to pass to PLINK
        // --read-freq <.freq filename>
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
            String cmdline = DockerUtils.run(PlinkWrapperAnalysis.PLINK_DOCKER_IMAGE, null, outputBinding, plinkParams, null);
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
}
