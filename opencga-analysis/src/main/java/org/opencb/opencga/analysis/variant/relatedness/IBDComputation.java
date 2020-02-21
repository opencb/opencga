package org.opencb.opencga.analysis.variant.relatedness;

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
        String filename = "variants";
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                "/data/output");

        // Apply filter and export variants in format .tped and .tfam to run plink
        Query query = new Query()
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.SAMPLE.key(), samples)
                .append(VariantQueryParam.TYPE.key(), "SNV");
        //.append(VariantQueryParam.FILTER.key(), "PASS")

        // First, autosomal chromosomes
        File tpedAutosomeFile = outDir.resolve(filename + ".autosome.tped").toFile();
        File tfamAutosomeFile = outDir.resolve(filename + ".autosome.tfam").toFile();
        query.put(VariantQueryParam.REGION.key(), Arrays.asList(new String[]{"22"}));
        //query.put(VariantQueryParam.STATS_MAF.key(), "ALL>0.3");
        exportData(tpedAutosomeFile, tfamAutosomeFile, query, storageManager, token);
        pruneVariants(filename + ".autosome", outputBinding);

        // First, X chromosome
        File tpedXFile = outDir.resolve(filename + ".x.tped").toFile();
        File tfamXFile = outDir.resolve(filename + ".x.tfam").toFile();
        query.put(VariantQueryParam.REGION.key(), "X");
        //query.put(VariantQueryParam.STATS_MAF.key(), "ALL>0.05");
        exportData(tpedXFile, tfamXFile, query, storageManager, token);
        pruneVariants(filename + ".x", outputBinding);

        // For performance purposes, append the X tped file to autosome tped file
        // (since tfam files contain sample information, X tfam file is equal to autosome tfam file)
        appendFile(tpedAutosomeFile, tpedXFile);

        // runIBD and return the result file
        return runIBD(filename, outputBinding);
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
        String plinkParams = "--noweb -tfile /data/output/" + basename + " --indep 50 5 2 --not-chr 24";
        try {
            String cmdline = DockerUtils.run(PlinkWrapperAnalysis.PLINK_DOCKER_IMAGE, null, outputBinding, plinkParams, null);
            System.out.println("Docker command line: " + cmdline);
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    private static void appendFile(File file, File filetoAppend) throws ToolException {
        try (FileWriter f = new FileWriter(file.getAbsolutePath(), true);
             PrintWriter p = new PrintWriter(new BufferedWriter(f))) {
            FileInputStream fis = new FileInputStream(filetoAppend.getAbsolutePath());
            Scanner sc = new Scanner(fis);
            while(sc.hasNextLine()) {
                p.println(sc.nextLine());
            }
        } catch (IOException e) {
            throw new ToolException(e);
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
                int a1 = 0;
                int a2 = 0;
                for (int i = 4; i < splits.length; i++) {
                    if (splits[i].equals("0")) {
                        a1++;
                    } else {
                        a2++;
                    }
                }
                String[] varSplits = splits[1].split(":");
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
        String mafFilename = "maf.freq";
        calculateMAF(outputBinding.getKey() + "/" + basename + ".tped", outputBinding.getKey() + "/" + mafFilename);

        // Run IBD using PLINK in docker
        String plinkParams = "--noweb -tfile /data/output/" + basename + " --genome rel-check --read-freq /data/output/" + mafFilename
                + " --out /data/output/" + outFilename;
        try {
            String cmdline = DockerUtils.run(PlinkWrapperAnalysis.PLINK_DOCKER_IMAGE, null, outputBinding, plinkParams, null);
            System.out.println("Docker command line: " + cmdline);
        } catch (IOException e) {
            throw new ToolException(e);
        }

        // Check output file
        File outFile = new File(outputBinding.getKey() + "/" + basename + ".genome");
        if (!outFile.exists()) {
            throw new ToolException("Something wrong executing relatedness analysis (i.e., IBD/IBS computation) in PLINK docker.");
        }

        return outFile;
    }
}
