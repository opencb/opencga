package org.opencb.opencga.analysis.variant.mutationalSignature;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.biodata.models.clinical.qc.SignatureFitting;
import org.opencb.biodata.models.clinical.qc.SignatureFittingScore;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.variant.manager.CatalogUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class MutationalSignatureAnalysisUtils {

    public static String getContextIndexFilename(String sample, String assembly) {
        return "OPENCGA_" + sample + "_" + assembly + "_genome_context.csv";
    }

    public static String getAssembly(String studyId, CatalogManager catalogManager, String token) throws CatalogException, ToolException {
        CatalogUtils catalogUtils = new CatalogUtils(catalogManager);
        String assembly = catalogUtils.getAssembly(studyId, token);

       if (StringUtils.isEmpty(assembly)) {
            throw new ToolException("Missing assembly for study '" + studyId + "'");
        }

        // TODO: improve this
        switch (assembly.toUpperCase()) {
            case "GRCH37":
                assembly = "GRCh37";
                break;
            case "GRCH38":
                assembly = "GRCh38";
                break;
            default:
                break;
        }
        return assembly;
    }

    public static File getGenomeContextFile(String sample, String studyId, CatalogManager catalogManager, String token)
            throws CatalogException, ToolException {
        File indexFile = null;
        String assembly = getAssembly(studyId, catalogManager, token);
        String indexFilename = getContextIndexFilename(sample, assembly);
        try {
            Query fileQuery = new Query("name", indexFilename);
            QueryOptions fileQueryOptions = new QueryOptions("include", "uri");
            OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult = catalogManager.getFileManager().search(studyId, fileQuery,
                    fileQueryOptions, token);

            long maxSize = 0;
            for (org.opencb.opencga.core.models.file.File file : fileResult.getResults()) {
                File auxFile = new File(file.getUri().getPath());
                if (auxFile.exists() && auxFile.length() > maxSize) {
                    maxSize = auxFile.length();
                    indexFile = auxFile;
                }
            }
        } catch (CatalogException e) {
            throw new ToolExecutorException(e);
        }

        return indexFile;
    }

    public static List<Signature.GenomeContextCount> parseCatalogueResults(Path dir) throws IOException {
        List<Signature.GenomeContextCount> sigCounts = null;

        // Context counts
        File contextFile = dir.resolve(MutationalSignatureAnalysis.CATALOGUES_FILENAME_DEFAULT).toFile();
        if (contextFile.exists()) {
            List<String> lines = FileUtils.readLines(contextFile, Charset.defaultCharset());
            sigCounts = new ArrayList<>(lines.size() - 1);
            for (int i = 1; i < lines.size(); i++) {
                String[] fields = lines.get(i).split("\t");
                sigCounts.add(new Signature.GenomeContextCount(fields[0], Math.round(Float.parseFloat((fields[1])))));
            }
        }

        return sigCounts;
    }

    public static SignatureFitting parseFittingResults(Path outDir, String fitId, String fitMethod, String fitSigVersion, Integer fitNBoot,
                                                       String fitOrgan, Float fitThresholdPerc, Float fitThresholdPval,
                                                       Integer fitMaxRareSigs) throws IOException {
        // Check for fitting coeffs. file
        File coeffsFile = outDir.resolve(MutationalSignatureAnalysis.SIGNATURE_COEFFS_FILENAME).toFile();
        if (!coeffsFile.exists()) {
            return null;
        }

        // Signature fitting
        SignatureFitting fitting = new SignatureFitting();
        if (StringUtils.isNotEmpty(fitId)) {
            fitting.setId(fitId);
        }
        if (StringUtils.isNotEmpty(fitMethod)) {
            fitting.setMethod(fitMethod);
        }
        if (StringUtils.isNotEmpty(fitSigVersion)) {
            fitting.setSignatureVersion(fitSigVersion);
            if (fitSigVersion.startsWith("COSMIC")) {
                fitting.setSignatureSource("COSMIC");
            } else if (fitSigVersion.startsWith("RefSig")) {
                fitting.setSignatureSource("RefSig");
            }
        }

        // Set fitting scores
        List<String> lines = FileUtils.readLines(coeffsFile, Charset.defaultCharset());
        String[] labels = lines.get(0).split("\t");
        String[] values = lines.get(1).split("\t");
        List<SignatureFittingScore> scores = new ArrayList<>(labels.length);
        for (int i = 0; i < labels.length; i++) {
            String label = labels[i];
            if (label.contains("_")) {
                String[] splits = label.split("_");
                label = splits[splits.length - 1];
            }
            scores.add(new SignatureFittingScore(label, Double.parseDouble(values[i + 1])));
        }
        fitting.setScores(scores);

        // Set files
        List<String> files = new ArrayList<>();
        for (File file : outDir.toFile().listFiles()) {
            if (file.getName().equals("catalogues.pdf")) {
                continue;
            }
            if (file.getName().endsWith("pdf") || file.getName().equals("fitData.rData")) {
                files.add(AnalysisUtils.getJobFileRelativePath(file.getAbsolutePath()));
            } else if (file.isDirectory()) {
                for (File file2 : file.listFiles()) {
                    if (file2.getName().endsWith("pdf")) {
                        files.add(AnalysisUtils.getJobFileRelativePath(file2.getAbsolutePath()));
                    }
                }
            }
        }
        fitting.setFiles(files);

        // Set params
        ObjectMap params = new ObjectMap();
        if (fitNBoot != null) {
            params.append("nBoot", fitNBoot);
        }
        if (StringUtils.isNotEmpty(fitOrgan)) {
            params.append("organ", fitOrgan);
        }
        if (fitThresholdPerc != null) {
            params.append("thresholdPerc", fitThresholdPerc);
        }
        if (fitThresholdPval != null) {
            params.append("thresholdPval", fitThresholdPval);
        }
        if (fitMaxRareSigs != null) {
            params.append("maxRareSigs", fitMaxRareSigs);
        }
        if (params.size() > 0) {
            fitting.setParams(params);
        }
        fitting.setParams(params);

        return fitting;
    }
}
