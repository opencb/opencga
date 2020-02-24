package org.opencb.opencga.analysis.variant.geneticChecks;

import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Chromosome;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureLocalAnalysisExecutor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KaryotypicSexComputation {

    public java.io.File compute(String study, List<String> samples, FileManager fileManager,
                                                                        AlignmentStorageManager alignmentStorageManager, String token)
            throws ToolException {
        Map<String, AbstractMap.SimpleEntry<Double, Double>> sampleRatioMap = new HashMap<>();
        Map<String, String> sampleBamMap = new HashMap();

        // Look for the bam file for each sample
        OpenCGAResult<File> fileQueryResult;

        Query query = new Query(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UUID.key());

        for (String sample : samples) {
            query.put(FileDBAdaptor.QueryParams.SAMPLES.key(), sample);
            try {
                fileQueryResult = fileManager.get(study, null, query, queryOptions, false, token);
            } catch (CatalogException e) {
                throw new ToolException(e);
            }

            // Sanity check
            if (fileQueryResult.getNumResults() == 0) {
                throw new ToolException("BAM file not found for sample " + sample);
            }
            if (fileQueryResult.getNumResults() > 1) {
                throw new ToolException("Found more than one BAM files (" + fileQueryResult.getNumResults() + ") for sample " + sample);
            }

            sampleBamMap.put(sample, fileQueryResult.first().getUuid());
        }

        // Compute coverage for each chromosome for each BAM file
        // FIX: get chromosomes from cellbase
        List<Chromosome> chromosomes = null;
        for (String sample : sampleBamMap.keySet()) {
            int[] means = new int[]{0, 0, 0};
            for (Chromosome chrom : chromosomes) {
                int windowSize = chrom.getSize() / 10000;
                Region region = new Region(chrom.getName(), chrom.getStart(), chrom.getEnd());
                try {
                    List<RegionCoverage> regionCoverages = alignmentStorageManager.coverageQuery(study, sampleBamMap.get(sample), region,
                            0, 100, windowSize, token).getResults();
                    int meanCoverage = 0;
                    for (RegionCoverage regionCoverage : regionCoverages) {
                        meanCoverage += regionCoverage.meanCoverage();
                    }
                    meanCoverage /= regionCoverages.size();

                    String name = chrom.getName().toLowerCase();
                    switch (name) {
                        case "y": {
                            means[2] = meanCoverage;
                            break;
                        }
                        case "x": {
                            means[1] = meanCoverage;
                            break;
                        }
                        default: {
                            means[0] += meanCoverage;
                            break;
                        }
                    }
                } catch (Exception e) {
                    throw new ToolException(e);
                }
            }
            means[0] /= 22;
            sampleRatioMap.put(sample, new AbstractMap.SimpleEntry<>(1.0d * means[1] / means[0], 1.0d * means[2] / means[0]));
        }

        // Write results
        // sampleRatioMap;
        java.io.File outputFile = null;
        return outputFile;
    }

    public static java.io.File plot(File inputFile, Path outDir) throws ToolException {
        // Execute R script in docker
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                "/data/output");
        String rParams = "R CMD Rscript --vanilla /data/input/" + inputFile.getName();
        try {
            String cmdline = DockerUtils.run(MutationalSignatureLocalAnalysisExecutor.R_DOCKER_IMAGE, null, outputBinding, rParams, null);
            System.out.println("Docker command line: " + cmdline);
        } catch (IOException e) {
            throw new ToolException(e);
        }

        // Check output file
        java.io.File outFile = new java.io.File(outDir + inputFile.getName() + ".png");
        if (!outFile.exists()) {
            throw new ToolException("Something wrong plotting the sex diagram.");
        }

        // return output file
        return outFile;
    }
}
