/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.individual.qc;

import org.opencb.biodata.models.alignment.RegionCoverage;
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
import java.util.*;

import static org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor.GRCH37_CHROMOSOMES;
import static org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor.GRCH38_CHROMOSOMES;

public class InferredSexComputation {

    public static double[] computeRatios(String study, File bamFile, String assembly, AlignmentStorageManager alignmentStorageManager,
                                         String token)
            throws ToolException {

        // Compute coverage for each chromosome for each BAM file
        // TODO get chromosomes from cellbase
        Map<String, Integer> chromosomes;
        if (assembly.toLowerCase().equals("grch37")) {
            chromosomes = GRCH37_CHROMOSOMES;
        } else {
            chromosomes = GRCH38_CHROMOSOMES;
        }

        double[] means = new double[]{0d, 0d, 0d};
        for (String chrom : chromosomes.keySet()) {
            int chromSize = chromosomes.get(chrom);
            Region region = new Region(chrom, 1, chromSize);
            try {
                List<RegionCoverage> regionCoverages = alignmentStorageManager.coverageQuery(study, bamFile.getUuid(), region, 0,
                        Integer.MAX_VALUE, chromSize, token).getResults();

                double meanCoverage = 0d;
                for (RegionCoverage regionCoverage : regionCoverages) {
                    meanCoverage += regionCoverage.getStats().getAvg();
                }
                meanCoverage /= regionCoverages.size();

                String name = chrom.toUpperCase();
                switch (name) {
                    case "Y": {
                        means[2] = meanCoverage;
                        break;
                    }
                    case "X": {
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

        means[0] /= (1.0d * (chromosomes.size() - 2));

        // Create sex report for that sample
        return new double[]{1.0d * means[1] / means[0], 1.0d * means[2] / means[0]};
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

    public static String inferKaryotypicSex(double xAuto, double yAuto, Map<String, Double> karyotypicSexThresholds) {
        String inferredKaryotypicSex = "UNKNOWN";

        if (xAuto >= karyotypicSexThresholds.get("xx.xmin") && xAuto <= karyotypicSexThresholds.get("xx.xmax")
                && yAuto >= karyotypicSexThresholds.get("xx.ymin") && yAuto <= karyotypicSexThresholds.get("xx.ymax")) {
            inferredKaryotypicSex = "XX";
        } else if (xAuto >= karyotypicSexThresholds.get("xy.xmin") && xAuto <= karyotypicSexThresholds.get("xy.xmax")
                && yAuto >= karyotypicSexThresholds.get("xy.ymin") && yAuto <= karyotypicSexThresholds.get("xy.ymax")) {
            inferredKaryotypicSex = "XY";
        } else if (xAuto >= karyotypicSexThresholds.get("xo_clearcut.xmin") && xAuto <= karyotypicSexThresholds.get("xo_clearcut.xmax")
                && yAuto >= karyotypicSexThresholds.get("xo_clearcut.ymin") && yAuto <= karyotypicSexThresholds.get("xo_clearcut" +
                ".ymax")) {
            inferredKaryotypicSex = "XO";
        } else if (xAuto >= karyotypicSexThresholds.get("xxy.xmin") && xAuto <= karyotypicSexThresholds.get("xxy.xmax")
                && yAuto >= karyotypicSexThresholds.get("xxy.ymin") && yAuto <= karyotypicSexThresholds.get("xxy.ymax")) {
            inferredKaryotypicSex = "XXY";
        } else if (xAuto >= karyotypicSexThresholds.get("xxx.xmin") && xAuto <= karyotypicSexThresholds.get("xxx.xmax")
                && yAuto >= karyotypicSexThresholds.get("xxx.ymin") && yAuto <= karyotypicSexThresholds.get("xxx.ymax")) {
            inferredKaryotypicSex = "XXX";
        } else if (xAuto >= karyotypicSexThresholds.get("xyy.xmin") && xAuto <= karyotypicSexThresholds.get("xyy.xmax")
                && yAuto >= karyotypicSexThresholds.get("xyy.ymin") && yAuto <= karyotypicSexThresholds.get("xyy.ymax")) {
            inferredKaryotypicSex = "XYY";
        } else if (xAuto >= karyotypicSexThresholds.get("xxxy.xmin") && xAuto <= karyotypicSexThresholds.get("xxxy.xmax")
                && yAuto >= karyotypicSexThresholds.get("xxxy.ymin") && yAuto <= karyotypicSexThresholds.get("xxxy.ymax")) {
            inferredKaryotypicSex = "XXXY";
        } else if (xAuto >= karyotypicSexThresholds.get("xyyy.xmin") && xAuto <= karyotypicSexThresholds.get("xyyy.xmax")
                && yAuto >= karyotypicSexThresholds.get("xyyy.ymin") && yAuto <= karyotypicSexThresholds.get("xyyy.ymax")) {
            inferredKaryotypicSex = "XYYY";
        }

        return inferredKaryotypicSex;
    }
}
