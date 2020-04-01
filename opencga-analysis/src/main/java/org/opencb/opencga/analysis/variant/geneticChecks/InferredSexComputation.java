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

package org.opencb.opencga.analysis.variant.geneticChecks;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Chromosome;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureLocalAnalysisExecutor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor.GRCH37_CHROMOSOMES;
import static org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor.GRCH38_CHROMOSOMES;

public class InferredSexComputation {

    public static double[] computeRatios(String study, String sample, String assembly, FileManager fileManager,
                                         AlignmentStorageManager alignmentStorageManager, String token)
            throws ToolException {

        // Look for the bam file for each sample
        OpenCGAResult<File> fileQueryResult;

        Query query = new Query(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UUID.key());

        query.put(FileDBAdaptor.QueryParams.SAMPLES.key(), sample);
        try {
            fileQueryResult = fileManager.search(study, query, queryOptions, token);
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
            Region region = new Region(chrom, 1, chromSize - 1);
            try {
                List<RegionCoverage> regionCoverages = alignmentStorageManager.coverageQuery(study, fileQueryResult.first().getUuid(),
                        region, 0, 100, chromSize, token).getResults();
                double meanCoverage = 0d;
                for (RegionCoverage regionCoverage : regionCoverages) {
                    meanCoverage += regionCoverage.meanCoverage();
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
        means[0] /= (chromosomes.size() - 2);

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
}
