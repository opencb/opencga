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

import org.junit.Test;
import org.opencb.biodata.models.clinical.qc.MendelianErrorReport;
import org.opencb.biodata.models.clinical.qc.RelatednessReport;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.IssueEntry;
import org.opencb.biodata.models.variant.avro.IssueType;
import org.opencb.opencga.analysis.family.qc.IBDComputation;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

public class IndividualQcUtilsTest {

    @Test
    public void buildRelatednessReport() throws ToolException, IOException {

        URI resourceUri = getResourceUri("ibd.genome");
        File file = Paths.get(resourceUri.getPath()).toFile();
        List<RelatednessReport.RelatednessScore> relatednessReport = IBDComputation.parseRelatednessScores(file);

        System.out.println(JacksonUtils.getDefaultNonNullObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(relatednessReport));
    }

    @Test
    public void parseMendelianError() throws IOException {
        URI resourceUri = getResourceUri("mendelian.error.variants.json");
        File file = Paths.get(resourceUri.getPath()).toFile();

        List<Variant> variants = Arrays.asList(JacksonUtils.getDefaultNonNullObjectMapper().readValue(file, Variant[].class));
        System.out.println(variants.size());

        MendelianErrorReport mendelianErrorReport = buildMendelianErrorReport(variants.iterator(), variants.size());
        System.out.println(JacksonUtils.getDefaultNonNullObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(mendelianErrorReport));

//        List<Variant> variants = JacksonUtils.getDefaultNonNullObjectMapper().readerFor(Variant.class).readValue(path.toFile());
//        System.out.println(variants.size());
    }

    private MendelianErrorReport buildMendelianErrorReport(Iterator iterator, long numVariants) {
        // Create auxiliary map
        //   sample      chrom      error    count
        Map<String, Map<String, Map<String, Integer>>> counter = new HashMap<>();
        int numErrors = 0;
        while (iterator.hasNext()) {
            Variant variant = (Variant) iterator.next();

            // Get sampleId and error code from variant issues
            boolean foundError = false;
            for (IssueEntry issue : variant.getStudies().get(0).getIssues()) {
                if (IssueType.MENDELIAN_ERROR == issue.getType() || IssueType.DE_NOVO == issue.getType()) {
                    foundError = true;

                    String sampleId = issue.getSample().getSampleId();
                    String errorCode = issue.getSample().getData().get(0);
                    if (!counter.containsKey(sampleId)) {
                        counter.put(sampleId, new HashMap<>());
                    }
                    if (!counter.get(sampleId).containsKey(variant.getChromosome())) {
                        counter.get(sampleId).put(variant.getChromosome(), new HashMap<>());
                    }
                    int val = 0;
                    if (counter.get(sampleId).get(variant.getChromosome()).containsKey(errorCode)) {
                        val = counter.get(sampleId).get(variant.getChromosome()).get(errorCode);
                    }
                    counter.get(sampleId).get(variant.getChromosome()).put(errorCode, val + 1);
                }
            }
            if (foundError) {
                numErrors++;
            }
        }

        // Create mendelian error report from auxiliary map
        MendelianErrorReport meReport = new MendelianErrorReport();
        meReport.setNumErrors(numErrors);
        for (String sampleId : counter.keySet()) {
            MendelianErrorReport.SampleAggregation sampleAgg = new MendelianErrorReport.SampleAggregation();
            int numSampleErrors = 0;
            for (String chrom : counter.get(sampleId).keySet()) {
                int numChromErrors = counter.get(sampleId).get(chrom).values().stream().mapToInt(Integer::intValue).sum();

                MendelianErrorReport.SampleAggregation.ChromosomeAggregation chromAgg = new MendelianErrorReport.SampleAggregation.ChromosomeAggregation();
                chromAgg.setChromosome(chrom);
                chromAgg.setNumErrors(numChromErrors);
                chromAgg.setErrorCodeAggregation(counter.get(sampleId).get(chrom));

                // Update sample aggregation
                sampleAgg.getChromAggregation().add(chromAgg);
                numSampleErrors += numChromErrors;
            }
            sampleAgg.setSample(sampleId);
            sampleAgg.setNumErrors(numSampleErrors);
            sampleAgg.setRatio(1.0d * numSampleErrors / numVariants);

            meReport.getSampleAggregation().add(sampleAgg);
        }

        return meReport;
    }
}