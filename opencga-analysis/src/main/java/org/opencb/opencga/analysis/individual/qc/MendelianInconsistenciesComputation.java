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

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.MendelianErrorReport;
import org.opencb.biodata.models.clinical.qc.MendelianErrorReport.SampleAggregation;
import org.opencb.biodata.models.clinical.qc.MendelianErrorReport.SampleAggregation.ChromosomeAggregation;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.IssueEntry;
import org.opencb.biodata.models.variant.avro.IssueType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MendelianInconsistenciesComputation {

    public static MendelianErrorReport compute(String studyId, String childId, String motherId, String fatherId,
                                               VariantStorageManager storageManager, String token) throws ToolException {
        List<String> sampleIds = new ArrayList<>();

        // Sanity check
        if (StringUtils.isEmpty(childId)) {
            throw new ToolException("Missing child sample ID.");
        }
        sampleIds.add(childId);
        if (StringUtils.isNotEmpty(motherId)) {
            sampleIds.add(motherId);
        }
        if (StringUtils.isNotEmpty(fatherId)) {
            sampleIds.add(fatherId);
        }
        if (sampleIds.size() == 1) {
            throw new ToolException("Invalid parameters: both mother and father sample IDs are empty but in order to compute mendelian"
                    + " errors at least one of them has to be not empty.");
        }

        // Query to retrive mendelian error variants from childId, motherId, fatherId
        Query query = new Query();
        query.put(VariantQueryParam.STUDY.key(), studyId);
        query.put(VariantQueryParam.SAMPLE.key(), childId + ":MendelianError");
        query.put("includeSample", StringUtils.join(sampleIds, ","));

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.EXCLUDE, "annotation");

        System.out.println("---> Query = " + query.toJson());
        System.out.println("---> QueryOptions = " + queryOptions.toJson());

        VariantDBIterator iterator;
        try {
            iterator = storageManager.iterator(query, queryOptions, token);
        } catch (CatalogException | StorageEngineException e) {
            throw new ToolException(e);
        }

        return buildMendelianErrorReport(iterator, getTotalVariants(studyId, childId, storageManager, token));
    }


    @Deprecated
    public static MendelianErrorReport compute(String studyId, String familyId, VariantStorageManager storageManager,
                                               String token) throws ToolException {
        // Create query to count the total number of variants
        Query query = new Query();
        query.put(VariantQueryParam.STUDY.key(), studyId);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.EXCLUDE, "annotation");

        // Get total number of variants
        long numVariants;
        try {
            numVariants = storageManager.count(query, token).first();
        } catch (CatalogException | StorageEngineException | IOException e) {
            throw new ToolException(e);
        }

        // Update quey to retrive mendelian error variants
        query.put(VariantCatalogQueryUtils.FAMILY.key(), familyId);
        query.put(VariantCatalogQueryUtils.FAMILY_SEGREGATION.key(), "MendelianError");

        // Create auxiliary map
        //   sample      chrom      error    count
        Map<String, Map<String, Map<String, Integer>>> counter = new HashMap<>();
        int numErrors = 0;
        try {
            VariantDBIterator iterator = storageManager.iterator(query, queryOptions, token);
            while (iterator.hasNext()) {
                Variant variant = iterator.next();

                // Get sampleId and error code from variant issues
                boolean foundError = false;
                for (IssueEntry issue : variant.getStudies().get(0).getIssues()) {
                    if ("MENDELIAN_ERROR".equals(issue.getType()) || "DE_NOVO".equals(issue.getType())) {
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
        } catch (CatalogException | StorageEngineException e) {
            throw new ToolException(e);
        }

        // Create mendelian error report from auxiliary map
        MendelianErrorReport meReport = new MendelianErrorReport();
        meReport.setNumErrors(numErrors);
        for (String sampleId : counter.keySet()) {
            SampleAggregation sampleAgg = new SampleAggregation();
            int numSampleErrors = 0;
            for (String chrom : counter.get(sampleId).keySet()) {
                int numChromErrors = counter.get(sampleId).get(chrom).values().stream().mapToInt(Integer::intValue).sum();

                ChromosomeAggregation chromAgg = new ChromosomeAggregation();
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

    private static long getTotalVariants(String studyId, String sampleId, VariantStorageManager storageManager, String token) throws ToolException {
        // Create query to count the total number of variants
        Query query = new Query()
                .append(VariantQueryParam.STUDY.key(), studyId)
                .append(VariantQueryParam.SAMPLE.key(), sampleId);

        // Get total number of variants
        long numVariants;
        try {
            numVariants = storageManager.count(query, token).first();
        } catch (CatalogException | StorageEngineException | IOException e) {
            throw new ToolException(e);
        }
        return numVariants;
    }

    private static MendelianErrorReport buildMendelianErrorReport(VariantDBIterator iterator, long numVariants) {
        // Create auxiliary map
        //   sample      chrom      error    count
        Map<String, Map<String, Map<String, Integer>>> counter = new HashMap<>();
        int numErrors = 0;
        while (iterator.hasNext()) {
            Variant variant = iterator.next();

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
                    break;
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
            SampleAggregation sampleAgg = new SampleAggregation();
            int numSampleErrors = 0;
            for (String chrom : counter.get(sampleId).keySet()) {
                int numChromErrors = counter.get(sampleId).get(chrom).values().stream().mapToInt(Integer::intValue).sum();

                ChromosomeAggregation chromAgg = new ChromosomeAggregation();
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
