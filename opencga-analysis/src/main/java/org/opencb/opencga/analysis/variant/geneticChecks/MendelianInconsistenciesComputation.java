package org.opencb.opencga.analysis.variant.geneticChecks;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.IssueEntry;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.variant.MendelianErrorReport;
import org.opencb.opencga.core.models.variant.MendelianErrorReport.SampleAggregation;
import org.opencb.opencga.core.models.variant.MendelianErrorReport.SampleAggregation.ChromosomeAggregation;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MendelianInconsistenciesComputation {

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
}
