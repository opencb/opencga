package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

/**
 * Created on 12/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class SampleIndexQuery {
    private final List<Region> regions;
    private final String study;
    private final Map<String, List<String>> samplesMap;
    private final VariantQueryUtils.QueryOperation queryOperation;

    private SampleIndexQuery(List<Region> regions, String study,
                             Map<String, List<String>> samplesMap,
                             VariantQueryUtils.QueryOperation queryOperation) {
        this.regions = regions;
        this.study = study;
        this.samplesMap = samplesMap;
        this.queryOperation = queryOperation;
    }

    /**
     * Determine if a given query can be used to query with the SampleIndex.
     * @param query Query
     * @return      if the query is valid
     */
    public static boolean validSampleIndexQuery(Query query) {
        VariantQueryUtils.VariantQueryXref xref = VariantQueryUtils.parseXrefs(query);
        if (!xref.getIds().isEmpty() || !xref.getVariants().isEmpty() || !xref.getOtherXrefs().isEmpty()) {
            // Can not be used for specific variant IDs. Only regions and genes
            return false;
        }

        if (isValidParam(query, GENOTYPE)) {
            HashMap<Object, List<String>> gtMap = new HashMap<>();
            VariantQueryUtils.parseGenotypeFilter(query.getString(GENOTYPE.key()), gtMap);
            for (List<String> gts : gtMap.values()) {
                boolean valid = true;
                for (String gt : gts) {
                    // Despite invalid genotypes (i.e. genotypes not in the index) can be used to filter within AND queries,
                    // we require at least one sample where all the genotypes are valid
                    valid &= SampleIndexDBLoader.validGenotype(gt);
                    valid &= !isNegated(gt);
                }
                if (valid) {
                    // If any sample is valid, go for it
                    return true;
                }
            }
        }
        if (isValidParam(query, SAMPLE, true)) {
            return true;
        }
        return false;
    }

    /**
     * Build SampleIndexQuery. Extract Regions (+genes), Study, Sample and Genotypes.
     *
     * Assumes that the query is valid.
     *
     * @param query         Input query. Will be modified.
     * @param scm           StudyConfigurationManager
     * @return              Valid SampleIndexQuery
     * @see                 SampleIndexQuery#validSampleIndexQuery(Query)
     */
    public static SampleIndexQuery extractSampleIndexQuery(Query query, StudyConfigurationManager scm) {
        //
        // Extract regions
        List<Region> regions = new ArrayList<>();
        if (isValidParam(query, REGION)) {
            regions = Region.parseRegions(query.getString(REGION.key()));
            query.remove(REGION.key());
        }

        if (isValidParam(query, ANNOT_GENE_REGIONS)) {
            regions = Region.parseRegions(query.getString(ANNOT_GENE_REGIONS.key()));
            query.remove(ANNOT_GENE_REGIONS.key());
        }

        regions = mergeRegions(regions);

        // TODO: Accept variant IDs?

        // Extract study
        StudyConfiguration defaultStudyConfiguration = VariantQueryUtils.getDefaultStudyConfiguration(query, null, scm);

        // Extract sample and genotypes to filter
        VariantQueryUtils.QueryOperation queryOperation;
        Map<String, List<String>> samplesMap = new HashMap<>();
        if (isValidParam(query, GENOTYPE)) {
            // Get samples with non negated genotypes

            HashMap<Object, List<String>> map = new HashMap<>();
            queryOperation = parseGenotypeFilter(query.getString(GENOTYPE.key()), map);

            for (Map.Entry<Object, List<String>> entry : map.entrySet()) {
                boolean valid = true;
                for (String gt : entry.getValue()) {
                    if (queryOperation == VariantQueryUtils.QueryOperation.OR && !SampleIndexDBLoader.validGenotype(gt)) {
                        // Invalid genotypes (i.e. genotypes not in the index) are not allowed in OR queries
                        throw new IllegalStateException("Genotype '" + gt + "' not in the SampleIndex.");
                    }
                    valid &= !isNegated(gt);
                }
                if (valid) {
                    samplesMap.put(entry.getKey().toString(), entry.getValue());
                }
            }

        } else if (isValidParam(query, SAMPLE)) {
            // Filter by all non negated samples
            queryOperation = VariantQueryUtils.QueryOperation.AND;
            List<String> samples = query.getAsStringList(SAMPLE.key());
            samples.stream().filter(s -> !isNegated(s)).forEach(sample -> samplesMap.put(sample, Collections.emptyList()));
        //} else if (isValidParam(query, FILE)) {
            // TODO: Add FILEs filter
        } else {
            throw new IllegalStateException("Unable to query SamplesIndex");
        }

        if (defaultStudyConfiguration == null) {
            String sample = samplesMap.keySet().iterator().next();
            throw VariantQueryException.missingStudyForSample(sample, scm.getStudyNames(null));
        }
        String study = defaultStudyConfiguration.getStudyName();

        return new SampleIndexQuery(regions, study, samplesMap, queryOperation);
    }

    public List<Region> getRegions() {
        return regions;
    }

    public String getStudy() {
        return study;
    }

    public Map<String, List<String>> getSamplesMap() {
        return samplesMap;
    }

    public VariantQueryUtils.QueryOperation getQueryOperation() {
        return queryOperation;
    }
}
