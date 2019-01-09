package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter.*;

/**
 * Created by jacobo on 06/01/19.
 */
public class SampleIndexQueryParser {

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
     * @see                 SampleIndexQueryParser#validSampleIndexQuery(Query)
     */
    public static SampleIndexQuery parseSampleIndexQuery(Query query, StudyConfigurationManager scm) {
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
        QueryOperation queryOperation;
        Map<String, List<String>> samplesMap = new HashMap<>();
        if (isValidParam(query, GENOTYPE)) {
            // Get samples with non negated genotypes

            HashMap<Object, List<String>> map = new HashMap<>();
            queryOperation = parseGenotypeFilter(query.getString(GENOTYPE.key()), map);

            for (Map.Entry<Object, List<String>> entry : map.entrySet()) {
                boolean valid = true;
                for (String gt : entry.getValue()) {
                    if (queryOperation == QueryOperation.OR && !SampleIndexDBLoader.validGenotype(gt)) {
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
            String samplesStr = query.getString(SAMPLE.key());
            queryOperation = VariantQueryUtils.checkOperator(samplesStr);
            List<String> samples = VariantQueryUtils.splitValue(samplesStr, queryOperation);
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

        byte annotationMask = parseAnnotationMask(query);
        System.out.println("maskToString(annotationMask) = " + maskToString(annotationMask));

        return new SampleIndexQuery(regions, study, samplesMap, annotationMask, queryOperation);
    }


    protected static byte parseAnnotationMask(Query query) {
        // TODO: Allow skip using annotation mask

        byte b = 0;
        List<String> types = query.getAsStringList(VariantQueryParam.TYPE.key());
        if (!types.isEmpty() && !types.contains(VariantType.SNV.toString()) && !types.contains(VariantType.SNP.toString())) {
            b |= NON_SNV_MASK;
        }

        for (String ct : VariantQueryUtils.splitValue(query.getString(ANNOT_CONSEQUENCE_TYPE.key())).getValue()) {
            ct = ConsequenceTypeMappings.accessionToTerm.get(VariantQueryUtils.parseConsequenceType(ct));
            if (ct.equals(VariantAnnotationUtils.MISSENSE_VARIANT)) {
                b |= MISSENSE_VARIANT_MASK;
            } else if (LOF_SET.contains(ct)) {
                b |= LOF_MASK;
            }
        }

        for (String biotype : query.getAsStringList(VariantQueryParam.ANNOT_BIOTYPE.key())) {
            if (biotype.equals(PROTEIN_CODING)) {
                b |= PROTEIN_CODING_MASK;
                break;
            }
        }

        // If filter by proteinSubstitution, without filter << or >>, add ProteinCodingMask
        String proteinSubstitution = query.getString(ANNOT_PROTEIN_SUBSTITUTION.key());
        if (StringUtils.isNotEmpty(proteinSubstitution)
                && !proteinSubstitution.contains("<<")
                && !proteinSubstitution.contains(">>")) {
            b |= MISSENSE_VARIANT_MASK;
        }

        // TODO: This will skip filters ANNOT_POPULATION_REFERENCE_FREQUENCY and ANNOT_POPULATION_MINNOR_ALLELE_FREQUENCY
        String value = query.getString(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
        Pair<QueryOperation, List<String>> pair = VariantQueryUtils.splitValue(value);
        if (pair.getKey() == null || pair.getKey().equals(VariantQueryUtils.QueryOperation.AND)) {
            for (String popFreq : pair.getValue()) {
                String[] keyOpValue = VariantQueryUtils.splitOperator(popFreq);
                if (keyOpValue[1].equals("<")) {
                    if (popFreq.startsWith(GNOMAD_GENOMES + IS + StudyEntry.DEFAULT_COHORT)
                            || popFreq.startsWith(K_GENOMES + IS + StudyEntry.DEFAULT_COHORT)) {
                        Double freqFilter = Double.valueOf(keyOpValue[2]);
//                        if (freqFilter <= POP_FREQ_THRESHOLD_005) {
//                            b |= POP_FREQ_005_MASK;
//                        }
                        if (freqFilter < POP_FREQ_THRESHOLD_001) {
                            b |= POP_FREQ_001_MASK;
                        }
                    }
                }
            }
        }

        return b;
    }

}
