package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.*;
import java.util.stream.Collectors;

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
     * <p>
     * Assumes that the query is valid.
     *
     * @param query           Input query. Will be modified.
     * @param metadataManager VariantStorageMetadataManager
     * @return Valid SampleIndexQuery
     * @see SampleIndexQueryParser#validSampleIndexQuery(Query)
     */
    public static SampleIndexQuery parseSampleIndexQuery(Query query, VariantStorageMetadataManager metadataManager) {
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
        StudyMetadata defaultStudy = VariantQueryUtils.getDefaultStudy(query, null, metadataManager);

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

        if (defaultStudy == null) {
            String sample = samplesMap.keySet().iterator().next();
            throw VariantQueryException.missingStudyForSample(sample, metadataManager.getStudyNames(null));
        }
        String study = defaultStudy.getName();

        byte[] fileMask = parseFileMask(query);
        byte annotationMask = parseAnnotationMask(query);

        return new SampleIndexQuery(regions, study, samplesMap, fileMask[0], fileMask[1], annotationMask, queryOperation);
    }

    protected static byte[] parseFileMask(Query query) {
        byte fileIndexMask = 0;
        byte fileIndex = 0;

        if (isValidParam(query, TYPE)) {
            List<String> types = new ArrayList<>(query.getAsStringList(VariantQueryParam.TYPE.key()));
            if (!types.isEmpty()) {

                boolean snv = types.remove(VariantType.SNV.toString());
                snv |= types.remove(VariantType.SNP.toString());
                boolean indel = !types.isEmpty(); // Other nonSNV values

                if (snv && !indel) { // Pure SNV
                    fileIndexMask |= SampleIndexToHBaseConverter.SNV_MASK;
                    fileIndex |= SampleIndexToHBaseConverter.SNV_MASK;
                } else if (indel && !snv) {
                    fileIndexMask |= SampleIndexToHBaseConverter.SNV_MASK;
                } // else ignore mixed SNV and INDEL filters
            }
        }

        return new byte[]{fileIndexMask, fileIndex};
    }

    protected static byte parseAnnotationMask(Query query) {
        // TODO: Allow skip using annotation mask

        byte b = 0;
//        if (isValidParam(query, TYPE)) {
//            List<String> types = query.getAsStringList(VariantQueryParam.TYPE.key());
//            if (!types.isEmpty() && !types.contains(VariantType.SNV.toString()) && !types.contains(VariantType.SNP.toString())) {
//                b |= UNUSED_6_MASK;
//            }
//        }

        if (isValidParam(query, ANNOT_CONSEQUENCE_TYPE)) {
            List<String> cts = query.getAsStringList(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key());
            cts = cts.stream()
                    .map(ct -> ConsequenceTypeMappings.accessionToTerm.get(VariantQueryUtils.parseConsequenceType(ct)))
                    .collect(Collectors.toList());
            if (LOF_SET.containsAll(cts)) {
                b |= LOF_MASK;
            } else if (LOF_SET_MISSENSE.containsAll(cts)) {
                b |= LOF_MISSENSE_MASK;
            }
        }

        if (isValidParam(query, ANNOT_BIOTYPE)) {
            List<String> biotypes = query.getAsStringList(VariantQueryParam.ANNOT_BIOTYPE.key());
            if (PROTEIN_CODING_BIOTYPE_SET.containsAll(biotypes)) {
                b |= PROTEIN_CODING_MASK;
            }
        }

        // If filter by proteinSubstitution, without filter << or >>, add ProteinCodingMask
        String proteinSubstitution = query.getString(ANNOT_PROTEIN_SUBSTITUTION.key());
        if (StringUtils.isNotEmpty(proteinSubstitution)
                && !proteinSubstitution.contains("<<")
                && !proteinSubstitution.contains(">>")) {
            b |= LOF_MASK;
        }

        // TODO: This will skip filters ANNOT_POPULATION_REFERENCE_FREQUENCY and ANNOT_POPULATION_MINNOR_ALLELE_FREQUENCY
        if (isValidParam(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY)) {
            String value = query.getString(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
            Pair<QueryOperation, List<String>> pair = VariantQueryUtils.splitValue(value);
            QueryOperation op = pair.getKey();

            Set<String> popFreqLessThan01 = new HashSet<>();
            Set<String> popFreqLessThan001 = new HashSet<>();

            for (String popFreq : pair.getValue()) {
                String[] keyOpValue = VariantQueryUtils.splitOperator(popFreq);
                String studyPop = keyOpValue[0];
                Double freqFilter = Double.valueOf(keyOpValue[2]);
                if (keyOpValue[1].equals("<") || keyOpValue[1].equals("<<")) {
                    if (freqFilter <= POP_FREQ_THRESHOLD_01) {
                        popFreqLessThan01.add(studyPop);
                    }
                    if (freqFilter <= POP_FREQ_THRESHOLD_001) {
                        popFreqLessThan001.add(studyPop);
                    }
                }

                if (QueryOperation.AND.equals(op)) {
                    // Use this filter if filtering by popFreq with, at least, all the
                    if (popFreqLessThan01.containsAll(POP_FREQ_ALL_01_SET)) {
                        b |= POP_FREQ_ALL_01_MASK;
                    }
                }

                if (QueryOperation.OR.equals(op)) {
                    // With OR, the query MUST contain ALL popFreq
                    if (popFreqLessThan001.containsAll(POP_FREQ_ANY_001_SET)) {
                        b |= POP_FREQ_ANY_001_MASK;
                    }
                } else {
                    // With AND, the query MUST contain ANY popFreq
                    for (String s : POP_FREQ_ANY_001_SET) {
                        if (popFreqLessThan001.contains(s)) {
                            b |= POP_FREQ_ANY_001_MASK;
                        }
                    }
                }

            }
        }

        return b;
    }

}
