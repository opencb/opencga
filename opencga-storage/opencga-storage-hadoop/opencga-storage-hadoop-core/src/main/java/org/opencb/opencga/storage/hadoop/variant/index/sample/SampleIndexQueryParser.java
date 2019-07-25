package org.opencb.opencga.storage.hadoop.variant.index.sample;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.family.GenotypeCodec;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery.SampleAnnotationIndexQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser.DEFAULT_LOADED_GENOTYPES;
import static org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter.*;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexToHBaseConverter.*;

/**
 * Created by jacobo on 06/01/19.
 */
public class SampleIndexQueryParser {
    private static Logger logger = LoggerFactory.getLogger(SampleIndexQueryParser.class);

    /**
     * Determine if a given query can be used to query with the SampleIndex.
     * @param query Query
     * @return      if the query is valid
     */
    public static boolean validSampleIndexQuery(Query query) {
        VariantQueryParser.VariantQueryXref xref = VariantQueryParser.parseXrefs(query);
        if (!xref.getIds().isEmpty() || !xref.getVariants().isEmpty() || !xref.getOtherXrefs().isEmpty()) {
            // Can not be used for specific variant IDs. Only regions and genes
            return false;
        }

        if (isValidParam(query, GENOTYPE)) {
            HashMap<Object, List<String>> gtMap = new HashMap<>();
            QueryOperation queryOperation = VariantQueryUtils.parseGenotypeFilter(query.getString(GENOTYPE.key()), gtMap);
            boolean allValid = true;
            boolean anyValid = false;
            for (List<String> gts : gtMap.values()) {
                boolean valid = true;
                for (String gt : gts) {
                    // Despite invalid genotypes (i.e. genotypes not in the index) can be used to filter within AND queries,
                    // we require at least one sample where all the genotypes are valid
                    valid &= SampleIndexDBLoader.validGenotype(gt);
                    valid &= !isNegated(gt);
                }
                anyValid |= valid;
                allValid &= valid;
            }
            if (queryOperation == QueryOperation.AND) {
                // Intersect sample filters. If any sample filter is valid, the SampleIndex can be used.
                return anyValid;
            } else {
                // Union of all sample filters. All sample filters must be valid to use the SampleIndex.
                return allValid;
            }
        }
        if (isValidParam(query, SAMPLE, true)) {
            return true;
        }
        if (isValidParam(query, SAMPLE_MENDELIAN_ERROR, true)) {
            return true;
        }
        if (isValidParam(query, SAMPLE_DE_NOVO, true)) {
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
            regions.addAll(Region.parseRegions(query.getString(REGION.key())));
            query.remove(REGION.key());
        }

        if (isValidParam(query, ANNOT_GENE_REGIONS)) {
            regions.addAll(Region.parseRegions(query.getString(ANNOT_GENE_REGIONS.key())));
            query.remove(ANNOT_GENE_REGIONS.key());
            query.remove(GENE.key());
        }

        regions = mergeRegions(regions);

        // TODO: Accept variant IDs?

        // Extract study
        StudyMetadata defaultStudy = VariantQueryUtils.getDefaultStudy(query, null, metadataManager);

        if (defaultStudy == null) {
            throw VariantQueryException.missingStudyForSample("", metadataManager.getStudyNames());
        }
        int studyId = defaultStudy.getId();

        List<String> allGenotypes = getAllLoadedGenotypes(defaultStudy);
        List<String> validGenotypes = allGenotypes.stream().filter(SampleIndexDBLoader::validGenotype).collect(Collectors.toList());

        Set<String> mendelianErrorSet = Collections.emptySet();
        boolean onlyDeNovo = false;
        Map<String, boolean[]> fatherFilterMap = new HashMap<>();
        Map<String, boolean[]> motherFilterMap = new HashMap<>();

        // Extract sample and genotypes to filter
        QueryOperation queryOperation;
        Map<String, List<String>> samplesMap = new HashMap<>();
        List<String> otherSamples = new LinkedList<>();
        if (isValidParam(query, GENOTYPE)) {
            // Get samples with non negated genotypes

            Map<Object, List<String>> map = new HashMap<>();
            queryOperation = parseGenotypeFilter(query.getString(GENOTYPE.key()), map);

            // Extract parents from each sample
            Map<String, List<String>> gtMap = new HashMap<>();
            Map<String, List<String>> parentsMap = new HashMap<>();
            for (Map.Entry<Object, List<String>> entry : map.entrySet()) {
                Object sample = entry.getKey();
                Integer sampleId = metadataManager.getSampleId(studyId, sample);
                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);

                if (sampleMetadata.getFamilyIndexStatus() == TaskMetadata.Status.READY) {
                    String fatherName = null;
                    if (sampleMetadata.getFather() != null) {
                        fatherName = metadataManager.getSampleName(studyId, sampleMetadata.getFather());
                    }
                    String motherName = null;
                    if (sampleMetadata.getMother() != null) {
                        motherName = metadataManager.getSampleName(studyId, sampleMetadata.getMother());
                    }
                    if (fatherName != null || motherName != null) {
                        parentsMap.put(sampleMetadata.getName(), Arrays.asList(fatherName, motherName));
                    }
                }

                gtMap.put(sampleMetadata.getName(), entry.getValue());
            }

            // Determine which samples are parents, and which are children
            Set<String> childrenSet = findChildren(gtMap, queryOperation, parentsMap);
            Set<String> parentsSet = new HashSet<>();
            for (String child : childrenSet) {
                // may add null values
                parentsSet.addAll(parentsMap.get(child));
            }

            boolean covered = true;
            if (isValidParam(query, FORMAT)) {
                covered = false;
            }
            for (Map.Entry<String, List<String>> entry : gtMap.entrySet()) {
                String sampleName = entry.getKey();
                if (queryOperation != QueryOperation.OR && parentsSet.contains(sampleName) && !childrenSet.contains(sampleName)) {
                    // We can skip parents, as their genotype filter will be tested in the child
                    // Discard parents that are not children of another sample
                    // Parents filter can only be used when intersecting (AND) with child
                    logger.debug("Discard parent {}", sampleName);
                    continue;
                }
                if (hasNegatedGenotypeFilter(queryOperation, entry.getValue())) {
                    samplesMap.put(sampleName, entry.getValue());
                    if (queryOperation != QueryOperation.OR && childrenSet.contains(sampleName)) {
                        // Parents filter can only be used when intersecting (AND) with child
                        List<String> parents = parentsMap.get(sampleName);
                        String father = parents.get(0);
                        String mother = parents.get(1);

                        if (father != null) {
                            boolean[] filter = buildParentGtFilter(gtMap.get(father));
                            if (!isFullyCoveredParentFilter(filter)) {
                                covered = false;
                            }
                            fatherFilterMap.put(sampleName, filter);
                        }
                        if (mother != null) {
                            boolean[] filter = buildParentGtFilter(gtMap.get(mother));
                            if (!isFullyCoveredParentFilter(filter)) {
                                covered = false;
                            }
                            motherFilterMap.put(sampleName, filter);
                        }
                    }
                } else {
                    otherSamples.add(sampleName);
                    covered = false;
                }
                // If not all genotypes are valid, query is not covered
                if (!entry.getValue().stream().allMatch(SampleIndexDBLoader::validGenotype)) {
                    covered = false;
                }
            }

            if (covered) {
                query.remove(GENOTYPE.key());
            }
        } else if (isValidParam(query, SAMPLE)) {
            // Filter by all non negated samples
            String samplesStr = query.getString(SAMPLE.key());
            queryOperation = VariantQueryUtils.checkOperator(samplesStr);
            List<String> samples = VariantQueryUtils.splitValue(samplesStr, queryOperation);
            samples.stream().filter(s -> !isNegated(s)).forEach(sample -> samplesMap.put(sample, validGenotypes));

            if (!isValidParam(query, FORMAT)) {
                // Do not remove FORMAT
                query.remove(SAMPLE.key());
            }
            //} else if (isValidParam(query, FILE)) {
            // TODO: Add FILEs filter
        } else if (isValidParam(query, SAMPLE_MENDELIAN_ERROR)) {
            onlyDeNovo = false;
            Pair<QueryOperation, List<String>> mendelianError = splitValue(query.getString(SAMPLE_MENDELIAN_ERROR.key()));
            mendelianErrorSet = new HashSet<>(mendelianError.getValue());
            queryOperation = mendelianError.getKey();
            for (String s : mendelianErrorSet) {
                // Return any genotype
                samplesMap.put(s, Collections.emptyList());
            }
            query.remove(SAMPLE_MENDELIAN_ERROR.key());
        } else if (isValidParam(query, SAMPLE_DE_NOVO)) {
            onlyDeNovo = true;
            Pair<QueryOperation, List<String>> mendelianError = splitValue(query.getString(SAMPLE_DE_NOVO.key()));
            mendelianErrorSet = new HashSet<>(mendelianError.getValue());
            queryOperation = mendelianError.getKey();
            for (String s : mendelianErrorSet) {
                // Return any genotype
                samplesMap.put(s, Collections.emptyList());
            }
            query.remove(SAMPLE_DE_NOVO.key());
        } else {
            throw new IllegalStateException("Unable to query SamplesIndex");
        }

        String study = defaultStudy.getName();

        Map<String, byte[]> fileIndexMap = new HashMap<>(samplesMap.size());
        for (String sample : samplesMap.keySet()) {
            byte[] fileMask = parseFileMask(query, sample, s -> {
                Integer sampleId = metadataManager.getSampleId(studyId, s);
                Set<Integer> fileIds = metadataManager.getFileIdsFromSampleIds(studyId, Collections.singleton(sampleId));
                List<String> fileNames = new ArrayList<>(fileIds.size());
                for (Integer fileId : fileIds) {
                    fileNames.add(metadataManager.getFileName(studyId, fileId));
                }
                return fileNames;
            });
            fileIndexMap.put(sample, fileMask);
        }
        boolean allSamplesAnnotated = true;
        if (otherSamples.isEmpty()) {
            for (String sample : samplesMap.keySet()) {
                Integer sampleId = metadataManager.getSampleId(studyId, sample);
                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
                if (!sampleMetadata.getStatus(SampleIndexAnnotationLoader.SAMPLE_INDEX_STATUS).equals(TaskMetadata.Status.READY)) {
                    allSamplesAnnotated = false;
                    break;
                }
            }
        } else {
            allSamplesAnnotated = false;
        }

        SampleAnnotationIndexQuery annotationIndexQuery = parseAnnotationIndexQuery(query, allSamplesAnnotated);
        Set<VariantType> variantTypes = null;
        if (isValidParam(query, TYPE)) {
            List<String> typesStr = query.getAsStringList(VariantQueryParam.TYPE.key());
            if (!typesStr.isEmpty()) {
                variantTypes = new HashSet<>(typesStr.size());
                for (String type : typesStr) {
                    variantTypes.add(VariantType.valueOf(type));
                }
            }
            query.remove(TYPE.key());
        }

        return new SampleIndexQuery(regions, variantTypes, study, samplesMap, fatherFilterMap, motherFilterMap, fileIndexMap,
                annotationIndexQuery,
                mendelianErrorSet, onlyDeNovo, queryOperation);
    }

    protected static boolean hasNegatedGenotypeFilter(QueryOperation queryOperation, List<String> gts) {
        boolean valid = true;
        for (String gt : gts) {
            if (queryOperation == QueryOperation.OR && !SampleIndexDBLoader.validGenotype(gt)) {
                // Invalid genotypes (i.e. genotypes not in the index) are not allowed in OR queries
                throw new IllegalStateException("Genotype '" + gt + "' not in the SampleIndex.");
            }
            valid &= !isNegated(gt);
        }
        return valid;
    }

    /**
     * Determine which samples are valid children.
     *
     * i.e. sample with non negated genotype filter and parents in the query
     *
     * @param gtMap Genotype filter map
     * @param queryOperation Query operation
     * @param parentsMap Parents map
     * @return Set with all children from the query
     */
    protected static Set<String> findChildren(Map<String, List<String>> gtMap, QueryOperation queryOperation,
                                              Map<String, List<String>> parentsMap) {
        Set<String> childrenSet = new HashSet<>(parentsMap.size());
        for (Map.Entry<String, List<String>> entry : parentsMap.entrySet()) {
            String child = entry.getKey();
            List<String> parents = entry.getValue();

            if (!hasNegatedGenotypeFilter(queryOperation, gtMap.get(child))) {
                // Discard children with negated iterators
                continue;
            }

            // Remove parents not in query
            for (int i = 0; i < parents.size(); i++) {
                String parent = parents.get(i);
                if (!gtMap.containsKey(parent)) {
                    parents.set(i, null);
                }
            }

            String father = parents.get(0);
            String mother = parents.get(1);
            if (father != null || mother != null) {
                // Is a child if has any parent
                childrenSet.add(child);
            }
        }
        return childrenSet;
    }

    protected static boolean[] buildParentGtFilter(List<String> parentGts) {
        boolean[] filter = new boolean[GenotypeCodec.NUM_CODES]; // all false by default
        for (String gt : parentGts) {
            filter[GenotypeCodec.encode(gt)] = true;
        }
        return filter;
    }

    public static boolean isFullyCoveredParentFilter(boolean[] filter) {
        for (int i = 0; i < filter.length; i++) {
            if (filter[i]) {
                if (GenotypeCodec.isAmbiguousCode(i)) {
                    return false;
                }
            }
        }
        return true;
    }

    protected static byte[] parseFileMask(Query query, String sample, Function<String, Collection<String>> filesFromSample) {
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

        if (isValidParam(query, FILTER)) {
            List<String> filterValues = splitValue(query.getString(FILTER.key())).getRight();

            if (filterValues.size() == 1) {
                if (filterValues.get(0).equals(VCFConstants.PASSES_FILTERS_v4)) {
                    // PASS
                    fileIndexMask |= SampleIndexToHBaseConverter.FILTER_PASS_MASK;
                    fileIndex |= SampleIndexToHBaseConverter.FILTER_PASS_MASK;
                } else if (filterValues.get(0).equals(VariantQueryUtils.NOT + VCFConstants.PASSES_FILTERS_v4)) {
                    // !PASS
                    fileIndexMask |= SampleIndexToHBaseConverter.FILTER_PASS_MASK;
                } else if (!isNegated(filterValues.get(0))) {
                    // Non negated filter, other than PASS
                    fileIndexMask |= SampleIndexToHBaseConverter.FILTER_PASS_MASK;
                }
            } else {
                if (!filterValues.contains(VCFConstants.PASSES_FILTERS_v4)) {
                    if (filterValues.stream().noneMatch(VariantQueryUtils::isNegated)) {
                        // None negated filter, without PASS
                        fileIndexMask |= SampleIndexToHBaseConverter.FILTER_PASS_MASK;
                    }
                } // else --> Mix PASS and other filters. Can not use index
            }
        }

        if (isValidParam(query, QUAL)) {
            String qualValue = query.getString(QUAL.key());
            List<String> qualValues = VariantQueryUtils.splitValue(qualValue).getValue();
            if (qualValues.size() == 1) {
                String[] split = VariantQueryUtils.splitOperator(qualValue);
                String op = split[1];
                double value = Double.valueOf(split[2]);
                Boolean index20 = IndexUtils.intersectIndexGreaterThan(op, value, QUAL_THRESHOLD_20);
                if (index20 != null) {
                    fileIndexMask |= QUAL_GT_20_MASK;
                    if (index20) {
                        fileIndex |= QUAL_GT_20_MASK;
                    }
                }
                Boolean index40 = IndexUtils.intersectIndexGreaterThan(op, value, QUAL_THRESHOLD_40);
                if (index40 != null) {
                    fileIndexMask |= QUAL_GT_40_MASK;
                    if (index40) {
                        fileIndex |= QUAL_GT_40_MASK;
                    }
                }
            }
        }

        if (isValidParam(query, INFO)) {
            Map<String, String> infoMap = VariantQueryUtils.parseInfo(query).getValue();
            // Lazy get files from sample
            Collection<String> files = filesFromSample.apply(sample);
            for (String file : files) {
                String values = infoMap.get(file);

                if (StringUtils.isNotEmpty(values)) {
                    for (String value : VariantQueryUtils.splitValue(values).getValue()) {
                        String[] split = VariantQueryUtils.splitOperator(value);
                        if (split[0].equals(VCFConstants.DEPTH_KEY)) {
                            Boolean indexDp = IndexUtils.intersectIndexGreaterThan(split[1], Double.valueOf(split[2]), DP_THRESHOLD_20);
                            if (indexDp != null) {
                                fileIndexMask |= DP_GT_20_MASK;
                                if (indexDp) {
                                    fileIndex |= DP_GT_20_MASK;
                                }
                            }
                        }
                    }
                }
            }
        }

        if (isValidParam(query, FORMAT)) {
            Map<String, String> format = VariantQueryUtils.parseFormat(query).getValue();
            String values = format.get(sample);

            if (StringUtils.isNotEmpty(values)) {
                for (String value : VariantQueryUtils.splitValue(values).getValue()) {
                    String[] split = VariantQueryUtils.splitOperator(value);
                    if (split[0].equals(VCFConstants.DEPTH_KEY)) {
                        Boolean indexDp = IndexUtils.intersectIndexGreaterThan(split[1], Double.valueOf(split[2]), DP_THRESHOLD_20);
                        if (indexDp != null) {
                            fileIndexMask |= DP_GT_20_MASK;
                            if (indexDp) {
                                fileIndex |= DP_GT_20_MASK;
                            }
                        }
                    }
                }
            }
        }

        return new byte[]{fileIndexMask, fileIndex};
    }

    protected static byte parseAnnotationMask(Query query) {
        return parseAnnotationMask(query, false);
    }

    protected static byte parseAnnotationMask(Query query, boolean allSamplesAnnotated) {
        return parseAnnotationIndexQuery(query, allSamplesAnnotated).getAnnotationIndexMask();
    }

    protected static SampleAnnotationIndexQuery parseAnnotationIndexQuery(Query query) {
        return parseAnnotationIndexQuery(query, false);
    }

    protected static SampleAnnotationIndexQuery parseAnnotationIndexQuery(Query query, boolean allSamplesAnnotated) {
        // TODO: Allow skip using annotation mask

        byte annotationIndex = 0;
        byte biotypeMask = 0;
        short consequenceTypeMask = 0;

        Boolean intergenic = null;

        if (!isValidParam(query, REGION)) {
            VariantQueryParser.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);
            if (!variantQueryXref.getGenes().isEmpty()
                    && variantQueryXref.getIds().isEmpty()
                    && variantQueryXref.getOtherXrefs().isEmpty()
                    && variantQueryXref.getVariants().isEmpty()) {
                // If only filtering by genes, is not intergenic.
                intergenic = false;
            }
        }

        BiotypeConsquenceTypeFlagCombination combination = BiotypeConsquenceTypeFlagCombination.fromQuery(query);

        if (isValidParam(query, ANNOT_CONSEQUENCE_TYPE)) {
            List<String> soNames = query.getAsStringList(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key());
            soNames = soNames.stream()
                    .map(ct -> ConsequenceTypeMappings.accessionToTerm.get(VariantQueryUtils.parseConsequenceType(ct)))
                    .collect(Collectors.toList());
            if (!soNames.contains(VariantAnnotationUtils.INTERGENIC_VARIANT)) {
                // All ct values bit "intergenic_variant" are in genes (i.e. non-intergenic)
                intergenic = false;
            } else if (soNames.size() == 1 && soNames.contains(VariantAnnotationUtils.INTERGENIC_VARIANT)) {
                intergenic = true;
            }
            boolean ctFilterCoveredBySummary = false;
            if (LOF_SET.containsAll(soNames)) {
                ctFilterCoveredBySummary = soNames.size() == LOF_SET.size();
                annotationIndex |= LOF_MASK;
                // If all present, remove consequenceType filter
                if (allSamplesAnnotated && LOF_SET.size() == soNames.size()) {
                    // Ensure not filtering by gene, and not combining with other params
                    if (!isValidParam(query, GENE) && combination.numParams() == 1) {
                        query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                    }
                }
            }
            if (LOF_EXTENDED_SET.containsAll(soNames)) {
                ctFilterCoveredBySummary = soNames.size() == LOF_EXTENDED_SET.size();
                annotationIndex |= LOF_EXTENDED_MASK;
                // If all present, remove consequenceType filter
                if (allSamplesAnnotated && LOF_EXTENDED_SET.size() == soNames.size() && !isValidParam(query, GENE)) {
                    // Ensure not filtering by gene, and not combining with other params
                    if (!isValidParam(query, GENE) && combination.numParams() == 1) {
                        query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                    }
                }
            }
            if (soNames.size() == 1 && soNames.get(0).equals(VariantAnnotationUtils.MISSENSE_VARIANT)) {
                ctFilterCoveredBySummary = true;
                annotationIndex |= MISSENSE_VARIANT_MASK;
            }

            // Do not use ctIndex if the CT filter is covered by the summary
            if (!ctFilterCoveredBySummary) {
                for (String soName : soNames) {
                    short mask = getMaskFromSoName(soName);
                    if (mask == IndexUtils.EMPTY_MASK) {
                        // If any element is not in the index, do not use this filter
                        consequenceTypeMask = IndexUtils.EMPTY_MASK;
                        break;
                    }
                    consequenceTypeMask |= mask;
                }
            }
        }

        if (isValidParam(query, ANNOT_BIOTYPE)) {
            // All biotype values are in genes (i.e. non-intergenic)
            intergenic = false;
            boolean biotypeFilterCoveredBySummary = false;
            List<String> biotypes = query.getAsStringList(VariantQueryParam.ANNOT_BIOTYPE.key());
            if (BIOTYPE_SET.containsAll(biotypes)) {
                biotypeFilterCoveredBySummary = BIOTYPE_SET.size() == biotypes.size();
                annotationIndex |= PROTEIN_CODING_MASK;
                // If all present, remove biotype filter
                if (allSamplesAnnotated && BIOTYPE_SET.size() == biotypes.size()) {
                    // Ensure not filtering by gene, and not combining with other params
                    if (!isValidParam(query, GENE) && combination.numParams() == 1) {
                        query.remove(ANNOT_BIOTYPE.key());
                    }
                }
            }
            if (!biotypeFilterCoveredBySummary) {
                for (String biotype : biotypes) {
                    byte mask = getMaskFromBiotype(biotype);
                    if (mask == IndexUtils.EMPTY_MASK) {
                        // If any element is not in the index, do not use this filter
                        biotypeMask = IndexUtils.EMPTY_MASK;
                        break;
                    }
                    biotypeMask |= mask;
                }
            }
        }

        // If filter by proteinSubstitution, without filter << or >>, add ProteinCodingMask
        String proteinSubstitution = query.getString(ANNOT_PROTEIN_SUBSTITUTION.key());
        if (StringUtils.isNotEmpty(proteinSubstitution)
                && !proteinSubstitution.contains("<<")
                && !proteinSubstitution.contains(">>")) {
            annotationIndex |= LOF_EXTENDED_MASK;
        }

        // TODO: This will skip filters ANNOT_POPULATION_REFERENCE_FREQUENCY and ANNOT_POPULATION_MINNOR_ALLELE_FREQUENCY
        if (isValidParam(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY)) {
            String value = query.getString(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
            Pair<QueryOperation, List<String>> pair = VariantQueryUtils.splitValue(value);
            QueryOperation op = pair.getKey();

            Set<String> popFreqLessThan001 = new HashSet<>();

            for (String popFreq : pair.getValue()) {
                String[] keyOpValue = VariantQueryUtils.splitOperator(popFreq);
                String studyPop = keyOpValue[0];
                Double freqFilter = Double.valueOf(keyOpValue[2]);
                if (keyOpValue[1].equals("<") || keyOpValue[1].equals("<<")) {
                    if (freqFilter <= POP_FREQ_THRESHOLD_001) {
                        popFreqLessThan001.add(studyPop);
                    }
                }

                if (QueryOperation.OR.equals(op)) {
                    // With OR, the query MUST contain ALL popFreq
                    if (popFreqLessThan001.containsAll(POP_FREQ_ANY_001_SET)) {
                        annotationIndex |= POP_FREQ_ANY_001_MASK;
                    }
                } else {
                    // With AND, the query MUST contain ANY popFreq
                    for (String s : POP_FREQ_ANY_001_SET) {
                        if (popFreqLessThan001.contains(s)) {
                            annotationIndex |= POP_FREQ_ANY_001_MASK;
                        }
                    }
                }
            }

            if (allSamplesAnnotated
                    && pair.getKey() == QueryOperation.OR
                    && POP_FREQ_ANY_001_FILTERS.size() == pair.getValue().size()
                    && POP_FREQ_ANY_001_FILTERS.containsAll(pair.getValue())) {
                query.remove(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
            }
        }

        byte annotationIndexMask = annotationIndex;
        if (intergenic != null) {
            if (!intergenic) {
                annotationIndexMask |= INTERGENIC_MASK;
            }
            // We can not use the intergenic mask in positive, as it only covers variants exclusively intergenic
            // It may left out some "regulatory_region_variant" or "TF_binding_site_variant"
        }

        if (intergenic == null || intergenic) {
            // If intergenic is undefined, or true, CT and BT filters can not be used.
            consequenceTypeMask = IndexUtils.EMPTY_MASK;
            biotypeMask = IndexUtils.EMPTY_MASK;
        }

        // TODO
        List<SampleAnnotationIndexQuery.PopulationFrequencyQuery> popFreqAnnotationIndexMask = Collections.emptyList();

        return new SampleAnnotationIndexQuery(
                new byte[]{annotationIndexMask, annotationIndex}, consequenceTypeMask, biotypeMask, popFreqAnnotationIndexMask);
    }

    private static List<String> getAllLoadedGenotypes(StudyMetadata studyMetadata) {
        List<String> allGts = studyMetadata
                .getAttributes()
                .getAsStringList(VariantStorageEngine.Options.LOADED_GENOTYPES.key());
        if (allGts == null || allGts.isEmpty()) {
            allGts = DEFAULT_LOADED_GENOTYPES;
        }
        return allGts;
    }
}
