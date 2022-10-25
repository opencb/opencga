package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.core.models.variant.VariantAnnotationConstants;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.*;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.CtBtFtCombinationIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;
import org.opencb.opencga.storage.hadoop.variant.index.core.RangeIndexField;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.NoOpIndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.RangeIndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.family.GenotypeCodec;
import org.opencb.opencga.storage.hadoop.variant.index.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser.DEFAULT_LOADED_GENOTYPES;
import static org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter.*;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR;

/**
 * Created by jacobo on 06/01/19.
 */
public class SampleIndexQueryParser {
    private static Logger logger = LoggerFactory.getLogger(SampleIndexQueryParser.class);
    private final VariantStorageMetadataManager metadataManager;
    private final SampleIndexSchemaFactory schemaFactory;

    public SampleIndexQueryParser(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
        this.schemaFactory = new SampleIndexSchemaFactory(metadataManager);
    }

    public SampleIndexQueryParser(VariantStorageMetadataManager metadataManager, SampleIndexSchemaFactory schemaFactory) {
        this.metadataManager = metadataManager;
        this.schemaFactory = schemaFactory;
    }

    /**
     * Determine if a given query can be used to query with the SampleIndex.
     * @param query Query
     * @return      if the query is valid
     */
    public static boolean validSampleIndexQuery(Query query) {
        ParsedVariantQuery.VariantQueryXref xref = VariantQueryParser.parseXrefs(query);
        if (!xref.getIds().isEmpty() || !xref.getOtherXrefs().isEmpty()) {
            // Can not be used for specific variant xrefs. Only variant, regions and genes
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
                    valid &= !isNegated(gt) && SampleIndexSchema.validGenotype(gt);
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
        if (isValidParam(query, SAMPLE_DE_NOVO_STRICT, true)) {
            return true;
        }
        return false;
    }

    public SingleSampleIndexQuery parse(List<Region> regions, String study, String sample, List<String> genotypes) {
        return parse(regions, study, Collections.singletonMap(sample, genotypes), null).forSample(sample);
    }

    public SampleIndexQuery parse(List<Region> regions, String study, Map<String, List<String>> samplesMap, QueryOperation queryOperation) {
        if (queryOperation == null) {
            queryOperation = QueryOperation.OR;
        }
        String gtFilter = samplesMap.entrySet()
                .stream()
                .map(e -> e.getKey() + ":" + String.join(",", e.getValue()))
                .collect(Collectors.joining(queryOperation.separator()));

        return parse(new Query(REGION.key(), regions).append(STUDY.key(), study).append(GENOTYPE.key(), gtFilter));
    }

    /**
     * Build SampleIndexQuery. Extract Regions (+genes), Study, Sample and Genotypes.
     * <p>
     * Assumes that the query is valid.
     *
     * @param query           Input query. Will be modified.
     * @return Valid SampleIndexQuery
     * @see SampleIndexQueryParser#validSampleIndexQuery(Query)
     */
    public SampleIndexQuery parse(Query query) {
        // Extract study
        StudyMetadata defaultStudy = VariantQueryParser.getDefaultStudy(query, metadataManager);

        if (defaultStudy == null) {
            throw VariantQueryException.missingStudyForSample("", metadataManager.getStudyNames());
        }
        int studyId = defaultStudy.getId();
        String study = defaultStudy.getName();

        List<String> allGenotypes = getAllLoadedGenotypes(defaultStudy);
        List<String> validGenotypes = allGenotypes.stream().filter(SampleIndexSchema::validGenotype).collect(Collectors.toList());
        List<String> mainGenotypes = GenotypeClass.MAIN_ALT.filter(validGenotypes);


        boolean partialIndex = false;

        QueryOperation queryOperation;
        // Unprocessed input genotype query
        Map<String, List<String>> sampleGenotypeInputQuery = new HashMap<>();
        // Map from all samples to query to its list of genotypes.
        Map<String, List<String>> sampleGenotypeQuery = new HashMap<>();
        // Samples that are returning data from more than one file
        Set<String> multiFileSamples = new HashSet<>();
        // Samples that are querying
        Set<String> negatedSamples = new HashSet<>();
        // Samples from the query that can not be used to filter. e.g. samples with invalid or negated genotypes
        // If any, the query is not covered.
        List<String> negatedGenotypesSamples = new LinkedList<>();
        // Samples from the query that are parents of any other sample in the query, with the FamilyIndex calculated.
        // These samples are excluded form the smaplesMap
        Set<String> parentsInQuery = new HashSet<>();

        // Father/Mother filters
        Map<String, boolean[]> fatherFilterMap = new HashMap<>();
        Map<String, boolean[]> motherFilterMap = new HashMap<>();

        Set<String> mendelianErrorSet = new HashSet<>();
        SampleIndexQuery.MendelianErrorType mendelianErrorType = null;
        boolean partialGtIndex = false;

        Map<String, SampleMetadata> sampleMetadatas = new HashMap<>();


        // Extract sample and genotypes to filter
        if (isValidParam(query, GENOTYPE)) {
            // Get samples with non negated genotypes
            Map<Object, List<String>> map = new HashMap<>();
            queryOperation = parseGenotypeFilter(query.getString(GENOTYPE.key()), map);

            for (Map.Entry<Object, List<String>> entry : map.entrySet()) {
                Object sample = entry.getKey();
                Integer sampleId = metadataManager.getSampleId(studyId, sample);

                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
                String sampleName = sampleMetadata.getName();
                sampleMetadatas.put(sampleName, sampleMetadata);

                if (VariantStorageEngine.SplitData.MULTI == sampleMetadata.getSplitData()) {
                    multiFileSamples.add(sampleName);
                }

                List<String> gts = GenotypeClass.filter(entry.getValue(), allGenotypes);
                if (!gts.stream().allMatch(SampleIndexSchema::validGenotype)) {
                    negatedSamples.add(sampleName);
                }

                if (hasNegatedGenotypeFilter(queryOperation, entry.getValue())) {
                    // Discard samples with negated genotypes
                    negatedGenotypesSamples.add(sampleName);
                    partialIndex = true;
                    logger.info("Set partialGtIndex to true. Prev value: {}", partialGtIndex);
                    partialGtIndex = true;
                } else {
                    sampleGenotypeQuery.put(sampleName, entry.getValue());
                }
                sampleGenotypeInputQuery.put(sampleName, gts);
            }
        } else if (isValidParam(query, SAMPLE)) {
            // Filter by all non negated samples
            String samplesStr = query.getString(SAMPLE.key());
            queryOperation = VariantQueryUtils.checkOperator(samplesStr);
            List<String> samples = VariantQueryUtils.splitValue(samplesStr, queryOperation);
            for (String s : samples) {
                if (!isNegated(s)) {
                    sampleGenotypeQuery.put(s, mainGenotypes);
                    sampleGenotypeInputQuery.put(s, mainGenotypes);
                }
            }

            if (!isValidParam(query, SAMPLE_DATA)) {
                // Do not remove if SAMPLE_DATA filter is present
                query.remove(SAMPLE.key());
            }
        } else if (isValidParam(query, SAMPLE_MENDELIAN_ERROR)) {
            mendelianErrorType = SampleIndexQuery.MendelianErrorType.ALL;
            ParsedQuery<String> mendelianError = splitValue(query, SAMPLE_MENDELIAN_ERROR);
            mendelianErrorSet = new HashSet<>(mendelianError.getValues());
            queryOperation = mendelianError.getOperation();
            for (String s : mendelianErrorSet) {
                // Return any genotype
                sampleGenotypeQuery.put(s, mainGenotypes);
            }
            query.remove(SAMPLE_MENDELIAN_ERROR.key());
            // Reading any MendelianError could return variants from GT=0/0, which is not annotated in the SampleIndex,
            // so the index is partial.
            partialIndex = true;
        } else if (isValidParam(query, SAMPLE_DE_NOVO)) {
            mendelianErrorType = SampleIndexQuery.MendelianErrorType.DE_NOVO;
            ParsedQuery<String> sampleDeNovo = splitValue(query, SAMPLE_DE_NOVO);
            mendelianErrorSet = new HashSet<>(sampleDeNovo.getValues());
            queryOperation = sampleDeNovo.getOperation();
            for (String s : mendelianErrorSet) {
                // Return any genotype
                sampleGenotypeQuery.put(s, mainGenotypes);
            }
            query.remove(SAMPLE_DE_NOVO.key());
        } else if (isValidParam(query, SAMPLE_DE_NOVO_STRICT)) {
            mendelianErrorType = SampleIndexQuery.MendelianErrorType.DE_NOVO_STRICT;
            ParsedQuery<String> sampleDeNovo = splitValue(query, SAMPLE_DE_NOVO_STRICT);
            mendelianErrorSet = new HashSet<>(sampleDeNovo.getValues());
            queryOperation = sampleDeNovo.getOperation();
            for (String s : mendelianErrorSet) {
                // Return any genotype
                sampleGenotypeQuery.put(s, mainGenotypes);
            }
            query.remove(SAMPLE_DE_NOVO_STRICT.key());
        //} else if (isValidParam(query, FILE)) {
            // Add FILEs filter ?
        } else {
            throw new IllegalStateException("Unable to query SamplesIndex");
        }
        boolean requireFamilyIndex = !mendelianErrorSet.isEmpty();
        SampleIndexSchema schema = schemaFactory.getSchema(studyId, sampleGenotypeQuery.keySet(), false, requireFamilyIndex);


        if (queryOperation == QueryOperation.AND && mendelianErrorSet.isEmpty()) {
            // Parents filter can only be used when intersecting (AND) with child
            // And if not filtering by mendelian error
            Map<String, List<String>> parentsMap = new HashMap<>();

            // Get parents "tree" from valid samples filter
            for (String sampleName : sampleGenotypeQuery.keySet()) {
                if (!negatedSamples.contains(sampleName)) {
                    SampleMetadata sampleMetadata = getSampleMetadata(sampleMetadatas, sampleName, studyId);

                    if (sampleMetadata.getFamilyIndexStatus(schema.getVersion()) == TaskMetadata.Status.READY) {
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
                }
            }

            // Determine which samples are parents, and which are children
            Set<String> childrenSet = findChildren(sampleGenotypeQuery, parentsMap);
            Set<String> parentsSet = findParents(childrenSet, parentsMap);

            // Copy list of samples, so we can modify the map internally
            for (String sampleName : new ArrayList<>(sampleGenotypeQuery.keySet())) {
                if (parentsSet.contains(sampleName) && !childrenSet.contains(sampleName)) {
                    parentsInQuery.add(sampleName);
                    // We can skip parents, as their genotype filter will be tested in the child
                    // Discard parents that are not children of another sample
                    // Parents filter can only be used when intersecting (AND) with child
                    boolean discardParent = true;

                    // We should avoid discarding parents if there were other sampleData filters covered by the parent sample index
                    // Samples of this map will determine which set of sampleIndexes should be read
                    if (isValidParam(query, SAMPLE_DATA)) {
                        ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> sampleDataParsedQuery = parseSampleData(query);
                        KeyValues<String, KeyOpValue<String, String>> sampleDataFilter
                                = sampleDataParsedQuery.getValue(k -> k.getKey().equals(sampleName));
                        if (sampleDataFilter != null) {
                            // SampleData filter exists for this sample!
                            // Check if ANY filter is covered by the index
                            for (KeyOpValue<String, String> entry : sampleDataFilter.getValues()) {
                                if (schema.getFileIndex().getField(IndexFieldConfiguration.Source.SAMPLE, entry.getKey()) != null) {
                                    // This key is covered by the sample index. Do not discard this parent!
                                    discardParent = false;
                                    break;
                                }
                            }
                        }
                    }

                    if (discardParent) {
                        logger.debug("Discard parent {}", sampleName);
                        // Remove from negatedSamples (if present)
                        negatedSamples.remove(sampleName);
                        sampleGenotypeQuery.remove(sampleName);
                    }
                } else if (childrenSet.contains(sampleName)) {
                    // Parents filter can only be used when intersecting (AND) with child
                    List<String> parents = parentsMap.get(sampleName);
                    String father = parents.get(0);
                    String mother = parents.get(1);

                    if (father != null) {
                        Integer fatherId = metadataManager.getSampleId(studyId, father);
                        boolean includeDiscrepancies = VariantStorageEngine.SplitData.MULTI
                                == metadataManager.getLoadSplitData(studyId, fatherId);
                        List<Integer> fatherFiles = sampleMetadatas.get(father).getFiles();
                        List<Integer> sampleFiles = sampleMetadatas.get(sampleName).getFiles();
                        boolean parentInSeparatedFile =
                                fatherFiles.size() != sampleFiles.size() || !fatherFiles.containsAll(sampleFiles);
                        boolean[] filter = buildParentGtFilter(
                                sampleGenotypeInputQuery.get(father),
                                includeDiscrepancies,
                                parentInSeparatedFile);
                        if (!isFullyCoveredParentFilter(filter)) {
                            logger.debug("FATHER - Set partialGtIndex to true. Prev value: {}", partialGtIndex);
                            partialGtIndex = true;
                        }
//                            logger.info("Father={}, includeDiscrepancies={}, fatherFiles={}, sampleFiles={}, "
//                                            + "parentInSeparatedFile={}, fullyCoveredFilter={}",
//                                    father, includeDiscrepancies, fatherFiles, sampleFiles,
//                                    parentInSeparatedFile, isFullyCoveredParentFilter(filter));
                        fatherFilterMap.put(sampleName, filter);
                    }
                    if (mother != null) {
                        Integer motherId = metadataManager.getSampleId(studyId, mother);
                        boolean includeDiscrepancies = VariantStorageEngine.SplitData.MULTI
                                .equals(metadataManager.getLoadSplitData(studyId, motherId));
                        List<Integer> motherFiles = sampleMetadatas.get(mother).getFiles();
                        List<Integer> sampleFiles = sampleMetadatas.get(sampleName).getFiles();
                        boolean parentInSeparatedFile =
                                motherFiles.size() != sampleFiles.size() || !motherFiles.containsAll(sampleFiles);
                        boolean[] filter = buildParentGtFilter(
                                sampleGenotypeInputQuery.get(mother),
                                includeDiscrepancies,
                                parentInSeparatedFile);
                        if (!isFullyCoveredParentFilter(filter)) {
                            logger.debug("MOTHER - Set partialGtIndex to true. Prev value: {}", partialGtIndex);
                            partialGtIndex = true;
                        }
//                            logger.info("Mother={}, includeDiscrepancies={}, motherFiles={}, sampleFiles={}, "
//                                            + "parentInSeparatedFile={}, fullyCoveredFilter={}",
//                                    mother, includeDiscrepancies, motherFiles, sampleFiles,
//                                    parentInSeparatedFile, isFullyCoveredParentFilter(filter));
                        motherFilterMap.put(sampleName, filter);
                    }
                }
            }
        }

        // If not all genotypes are valid, query is not covered
        if (!negatedSamples.isEmpty()) {
            logger.debug("NEG_SAMPLES - Set partialGtIndex to true. Prev value: {}, negSamples: {}", partialGtIndex, negatedSamples);
            partialGtIndex = true;
        }
        for (String negatedSample : negatedSamples) {
            List<String> negatedGenotypes = new ArrayList<>(validGenotypes);
            negatedGenotypes.removeAll(sampleGenotypeQuery.get(negatedSample));
            sampleGenotypeQuery.put(negatedSample, negatedGenotypes);
        }
        if (!partialGtIndex) {
            if (isValidParam(query, GENOTYPE)) {
                // Do not remove genotypes list if FORMAT is present.
                if (!isValidParam(query, SAMPLE_DATA)) {
                    query.remove(GENOTYPE.key());
                }
            }
        }

        boolean partialFilesIndex = false;
        if (!negatedGenotypesSamples.isEmpty() || !parentsInQuery.isEmpty()) {
            Set<Integer> sampleFiles = new HashSet<>(sampleGenotypeQuery.size());
            for (String sample : sampleGenotypeQuery.keySet()) {
                sampleFiles.addAll(getSampleMetadata(sampleMetadatas, sample, studyId).getFiles());
            }

            // If the file of any other sample is not between the files of the samples in the query, mark as partial
            for (String sample : negatedGenotypesSamples) {
                for (Integer file : getSampleMetadata(sampleMetadatas, sample, studyId).getFiles()) {
                    if (!sampleFiles.contains(file)) {
                        partialFilesIndex = true;
                        break;
                    }
                }
            }
            // If the file of any parent is not between the files of the samples in the query, mark as partial
            for (String sample : parentsInQuery) {
                for (Integer file : getSampleMetadata(sampleMetadatas, sample, studyId).getFiles()) {
                    if (!sampleFiles.contains(file)) {
                        partialFilesIndex = true;
                        break;
                    }
                }
            }

        }
        Map<String, Values<SampleFileIndexQuery>> fileIndexMap
                = parseSampleSpecificQuery(schema, query, studyId, queryOperation, sampleGenotypeQuery, multiFileSamples,
                partialGtIndex, partialFilesIndex);

        // It might happen that some sampleIndexes are not fully annotated.
        boolean allSamplesAnnotated = true;
        if (negatedGenotypesSamples.isEmpty()) {
            for (String sample : sampleGenotypeQuery.keySet()) {
                SampleMetadata sampleMetadata = getSampleMetadata(sampleMetadatas, sample, studyId);
                if (sampleMetadata.getSampleIndexAnnotationStatus(schema.getVersion()) != TaskMetadata.Status.READY) {
                    allSamplesAnnotated = false;
                    break;
                }
            }
        } else {
            allSamplesAnnotated = false;
        }


        boolean completeIndex = allSamplesAnnotated && !partialIndex;
        SampleAnnotationIndexQuery annotationIndexQuery = parseAnnotationIndexQuery(schema, query, completeIndex);
        Set<VariantType> variantTypes = null;
        if (isValidParam(query, TYPE)) {
            List<String> typesStr = query.getAsStringList(VariantQueryParam.TYPE.key());
            if (!typesStr.isEmpty()) {
                variantTypes = new HashSet<>(typesStr.size());
                for (String type : typesStr) {
                    variantTypes.add(VariantType.valueOf(type));
                }
                if (variantTypes.contains(VariantType.COPY_NUMBER_GAIN)) {
                    // Can not distinguish between COPY_NUMBER_GAIN and COPY_NUMBER. Filter only by COPY_NUMBER
                    variantTypes.remove(VariantType.COPY_NUMBER_GAIN);
                    variantTypes.add(VariantType.COPY_NUMBER);
                }
                if (variantTypes.contains(VariantType.COPY_NUMBER_LOSS)) {
                    // Can not distinguish between COPY_NUMBER_LOSS and COPY_NUMBER. Filter only by COPY_NUMBER
                    variantTypes.remove(VariantType.COPY_NUMBER_LOSS);
                    variantTypes.add(VariantType.COPY_NUMBER);
                }
            }
            if (!hasCopyNumberGainFilter(typesStr) && !hasCopyNumberLossFilter(typesStr)) {
                query.remove(TYPE.key());
            }
        }

        ParsedVariantQuery.VariantQueryXref xref = VariantQueryParser.parseXrefs(query);
        boolean mixRegionsOrIdsWithGenesAndCtBt =
                (isValidParam(query, REGION) || !xref.getVariants().isEmpty())
                &&
                (!xref.getGenes().isEmpty() && (isValidParam(query, ANNOT_CONSEQUENCE_TYPE) || isValidParam(query, ANNOT_BIOTYPE)));

        // Extract regions
        List<Region> regions = new ArrayList<>();
        if (isValidParam(query, REGION)) {
            regions.addAll(Region.parseRegions(query.getString(REGION.key()), true));
            if (!mixRegionsOrIdsWithGenesAndCtBt) {
                query.remove(REGION.key());
            }
//            else {
//                logger.info("Keep region!");
//            }
        }
        // Extract IDs
        List<Variant> variants = xref.getVariants();
        if (!variants.isEmpty()) {
            if (!mixRegionsOrIdsWithGenesAndCtBt) {
                query.remove(ID.key());
            }
//            else {
//                logger.info("Keep variants!");
//            }
        }

        if (isValidParam(query, ANNOT_GENE_REGIONS)) {
            regions.addAll(Region.parseRegions(query.getString(ANNOT_GENE_REGIONS.key()), true));
            if (!mixRegionsOrIdsWithGenesAndCtBt) {
                if (isValidParam(query, ANNOT_CONSEQUENCE_TYPE) || isValidParam(query, ANNOT_BIOTYPE)) {
                    query.put(ANNOT_GENE_REGIONS.key(), SKIP_GENE_REGIONS);
                } else {
                    query.remove(ANNOT_GENE_REGIONS.key());
                    query.remove(GENE.key());
                }
            }
//            else {
//                logger.info("Keep genes!");
//            }
        }

        Collection<LocusQuery> regionGroups = buildLocusQueries(regions, variants);

        return new SampleIndexQuery(schema, regionGroups, variantTypes, study, sampleGenotypeQuery, multiFileSamples, negatedSamples,
                fatherFilterMap, motherFilterMap,
                fileIndexMap, annotationIndexQuery, mendelianErrorSet, mendelianErrorType, queryOperation);
    }

    private Set<String> findParents(Set<String> childrenSet, Map<String, List<String>> parentsMap) {
        Set<String> parentsSet = new HashSet<>();
        for (String child : childrenSet) {
            // may add null values
            parentsSet.addAll(parentsMap.get(child));
        }
        return parentsSet;
    }

    private SampleMetadata getSampleMetadata(Map<String, SampleMetadata> sampleMetadatas, String sampleName, int studyId) {
        SampleMetadata sampleMetadata = sampleMetadatas.get(sampleName);
        if (sampleMetadata == null) {
            Integer sampleId = metadataManager.getSampleId(studyId, sampleName);
            sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
            sampleMetadatas.put(sampleName, sampleMetadata);
        }
        return sampleMetadata;
    }


    /**
     * Merge and group regions by chunk start.
     * Concurrent groups of regions are merged into one single group.
     * Resulting groups are sorted using {@link VariantQueryUtils#REGION_COMPARATOR}.
     * Each group will be translated to a SCAN.
     *
     * @param regions List of regions to group
     * @param variants List of variants to group
     * @return Locus Queries
     */
    public static Collection<LocusQuery> buildLocusQueries(List<Region> regions, List<Variant> variants) {
        regions = mergeRegions(regions);
        Map<Region, LocusQuery> groupsMap = new HashMap<>();
        for (Region region : regions) {
            Region chunkRegion = SampleIndexSchema.getChunkRegion(region);
            groupsMap.computeIfAbsent(chunkRegion, LocusQuery::new).getRegions().add(region);
        }
        for (Variant variant : variants) {
            Region chunkRegion = SampleIndexSchema.getChunkRegion(variant);
            groupsMap.computeIfAbsent(chunkRegion, LocusQuery::new).getVariants().add(variant);
        }

        List<LocusQuery> locusQueries = new ArrayList<>(groupsMap.values());

        if (!locusQueries.isEmpty()) {
            locusQueries.forEach(rg -> rg.getRegions().sort(REGION_COMPARATOR));
            locusQueries.forEach(rg -> rg.getVariants().sort(INTRA_CHROMOSOME_VARIANT_COMPARATOR));
            locusQueries.sort(Comparator.comparing(LocusQuery::getChunkRegion, REGION_COMPARATOR));

            //Merge consecutive groups
            Iterator<LocusQuery> iterator = locusQueries.iterator();
            LocusQuery prevQuery = iterator.next();
            while (iterator.hasNext()) {
                LocusQuery query = iterator.next();
                Region prevRegion = prevQuery.getChunkRegion();
                Region region = query.getChunkRegion();
                // Merge if the distance between groups is less than 2 batches size
                // TODO: This rule could be changed to reduce the number of small queries, even if more data is fetched.
                if (region.getChromosome().equals(prevRegion.getChromosome())
                        && (region.getStart() - prevRegion.getEnd()) <= SampleIndexSchema.BATCH_SIZE * 2) {
                    // Merge groups
                    prevQuery.merge(query);
                    iterator.remove();
                } else {
                    prevQuery = query;
                }
            }
        }
        return locusQueries;
    }


    protected static boolean hasNegatedGenotypeFilter(QueryOperation queryOperation, List<String> gts) {
        boolean anyNegated = false;
        for (String gt : gts) {
            if (queryOperation == QueryOperation.OR && !SampleIndexSchema.validGenotype(gt)) {
                // Invalid genotypes (i.e. genotypes not in the index) are not allowed in OR queries
                throw new IllegalStateException("Genotype '" + gt + "' not in the SampleIndex.");
            }
            anyNegated |= isNegated(gt);
        }
        return anyNegated;
    }

    /**
     * Determine which samples are valid children.
     *
     * i.e. sample with non negated genotype filter and parents in the query
     *
     * @param samplesMap     Genotype filter map
     * @param parentsMap Parents map
     * @return Set with all children from the query
     */
    protected Set<String> findChildren(Map<String, List<String>> samplesMap, Map<String, List<String>> parentsMap) {
        Set<String> childrenSet = new HashSet<>(parentsMap.size());
        for (Map.Entry<String, List<String>> entry : parentsMap.entrySet()) {
            String child = entry.getKey();
            List<String> parents = entry.getValue();

//            if (hasNegatedGenotypeFilter(queryOperation, gtMap.get(child))) {
//                // Discard children with negated iterators
//                continue;
//            }

            // Remove parents not in query
            for (int i = 0; i < parents.size(); i++) {
                String parent = parents.get(i);
                if (!samplesMap.containsKey(parent)) {
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

    protected static boolean[] buildParentGtFilter(List<String> parentGts, boolean includeDiscrepancies, boolean parentInSeparatedFile) {
        boolean[] filter = new boolean[GenotypeCodec.NUM_CODES]; // all false by default
        for (String gt : parentGts) {
            filter[GenotypeCodec.encode(gt)] = true;
        }
        if (includeDiscrepancies) {
            filter[GenotypeCodec.DISCREPANCY_SIMPLE] = true;
            filter[GenotypeCodec.DISCREPANCY_ANY] = true;
        }
        if (parentInSeparatedFile) {
            // If parents were in separated files, missing and hom_ref might be registered as "unknown"
            if (filter[GenotypeCodec.MISSING_HOM] || filter[GenotypeCodec.HOM_REF_UNPHASED] || filter[GenotypeCodec.HOM_REF_PHASED]) {
                filter[GenotypeCodec.UNKNOWN] = true;
            }
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



    private Map<String, Values<SampleFileIndexQuery>> parseSampleSpecificQuery(
            SampleIndexSchema schema, Query query, int studyId, QueryOperation queryOperation,
            Map<String, List<String>> samplesMap, Set<String> multiFileSamples, boolean partialGtIndex, boolean partialFilesIndex) {
        // 1. Split query -- getQueriesPerSample
        // 2. Parse Files Query
        //   2.1. Parse File query
        // 3. Rebuild Query with covered/non-covered params

        Map<String, Values<SampleFileIndexQuery>> fileIndexMap = new HashMap<>(samplesMap.size());

        Query nonCoveredQuery = new Query();
        Map<String, Query> queriesPerSample = getQueriesPerSample(studyId, query, queryOperation, samplesMap.keySet(), nonCoveredQuery);
        for (String sample : samplesMap.keySet()) {
            Query sampleQuery = queriesPerSample.get(sample);
            Values<SampleFileIndexQuery> fileIndexQuery = parseFilesQuery(
                    schema, sampleQuery, studyId, sample, multiFileSamples.contains(sample), partialGtIndex, partialFilesIndex);
            fileIndexMap.put(sample, fileIndexQuery);
        }


        // -- Check for covered filters --
        // Filters need to be checked for every sample, as each sample may provide different set of variants.
        //
        // Sample INTERSECTION (AND)
        //   If any sample covers a filter from QUERY it doesn't need to be even checked on any other sample.
        //   That query will be then removed from the QUERY object.
        //   We can share the same QUERY object.
        //
        //   e.g.
        //   QUERY = samples = (S1 AND S2) AND type = SNV AND (S1:DP>10 OR S2:DP>30)
        //     SampleFileIndexQuery[0] = S1 , type = SNV , DP>10
        //     SampleFileIndexQuery[1] = S2 , type = SNV , DP>30
        //
        // Sample UNION (OR)
        //   A filter will be covered if it's covered by ALL samples.
        //   If a sample covers a filter, it will be removed from the QUERY.
        //   If all samples remove one particular filter from QUERY (i.e. all samples cover that filter)
        //   it is definitely covered, and can be removed from the final query
        //
        //   e.g.
        //   QUERY = samples = (S1 OR S2) AND type = SNV AND (S1:DP>10 OR S2:DP>30)
        //     SampleFileIndexQuery[0] = S1 , type = SNV , DP>10
        //     SampleFileIndexQuery[1] = S2 , type = SNV , DP>30
        //     Query is covered
        //
        //   QUERY = samples = (S1 OR S2) AND file=file_S1.vcf.gz
        //     SampleFileIndexQuery[0] = S1 , file = file_S1.vcf.gz
        //     SampleFileIndexQuery[1] = S2
        //     Query is NOT covered. Still need to filter by "file=file_S1.vcf.gz", as it was not covered by S2


        List<String> coveredParams = new ArrayList<>(query.size());
        for (String queryParam : query.keySet()) {
            boolean queryParamCoveredOnAllSamples = true;
            if (nonCoveredQuery.get(queryParam) != null) {
                queryParamCoveredOnAllSamples = false;
            }
            for (Query sampleQuery : queriesPerSample.values()) {
                if (sampleQuery.containsKey(queryParam)) {
                    // This sample does not cover this query param, so it's not covered on ALL samples.
                    queryParamCoveredOnAllSamples = false;
                    break;
                }
            }
            if (queryParamCoveredOnAllSamples) {
                // This query param is covered on ALL samples. We can remove it.
                coveredParams.add(queryParam);
            }
        }
        for (String coveredParam : coveredParams) {
            query.remove(coveredParam);
        }

        return fileIndexMap;
    }

    private Map<String, Query> getQueriesPerSample(int studyId, Query query, QueryOperation queryOperation, Collection<String> samples,
                                                   Query nonCoveredQuery) {
        Map<String, Query> queriesPerSample = new HashMap<>(samples.size());

        // Non processed query params.
        // This method will remove elements from these ParsedQueries when they are processed by any sample.
        // If any of these values keep any element at the end of the method, will mean that the filter param is not being fully used.
        // Therefore, it's added to "nonCovered"
        ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> unprocessedFileDataParsedQuery = parseFileData(query);
        ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> unprocessedSampleDataParsedQuery = parseSampleData(query);
        ParsedQuery<String> unprocessedFilesQuery = splitValue(query, FILE);

        for (String sample : samples) {
            Query sampleQuery = new Query(query);
            List<String> filesFromSample = getFilesFromSample(studyId, sample);

            if (queryOperation == QueryOperation.OR) {
                // This new sampleQuery will process all file and fileData elements.
                unprocessedFilesQuery.getValues().clear();
                unprocessedFileDataParsedQuery.getValues().clear();
            } else {
//                if (isValidParam(sampleQuery, TYPE)) {
//                    List<VariantType> types = query.getAsStringList(VariantQueryParam.TYPE.key())
//                            .stream()
//                            .map(t -> VariantType.valueOf(t.toUpperCase()))
//                            .collect(Collectors.toList());
//                    if (schema.getFileIndex().getTypeIndex().buildFilter(QueryOperation.OR, types).isExactFilter()) {
//                        query.remove(TYPE.key());
//                    }
//                }
                if (isValidParam(sampleQuery, FILE)) {
                    ParsedQuery<String> filesQuery = splitValue(query, FILE);
                    boolean processFileQuery = false;
                    if (filesQuery.getOperation() == QueryOperation.AND || filesQuery.getOperation() == null) {
                        // FILE filter with AND (or just one file) can be processed by this sample, and potentially be split
                        processFileQuery = true;
                    } else if (filesQuery.getOperation() == QueryOperation.OR && filesFromSample.containsAll(filesQuery.getValues())) {
                        // FILE filter with OR where all files are from THIS sample, can be processed
                        processFileQuery = true;
                    }
                    if (processFileQuery) {
                        filesQuery.getValues().removeIf(file -> !filesFromSample.contains(file));
                        if (filesQuery.isEmpty()) {
                            sampleQuery.remove(FILE.key());
                        } else {
                            unprocessedFilesQuery.getValues().removeAll(filesQuery.getValues());
                            sampleQuery.put(FILE.key(), filesQuery.toQuery());
                        }
                    }
                }
                if (isValidParam(sampleQuery, FILE_DATA)) {
                    ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> fileDataParsedQuery = parseFileData(query);
//                    System.out.println(fileDataParsedQuery.describe());

                    ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> subFilter
                            = fileDataParsedQuery.filter(p -> filesFromSample.contains(p.getKey()));
                    if (subFilter.getValues().isEmpty()) {
                        // FileData not found for this sample.
                        sampleQuery.remove(FILE_DATA.key());
                    } else {
                        // FileData found. Remove from input query.
                        for (KeyValues<String, KeyOpValue<String, String>> value : subFilter.getValues()) {
                            unprocessedFileDataParsedQuery.getValues()
                                    .removeIf(inputFileDataQuery -> inputFileDataQuery.getKey().equals(value.getKey()));
                        }
                        fileDataParsedQuery.getValues().removeAll(subFilter.getValues());
//                        query.put(FILE_DATA.key(), fileDataParsedQuery.toQuery());
                        sampleQuery.put(FILE_DATA.key(), subFilter.toQuery());
                    }
                }
            }

            if (isValidParam(sampleQuery, SAMPLE_DATA)) {
                ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> sampleDataParsedQuery = parseSampleData(query);
//                System.out.println(sampleDataParsedQuery.describe());

                KeyValues<String, KeyOpValue<String, String>> kv = sampleDataParsedQuery.getValue(p -> p.getKey().equals(sample));
                if (kv == null) {
                    // SampleData not found for this sample.
                    sampleQuery.remove(SAMPLE_DATA.key());
                } else {
                    // SampleData found. Remove from query.
                    sampleDataParsedQuery.getValues().remove(kv);
                    unprocessedSampleDataParsedQuery.getValues().removeIf(thisKv -> thisKv.getKey().equals(kv.getKey()));
//                        query.put(SAMPLE_DATA.key(), sampleDataParsedQuery.toQuery());
                    sampleQuery.put(SAMPLE_DATA.key(), kv.toQuery());
                }
            }

            queriesPerSample.put(sample, sampleQuery);
        }

        if (unprocessedFileDataParsedQuery.isNotEmpty()) {
            nonCoveredQuery.put(FILE_DATA.key(), query.get(FILE_DATA.key()));
        }
        if (unprocessedSampleDataParsedQuery.isNotEmpty()) {
            nonCoveredQuery.put(SAMPLE_DATA.key(), query.get(SAMPLE_DATA.key()));
        }
        if (unprocessedFilesQuery.isNotEmpty()) {
            nonCoveredQuery.put(FILE.key(), query.get(FILE.key()));
        }

        return queriesPerSample;
    }


    protected Values<SampleFileIndexQuery> parseFilesQuery(SampleIndexSchema schema, Query query, int studyId, String sample,
                                                           boolean multiFileSample, boolean partialGtIndex, boolean partialFilesIndex) {
        return parseFilesQuery(
                schema,
                query,
                sample,
                multiFileSample,
                partialGtIndex,
                partialFilesIndex,
                s -> getFilesFromSample(studyId, s));
    }

    private List<String> getFilesFromSample(int studyId, String s) {
        Integer sampleId = metadataManager.getSampleId(studyId, s);
        List<Integer> fileIds = metadataManager.getFileIdsFromSampleId(studyId, sampleId);
        List<String> fileNames = new ArrayList<>(fileIds.size());
        for (Integer fileId : fileIds) {
            fileNames.add(metadataManager.getFileName(studyId, fileId));
        }
        return fileNames;
    }

    protected Values<SampleFileIndexQuery> parseFilesQuery(SampleIndexSchema schema, Query query, String sample, boolean multiFileSample,
                                                           boolean partialGtIndex, boolean partialFilesIndex,
                                                           Function<String, List<String>> filesFromSample) {
        ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> fileDataParsedQuery = parseFileData(query);
        QueryOperation filesOperation;
        List<String> filesFromQuery;
        if (fileDataParsedQuery.isEmpty()) {
            ParsedQuery<String> files = splitValue(query, FILE);
            filesOperation = files.getOperation();
            filesFromQuery = files.getValues();
        } else {
            filesOperation = fileDataParsedQuery.getOperation();
            filesFromQuery = fileDataParsedQuery.mapValues(KeyValues::getKey);
        }


        boolean splitFileDataQuery = false;
        if (filesOperation == QueryOperation.AND) {
            splitFileDataQuery = true;
        } else if (filesOperation == QueryOperation.OR && multiFileSample) {
            List<String> files = filesFromSample.apply(sample);
            if (files.containsAll(filesFromQuery)) {
                // All samples from the query are from the same sample (aka multi-query sample)
                splitFileDataQuery = true;
            }
        }
        if (splitFileDataQuery) {
            List<SampleFileIndexQuery> fileIndexQueries = new ArrayList<>(filesFromQuery.size());
            boolean fileDataCovered = true;
            boolean fileCovered = true;
            boolean typeCovered = true;
            boolean filterCovered = true;
            boolean qualCovered = true;
            for (String fileFromQuery : filesFromQuery) {
                Query subQuery = new Query(query);
//                    subQuery.remove(FILE.key());
                KeyValues<String, KeyOpValue<String, String>> fileDataQuery =
                        fileDataParsedQuery.getValue(kv -> kv.getKey().equals(fileFromQuery));
                if (fileDataQuery == null) {
                    subQuery.put(FILE.key(), fileFromQuery);
                } else {
                    subQuery.put(FILE_DATA.key(), fileDataQuery.toQuery());
                }
                fileIndexQueries.add(parseFileQuery(schema, subQuery, sample, multiFileSample,
                        partialGtIndex, partialFilesIndex, filesFromSample));
                if (isValidParam(subQuery, FILE_DATA)) {
                    // This subquery did not remove the fileData, so it's not fully covered. Can't remove the fileData filter.
                    fileDataCovered = false;
                }
                if (isValidParam(subQuery, TYPE)) {
                    typeCovered = false;
                }
                if (isValidParam(subQuery, FILE)) {
                    fileCovered = false;
                }
                if (isValidParam(subQuery, FILTER)) {
                    filterCovered = false;
                }
                if (isValidParam(subQuery, QUAL)) {
                    qualCovered = false;
                }
            }
            if (!partialFilesIndex) {
                if (fileDataCovered) {
                    query.remove(FILE_DATA.key());
                }
                if (fileCovered) {
                    query.remove(FILE.key());
                }
                if (filterCovered) {
                    query.remove(FILTER.key());
                }
                if (qualCovered) {
                    query.remove(QUAL.key());
                }
            }
            if (typeCovered) {
                query.remove(TYPE.key());
            }
            return new Values<>(filesOperation, fileIndexQueries);
        } else {
            return new Values<>(null, Collections.singletonList(
                    parseFileQuery(schema, query, sample, multiFileSample, partialGtIndex, partialFilesIndex, filesFromSample)));
        }
    }

    protected SampleFileIndexQuery parseFileQuery(SampleIndexSchema schema, Query query, String sample, boolean multiFileSample,
                                                  boolean partialGtIndex, boolean partialFilesIndex,
                                                  Function<String, List<String>> filesFromSample) {

        List<String> files = null;
        List<IndexFieldFilter> filtersList = new ArrayList<>(schema.getFileIndex().getFields().size());

        if (isValidParam(query, TYPE)) {
            List<VariantType> types = query.getAsStringList(VariantQueryParam.TYPE.key())
                    .stream()
                    .map(t -> VariantType.valueOf(t.toUpperCase()))
                    .collect(Collectors.toList());
            if (!types.isEmpty()) {
                IndexFieldFilter typeFilter = schema.getFileIndex().getTypeIndex().buildFilter(QueryOperation.OR, types);
                filtersList.add(typeFilter);
                if (typeFilter.isExactFilter()) {
                    query.remove(TYPE.key());
                }
            }
        }

        List<Integer> sampleFilesFilter = new ArrayList<>();
        // Can only filter by file if the sample was multiFile
        if (multiFileSample) {
            // Lazy get files from sample
            if (files == null) {
                files = filesFromSample.apply(sample);
            }
            final List<String> filesFromQuery;
            final QueryOperation filesOperation;
            if (isValidParam(query, FILE)) {
                ParsedQuery<String> filesQuery = splitValue(query, FILE);
                filesFromQuery = filesQuery.getValues();
                filesOperation = filesQuery.getOperation();
                if (files.containsAll(filesFromQuery)) {
                    query.remove(FILE.key());
                }
            } else if (isValidParam(query, FILE_DATA)) {
                ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> fileData = parseFileData(query);
                filesFromQuery = fileData.mapValues(KeyValues::getKey);
                filesOperation = fileData.getOperation();
            } else {
                filesFromQuery = null;
                filesOperation = null;
            }
            if (filesFromQuery != null) {
                if (CollectionUtils.containsAll(files, filesFromQuery)
                        || CollectionUtils.containsAny(files, filesFromQuery) && filesOperation == QueryOperation.AND) {
                    for (String file : filesFromQuery) {
                        int indexOf = files.indexOf(file);
                        if (indexOf >= 0) {
                            sampleFilesFilter.add(indexOf);
                        }
                    }
                    IndexFieldFilter filePositionFilter =
                            schema.getFileIndex().getFilePositionIndex().buildFilter(QueryOperation.OR, sampleFilesFilter);
                    filtersList.add(filePositionFilter);
                }

            }
        } else {
            if (isValidParam(query, FILE)) {
                ParsedQuery<String> filesQuery = splitValue(query, FILE);
                // Conditions for having a covered file query param
                // 1. Filter by one single file
                //   In case of filtering my multiple samples, the upper layer should have
                //   been able to remove some of them if affecting to other samples
                // 2. Sample present in one single file
                //   This block has already discarded the "multi-file-sample" scenario, but
                //   we could still be in a "SplitData.REGION" scenario
                if (filesQuery.size() == 1) {
                    // Lazy get files from sample
                    if (files == null) {
                        files = filesFromSample.apply(sample);
                    }
                    if (files.size() == 1 && files.equals(filesQuery.getValues())) {
                        query.remove(FILE.key());
                    }
                }
            }
        }

        if (isValidParam(query, FILTER)) {
            IndexField<String> filterIndexField = schema.getFileIndex()
                    .getCustomField(IndexFieldConfiguration.Source.FILE, StudyEntry.FILTER);
            if (filterIndexField != null) {
                Values<String> filterValues = splitValue(query, FILTER);
                IndexFieldFilter indexFieldFilter = filterIndexField.buildFilter(filterValues.getOperation(), filterValues.getValues());
                filtersList.add(indexFieldFilter);
                if (indexFieldFilter.isExactFilter() && !partialFilesIndex) {
                    query.remove(FILTER.key());
                }
            }
        }

        if (isValidParam(query, QUAL)) {
            IndexField<String> qualIndexField = schema.getFileIndex()
                    .getCustomField(IndexFieldConfiguration.Source.FILE, StudyEntry.QUAL);
            if (qualIndexField != null) {
                OpValue<String> opValue = parseOpValue(query.getString(QUAL.key()));
                IndexFieldFilter indexFieldFilter = qualIndexField.buildFilter(opValue);
                filtersList.add(indexFieldFilter);
                if (indexFieldFilter.isExactFilter() && !partialFilesIndex) {
                    query.remove(QUAL.key());
                }
            }
        }

        boolean fileDataCovered = true;
        if (isValidParam(query, FILE_DATA)) {
            //ParsedQuery<KeyValues< FileId , KeyOpValue< INFO , Value >>>
            ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> parsedQuery = parseFileData(query);
            if (parsedQuery.getOperation() != QueryOperation.OR) {
                Map<String, KeyValues<String, KeyOpValue<String, String>>> fileDataMap =
                        parsedQuery.getValues().stream().collect(Collectors.toMap(KeyValues::getKey, i -> i));
                // Lazy get files from sample
                if (files == null) {
                    files = filesFromSample.apply(sample);
                }
                if (!files.containsAll(fileDataMap.keySet())) {
                    // Some of the files in FileData filter are not from this sample.
                    fileDataCovered = false;
                }
                for (String file : files) {
                    KeyValues<String, KeyOpValue<String, String>> keyValues = fileDataMap.get(file);
                    if (keyValues == null) {
                        continue;
                    }
                    for (KeyOpValue<String, String> keyOpValue : keyValues.getValues()) {
                        IndexField<String> fileDataIndexField = schema.getFileIndex()
                                .getCustomField(IndexFieldConfiguration.Source.FILE, keyOpValue.getKey());
                        if (fileDataIndexField == null) {
                            // Unknown key
                            fileDataCovered = false;
                        } else {
                            Values<String> values = splitValues(keyOpValue.getValue());
                            IndexFieldFilter indexFieldFilter =
                                    fileDataIndexField.buildFilter(values.getOperation(), keyOpValue.getOp(), values.getValues());
                            filtersList.add(indexFieldFilter);
                            if (!indexFieldFilter.isExactFilter()) {
                                fileDataCovered = false;
                            }
                        }
                    }
                }
            } else {
                fileDataCovered = false;
            }
        }
        if (fileDataCovered && !partialFilesIndex) {
            query.remove(FILE_DATA.key());
        }

        if (isValidParam(query, SAMPLE_DATA)) {
            ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> sampleDataQuery = parseSampleData(query);
            QueryOperation sampleDataOp = sampleDataQuery.getOperation();
            KeyValues<String, KeyOpValue<String, String>> sampleDataFilter = sampleDataQuery.getValue(kv -> kv.getKey().equals(sample));

            if (!sampleDataFilter.isEmpty() && sampleDataOp != QueryOperation.OR) {
                for (KeyOpValue<String, String> keyOpValue : sampleDataFilter) {
                    IndexField<String> sampleDataIndexField = schema.getFileIndex()
                            .getCustomField(IndexFieldConfiguration.Source.SAMPLE, keyOpValue.getKey());
                    if (sampleDataIndexField != null) {
                        IndexFieldFilter indexFieldFilter = sampleDataIndexField.buildFilter(keyOpValue);
                        filtersList.add(indexFieldFilter);
                        if (indexFieldFilter.isExactFilter() && !partialFilesIndex) {
                            sampleDataQuery.getValues().remove(sampleDataFilter);
                        }
                    }
                }
                if (!partialFilesIndex) {
                    if (sampleDataQuery.isEmpty()) {
                        query.remove(SAMPLE_DATA.key());
                        if (!partialGtIndex) {
                            query.remove(GENOTYPE.key());
                            query.remove(SAMPLE.key());
                        }
                    } else {
                        query.put(SAMPLE_DATA.key(), sampleDataQuery.toQuery());
                    }
                }
            }
        }

        filtersList.removeIf(f -> f instanceof NoOpIndexFieldFilter);

        return new SampleFileIndexQuery(sample, filtersList);
    }

    private boolean hasCopyNumberGainFilter(List<String> types) {
        return types.contains(VariantType.COPY_NUMBER_GAIN.name()) && !types.contains(VariantType.COPY_NUMBER.name());
    }

    private boolean hasCopyNumberLossFilter(List<String> types) {
        return types.contains(VariantType.COPY_NUMBER_LOSS.name()) && !types.contains(VariantType.COPY_NUMBER.name());
    }

    protected SampleAnnotationIndexQuery parseAnnotationIndexQuery(SampleIndexSchema schema, Query query) {
        return parseAnnotationIndexQuery(schema, query, false);
    }

    /**
     * Builds the SampleAnnotationIndexQuery given a VariantQuery.
     *
     * @param schema Sample index schema for the set of samples in the query
     * @param query Input VariantQuery. If the index is complete, covered filters could be removed from here.
     * @param completeIndex Indicates if the annotation index is complete for the samples in the query.
     *                      Otherwise, the index can only be used as a hint, and should be completed with further filtering.
     * @return SampleAnnotationIndexQuery
     */
    protected SampleAnnotationIndexQuery parseAnnotationIndexQuery(SampleIndexSchema schema, Query query, boolean completeIndex) {
        byte annotationIndex = 0;
        IndexFieldFilter biotypeFilter = schema.getBiotypeIndex().getField().noOpFilter();
        IndexFieldFilter consequenceTypeFilter = schema.getCtIndex().getField().noOpFilter();
        IndexFieldFilter tfFilter = schema.getTranscriptFlagIndexSchema().getField().noOpFilter();
        CtBtFtCombinationIndexSchema.Filter ctBtTfFilter = schema.getCtBtTfIndex().getField().noOpFilter();
        IndexFilter clinicalFilter = schema.getClinicalIndexSchema().noOpFilter();

        Boolean intergenic = null;

        ParsedVariantQuery.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);
        if (!isValidParam(query, REGION)) {
            if (!variantQueryXref.getGenes().isEmpty()
                    && variantQueryXref.getIds().isEmpty()
                    && variantQueryXref.getOtherXrefs().isEmpty()
                    && variantQueryXref.getVariants().isEmpty()) {
                // If only filtering by genes, is not intergenic.
                intergenic = false;
            }
        }

//        BiotypeConsquenceTypeFlagCombination combination = BiotypeConsquenceTypeFlagCombination
//                .fromQuery(query, Arrays.asList(schema.getTranscriptFlagIndexSchema().getField().getConfiguration().getValues()));
        BiotypeConsquenceTypeFlagCombination combination = BiotypeConsquenceTypeFlagCombination.fromQuery(query, null);
        boolean btCovered = false;
        boolean ctCovered = false;
        boolean tfCovered = false;

        if (isValidParam(query, ANNOT_CONSEQUENCE_TYPE)) {
            List<String> soNames = query.getAsStringList(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key());
            soNames = soNames.stream()
                    .map(ct -> ConsequenceTypeMappings.accessionToTerm.get(VariantQueryUtils.parseConsequenceType(ct)))
                    .collect(Collectors.toList());
            if (!soNames.contains(VariantAnnotationConstants.INTERGENIC_VARIANT)
                    && !soNames.contains(VariantAnnotationConstants.REGULATORY_REGION_VARIANT)
                    && !soNames.contains(VariantAnnotationConstants.TF_BINDING_SITE_VARIANT)) {
                // All ct values but "intergenic_variant" and "regulatory_region_variant" are in genes (i.e. non-intergenic)
                intergenic = false;
            } else if (soNames.size() == 1 && soNames.contains(VariantAnnotationConstants.INTERGENIC_VARIANT)) {
                intergenic = true;
            } // else, leave undefined : intergenic = null
            boolean ctFilterCoveredBySummary = false;
            boolean ctBtCombinationCoveredBySummary = false;
            if (SampleIndexSchema.CUSTOM_LOF.containsAll(soNames)) {
                ctFilterCoveredBySummary = soNames.size() == SampleIndexSchema.CUSTOM_LOF.size();
                annotationIndex |= CUSTOM_LOF_MASK;
                // If all present, remove consequenceType filter
                if (completeIndex && SampleIndexSchema.CUSTOM_LOF.size() == soNames.size()) {
                    // Ensure not filtering by gene, and not combining with other params
                    if (!isValidParam(query, GENE) && simpleCombination(combination)) {
                        query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                    }
                }
            }
            if (SampleIndexSchema.CUSTOM_LOFE.containsAll(soNames)) {
                boolean proteinCodingOnly = query.getString(ANNOT_BIOTYPE.key()).equals(VariantAnnotationConstants.PROTEIN_CODING);
                ctFilterCoveredBySummary = soNames.size() == SampleIndexSchema.CUSTOM_LOFE.size();
                annotationIndex |= CUSTOM_LOFE_MASK;
                // If all present, remove consequenceType filter
                if (SampleIndexSchema.CUSTOM_LOFE.size() == soNames.size()) {
                    // Ensure not filtering by gene, and not combining with other params
                    if (completeIndex && !isValidParam(query, GENE)) {
                        if (simpleCombination(combination)) {
                            query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                        } else if (proteinCodingOnly && combination.equals(BiotypeConsquenceTypeFlagCombination.BIOTYPE_CT)) {
                            query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                            query.remove(ANNOT_BIOTYPE.key());
                            ctBtCombinationCoveredBySummary = true;
                        }
                    }
                }
                if (proteinCodingOnly) {
                    annotationIndex |= CUSTOM_LOFE_PROTEIN_CODING_MASK;
                }
            }
            if (soNames.size() == 1 && soNames.get(0).equals(VariantAnnotationConstants.MISSENSE_VARIANT)) {
                ctFilterCoveredBySummary = true;
                ctCovered = true;
                annotationIndex |= MISSENSE_VARIANT_MASK;
                // Ensure not filtering by gene, and not combining with other params
                if (completeIndex && !isValidParam(query, GENE)) {
                    if (simpleCombination(combination)) {
                        query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                    }
                }
            }

            // Do not use ctIndex if the CT filter is covered by the summary
            // Use the ctIndex if:
            // - The CtFilter is not covered by the summary
            // - The query has the combination CT+BT , and it is not covered by the summary
            // - The query has the combination CT+TF
            boolean useCtIndexFilter = !ctFilterCoveredBySummary
                    || (!ctBtCombinationCoveredBySummary && combination.isBiotype())
                    || combination.isFlag();
            if (useCtIndexFilter) {
                ctCovered = completeIndex;
                consequenceTypeFilter = schema.getCtIndex().getField().buildFilter(new OpValue<>("=", soNames));
                ctCovered &= consequenceTypeFilter.isExactFilter();
                // ConsequenceType filter is covered by index
                if (ctCovered) {
                    if (!isValidParam(query, GENE) && simpleCombination(combination)) {
                        query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                    }
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
                if (completeIndex && BIOTYPE_SET.size() == biotypes.size()) {
                    // Ensure not filtering by gene, and not combining with other params
                    if (!isValidParam(query, GENE) && simpleCombination(combination)) {
                        query.remove(ANNOT_BIOTYPE.key());
                    }
                }
            }

            boolean useBtIndexFilter = !biotypeFilterCoveredBySummary || combination.numParams() > 1;
            if (useBtIndexFilter) {
                biotypeFilter = schema.getBiotypeIndex().getField().buildFilter(new OpValue<>("=", biotypes));
                btCovered = completeIndex & biotypeFilter.isExactFilter();
                // Biotype filter is covered by index
                if (btCovered) {
                    if (!isValidParam(query, GENE) && simpleCombination(combination)) {
                        query.remove(ANNOT_BIOTYPE.key());
                    }
                }
            }
        }

        if (isValidParam(query, ANNOT_TRANSCRIPT_FLAG)) {
            List<String> transcriptFlags = query.getAsStringList(ANNOT_TRANSCRIPT_FLAG.key());
            tfFilter = schema.getTranscriptFlagIndexSchema().getField().buildFilter(new OpValue<>("=", transcriptFlags));
            tfCovered = completeIndex & tfFilter.isExactFilter();
            // Transcript flags are in transcripts/genes. (i.e. non-intergenic)
            intergenic = false;
            // TranscriptFlag filter is covered by index
            if (tfCovered) {
                if (!isValidParam(query, GENE) && simpleCombination(combination)) {
                    query.remove(ANNOT_TRANSCRIPT_FLAG.key());
                }
            }
        }

        if (combination.numParams() > 1 && schema.getConfiguration().getAnnotationIndexConfiguration().getTranscriptCombination()) {
            ctBtTfFilter = schema.getCtBtTfIndex().getField().buildFilter(consequenceTypeFilter, biotypeFilter, tfFilter);

            if (completeIndex && !isValidParam(query, GENE)) {
                switch (combination) {
                    case BIOTYPE_CT:
                        if (btCovered && ctCovered) {
                            query.remove(ANNOT_BIOTYPE.key());
                            query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                        }
                    break;
                    case CT_FLAG:
                        if (ctCovered && tfCovered) {
                            query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                            query.remove(ANNOT_TRANSCRIPT_FLAG.key());
                        }
                    break;
                    case BIOTYPE_FLAG:
                        if (btCovered && tfCovered) {
                            query.remove(ANNOT_BIOTYPE.key());
                            query.remove(ANNOT_TRANSCRIPT_FLAG.key());
                        }
                    break;
                    case BIOTYPE_CT_FLAG:
                        if (btCovered && ctCovered && tfCovered) {
                            query.remove(ANNOT_BIOTYPE.key());
                            query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                            query.remove(ANNOT_TRANSCRIPT_FLAG.key());
                        }
                    break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + combination);
                }
            }
        }

        // If filter by proteinSubstitution, without filter << or >>, add ProteinCodingMask
        String proteinSubstitution = query.getString(ANNOT_PROTEIN_SUBSTITUTION.key());
        if (StringUtils.isNotEmpty(proteinSubstitution)
                && !proteinSubstitution.contains("<<")
                && !proteinSubstitution.contains(">>")) {
            annotationIndex |= CUSTOM_LOFE_MASK;
        }

        List<IndexFieldFilter> clinicalFieldFilters = new ArrayList<>();
        if (isValidParam(query, ANNOT_CLINICAL)) {
            annotationIndex |= CLINICAL_MASK;
            Values<String> sources = splitValues(query.getString(ANNOT_CLINICAL.key()));

            clinicalFieldFilters.add(schema.getClinicalIndexSchema().getSourceField().buildFilter(
                    new Values<>(sources.getOperation(), sources.mapValues(s -> new OpValue<>("=", Collections.singletonList(s))))));
        }
        if (isValidParam(query, ANNOT_CLINICAL_SIGNIFICANCE) || query.getBoolean(ANNOT_CLINICAL_CONFIRMED_STATUS.key())) {
            annotationIndex |= CLINICAL_MASK;
            List<List<String>> clinicalLists = VariantQueryParser.parseClinicalCombination(query, true);

            clinicalFieldFilters.add(schema.getClinicalIndexSchema().getClinicalSignificanceField()
                    .buildFilter(new Values<>(QueryOperation.AND, clinicalLists.stream()
                            .map(l -> new OpValue<>("=", l)).collect(Collectors.toList()))));
        }
        if (!clinicalFieldFilters.isEmpty()) {
            clinicalFilter = schema.getClinicalIndexSchema().buildFilter(clinicalFieldFilters, QueryOperation.AND);

            boolean clinicalCovered = clinicalFilter.isExactFilter();
            if (!clinicalCovered) {
                // Not all values are covered by the index. Unable to filter using this index, as it may return less values than required.
                clinicalFilter = schema.getClinicalIndexSchema().noOpFilter();
            }
            if (completeIndex && clinicalCovered) {
                query.remove(ANNOT_CLINICAL.key());
                query.remove(ANNOT_CLINICAL_CONFIRMED_STATUS.key());
                query.remove(ANNOT_CLINICAL_SIGNIFICANCE.key());
            }
        }

        IndexFilter populationFrequencyFilter = schema.getPopFreqIndex().noOpFilter();
        // TODO: This will skip filters ANNOT_POPULATION_REFERENCE_FREQUENCY and ANNOT_POPULATION_MINNOR_ALLELE_FREQUENCY
        if (isValidParam(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY)) {
            List<IndexFieldFilter> populationFrequencyFilters = new ArrayList<>();
            QueryOperation popFreqOp;
            boolean popFreqPartial = false;
            ParsedQuery<String> popFreqFilter = VariantQueryUtils.splitValue(query, VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY);
            popFreqOp = popFreqFilter.getOperation();

            Set<String> studyPops = new HashSet<>();
            Set<String> popFreqLessThan001 = new HashSet<>();
            List<String> filtersNotCoveredByPopFreqQuery = new ArrayList<>(popFreqFilter.getValues().size());

            for (String popFreq : popFreqFilter) {
                KeyOpValue<String, String> keyOpValue = VariantQueryUtils.parseKeyOpValue(popFreq);
                String studyPop = keyOpValue.getKey();
                studyPops.add(studyPop);
                double freqFilter = Double.parseDouble(keyOpValue.getValue());
                if (keyOpValue.getOp().equals("<") || keyOpValue.getOp().equals("<<")) {
                    if (freqFilter <= POP_FREQ_THRESHOLD_001) {
                        popFreqLessThan001.add(studyPop);
                    }
                }

                boolean populationInSampleIndex = false;
                boolean populationFilterFullyCovered = false;
                IndexField<Double> popFreqField = schema.getPopFreqIndex().getField(studyPop);
                if (popFreqField != null) {
                    populationInSampleIndex = true;
                    IndexFieldFilter fieldFilter = popFreqField.buildFilter(new OpValue<>(keyOpValue.getOp(), freqFilter));
                    populationFrequencyFilters.add(fieldFilter);
                    populationFilterFullyCovered = fieldFilter.isExactFilter();
                }

                if (!populationInSampleIndex) {
                    // If there is any populationFrequency from the query not in the SampleIndex, mark as partial
                    popFreqPartial = true;
                    filtersNotCoveredByPopFreqQuery.add(popFreq);
                } else if (!populationFilterFullyCovered) {
                    filtersNotCoveredByPopFreqQuery.add(popFreq);
                }
            }
            if (QueryOperation.OR.equals(popFreqOp)) {
                // Should use summary popFreq mask?
                if (POP_FREQ_ANY_001_SET.containsAll(popFreqLessThan001) && studyPops.equals(popFreqLessThan001)) {

                    annotationIndex |= POP_FREQ_ANY_001_MASK;

                    if (POP_FREQ_ANY_001_SET.size() == popFreqFilter.getValues().size()) {
                        // Do not filter using the PopFreq index, as the summary bit covers the filter
                        populationFrequencyFilters.clear();

                        // If the index is complete for all samples, remove the filter from main query
                        if (completeIndex) {
                            query.remove(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
                        }
                    }
                }
                if (popFreqPartial) {
                    // Can not use the index with partial OR queries.
                    populationFrequencyFilters.clear();
                } else if (filtersNotCoveredByPopFreqQuery.isEmpty()) {
                    // If all filters are covered, remove filter form query.
                    if (completeIndex) {
                        query.remove(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
                    }
                }

            } else {
                popFreqOp = QueryOperation.AND; // it could be null
                // With AND, the query MUST contain ANY popFreq
                for (String s : POP_FREQ_ANY_001_SET) {
                    if (popFreqLessThan001.contains(s)) {
                        annotationIndex |= POP_FREQ_ANY_001_MASK;
                        break;
                    }
                }
                if (completeIndex) {
                    if (filtersNotCoveredByPopFreqQuery.isEmpty()) {
                        query.remove(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
                    } else {
                        query.put(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                                String.join(popFreqOp.separator(), filtersNotCoveredByPopFreqQuery));
                    }
                }
            }
            if (!populationFrequencyFilters.isEmpty()) {
                populationFrequencyFilter = schema.getPopFreqIndex().buildFilter(populationFrequencyFilters, popFreqOp);
            }
        }

        byte annotationIndexMask = annotationIndex;
        if (intergenic != null) {
            annotationIndexMask |= INTERGENIC_MASK;
            if (intergenic) {
                annotationIndex |= INTERGENIC_MASK;
            }
        }

        if (intergenic == null || intergenic) {
            // If intergenic is undefined, or true, CT and BT filters can not be used.
            biotypeFilter = schema.getBiotypeIndex().getField().noOpFilter();
            consequenceTypeFilter = schema.getCtIndex().getField().noOpFilter();
        }

        return new SampleAnnotationIndexQuery(new byte[]{annotationIndexMask, annotationIndex},
                consequenceTypeFilter, biotypeFilter, tfFilter, ctBtTfFilter, clinicalFilter, populationFrequencyFilter);
    }

    private boolean simpleCombination(BiotypeConsquenceTypeFlagCombination combination) {
        return combination.numParams() == 1;
    }

    @Deprecated
    protected RangeQuery getRangeQuery(String op, double value, double[] thresholds, double min, double max) {
        double[] range = RangeIndexFieldFilter.queryRange(op, value, min, max);
        return getRangeQuery(range, thresholds, min, max);
    }

    @Deprecated
    private RangeQuery getRangeQuery(double[] range, double[] thresholds, double min, double max) {
        byte[] rangeCode = RangeIndexFieldFilter.getRangeCodes(range, thresholds);
        boolean exactQuery;
        if (rangeCode[0] == 0) {
            if (rangeCode[1] - 1 == thresholds.length) {
                exactQuery = RangeIndexField.equalsTo(range[0], min) && RangeIndexField.equalsTo(range[1], max);
            } else {
                exactQuery = RangeIndexField.equalsTo(range[1], thresholds[rangeCode[1] - 1])
                        && RangeIndexField.equalsTo(range[0], min);
            }
        } else if (rangeCode[1] - 1 == thresholds.length) {
            exactQuery = RangeIndexField.equalsTo(range[0], thresholds[rangeCode[0] - 1])
                    && RangeIndexField.equalsTo(range[1], max);
        } else {
            exactQuery = false;
        }
        return new RangeQuery(
                range[0],
                range[1],
                rangeCode[0],
                rangeCode[1],
                exactQuery
        );
    }

    private static List<String> getAllLoadedGenotypes(StudyMetadata studyMetadata) {
        List<String> allGts = studyMetadata
                .getAttributes()
                .getAsStringList(VariantStorageOptions.LOADED_GENOTYPES.key());
        if (allGts == null || allGts.isEmpty()) {
            allGts = DEFAULT_LOADED_GENOTYPES;
        }
        return allGts;
    }

}
