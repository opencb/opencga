package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationConstants;
import org.opencb.opencga.storage.core.variant.query.*;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.core.RangeIndexField;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.NoOpIndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.RangeIndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.family.GenotypeCodec;
import org.opencb.opencga.storage.hadoop.variant.index.query.*;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery.PopulationFrequencyQuery;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration.PopulationFrequencyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser.DEFAULT_LOADED_GENOTYPES;
import static org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter.*;

/**
 * Created by jacobo on 06/01/19.
 */
public class SampleIndexQueryParser {
    private static Logger logger = LoggerFactory.getLogger(SampleIndexQueryParser.class);
    private final SampleIndexSchema schema;
    private final VariantStorageMetadataManager metadataManager;

    public SampleIndexQueryParser(VariantStorageMetadataManager metadataManager, SampleIndexSchema schema) {
        this.schema = schema;
        this.metadataManager = metadataManager;
    }

    /**
     * Determine if a given query can be used to query with the SampleIndex.
     * @param query Query
     * @return      if the query is valid
     */
    public static boolean validSampleIndexQuery(Query query) {
        ParsedVariantQuery.VariantQueryXref xref = VariantQueryParser.parseXrefs(query);
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
        // TODO: Accept variant IDs?

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
        // Map from all samples to query to its list of genotypes.
        Map<String, List<String>> samplesMap = new HashMap<>();
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
        boolean onlyDeNovo = false;

        // Extract sample and genotypes to filter
        if (isValidParam(query, GENOTYPE)) {
            // Get samples with non negated genotypes

            Map<Object, List<String>> map = new HashMap<>();
            Map<String, SampleMetadata> allSamples = new HashMap<>();
            queryOperation = parseGenotypeFilter(query.getString(GENOTYPE.key()), map);

            // Extract parents from each sample
            Map<String, List<String>> gtMap = new HashMap<>();
            Map<String, List<String>> parentsMap = new HashMap<>();
            for (Map.Entry<Object, List<String>> entry : map.entrySet()) {
                Object sample = entry.getKey();
                Integer sampleId = metadataManager.getSampleId(studyId, sample);

                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
                allSamples.put(sampleMetadata.getName(), sampleMetadata);

                List<String> gts = GenotypeClass.filter(entry.getValue(), allGenotypes);
                if (gts.stream().allMatch(SampleIndexSchema::validGenotype)) {
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
                } else {
                    negatedSamples.add(sampleMetadata.getName());
                }
                if (VariantStorageEngine.SplitData.MULTI.equals(sampleMetadata.getSplitData())) {
                    multiFileSamples.add(sampleMetadata.getName());
                }

                gtMap.put(sampleMetadata.getName(), gts);
            }

            // Determine which samples are parents, and which are children
            Set<String> childrenSet = findChildren(gtMap, queryOperation, parentsMap);
            Set<String> parentsSet = new HashSet<>();
            for (String child : childrenSet) {
                // may add null values
                parentsSet.addAll(parentsMap.get(child));
            }

            boolean partialGtIndex = false;
            for (Map.Entry<String, List<String>> entry : gtMap.entrySet()) {
                String sampleName = entry.getKey();
                if (queryOperation != QueryOperation.OR && parentsSet.contains(sampleName) && !childrenSet.contains(sampleName)) {
                    // We can skip parents, as their genotype filter will be tested in the child
                    // Discard parents that are not children of another sample
                    // Parents filter can only be used when intersecting (AND) with child
                    logger.debug("Discard parent {}", sampleName);
                    parentsInQuery.add(sampleName);

                    // Remove from negatedSamples (if present)
                    negatedSamples.remove(sampleName);

                    continue;
                }
                if (hasNegatedGenotypeFilter(queryOperation, entry.getValue())) {
                    // Discard samples with negated genotypes
                    negatedGenotypesSamples.add(sampleName);
                    partialIndex = true;
                    partialGtIndex = true;
                } else {
                    samplesMap.put(sampleName, entry.getValue());
                    if (queryOperation != QueryOperation.OR && childrenSet.contains(sampleName)) {
                        // Parents filter can only be used when intersecting (AND) with child
                        List<String> parents = parentsMap.get(sampleName);
                        String father = parents.get(0);
                        String mother = parents.get(1);

                        if (father != null) {
                            Integer fatherId = metadataManager.getSampleId(studyId, father);
                            boolean includeDiscrepancies = VariantStorageEngine.SplitData.MULTI
                                    .equals(metadataManager.getLoadSplitData(studyId, fatherId));
                            List<Integer> fatherFiles = allSamples.get(father).getFiles();
                            List<Integer> sampleFiles = allSamples.get(sampleName).getFiles();
                            boolean parentInSeparatedFile = fatherFiles.size() == sampleFiles.size() && fatherFiles.containsAll(sampleFiles);
                            boolean[] filter = buildParentGtFilter(gtMap.get(father), includeDiscrepancies, parentInSeparatedFile);
                            if (!isFullyCoveredParentFilter(filter)) {
                                partialGtIndex = true;
                            }
                            fatherFilterMap.put(sampleName, filter);
                        }
                        if (mother != null) {
                            Integer motherId = metadataManager.getSampleId(studyId, mother);
                            boolean includeDiscrepancies = VariantStorageEngine.SplitData.MULTI
                                    .equals(metadataManager.getLoadSplitData(studyId, motherId));
                            List<Integer> motherFiles = allSamples.get(mother).getFiles();
                            List<Integer> sampleFiles = allSamples.get(sampleName).getFiles();
                            boolean parentInSeparatedFile = motherFiles.size() == sampleFiles.size() && motherFiles.containsAll(sampleFiles);
                            boolean[] filter = buildParentGtFilter(gtMap.get(mother), includeDiscrepancies, parentInSeparatedFile);
                            if (!isFullyCoveredParentFilter(filter)) {
                                partialGtIndex = true;
                            }
                            motherFilterMap.put(sampleName, filter);
                        }
                    }
                }
                // If not all genotypes are valid, query is not covered
                if (!negatedSamples.isEmpty()) {
                    partialGtIndex = true;
                }
            }

            for (String negatedSample : negatedSamples) {
                List<String> negatedGenotypes = new ArrayList<>(validGenotypes);
                negatedGenotypes.removeAll(samplesMap.get(negatedSample));
                samplesMap.put(negatedSample, negatedGenotypes);
            }

            if (!partialGtIndex) {
                // Do not remove genotypes list if FORMAT is present.
                if (!isValidParam(query, SAMPLE_DATA)) {
                    query.remove(GENOTYPE.key());
                }
            }
        } else if (isValidParam(query, SAMPLE)) {
            // Filter by all non negated samples
            String samplesStr = query.getString(SAMPLE.key());
            queryOperation = VariantQueryUtils.checkOperator(samplesStr);
            List<String> samples = VariantQueryUtils.splitValue(samplesStr, queryOperation);
            for (String s : samples) {
                if (!isNegated(s)) {
                    samplesMap.put(s, mainGenotypes);
                }
            }

            if (!isValidParam(query, SAMPLE_DATA)) {
                // Do not remove FORMAT
                query.remove(SAMPLE.key());
            }
        } else if (isValidParam(query, SAMPLE_MENDELIAN_ERROR)) {
            onlyDeNovo = false;
            ParsedQuery<String> mendelianError = splitValue(query, SAMPLE_MENDELIAN_ERROR);
            mendelianErrorSet = new HashSet<>(mendelianError.getValues());
            queryOperation = mendelianError.getOperation();
            for (String s : mendelianErrorSet) {
                // Return any genotype
                samplesMap.put(s, mainGenotypes);
            }
            query.remove(SAMPLE_MENDELIAN_ERROR.key());
            // Reading any MendelianError could return variants from GT=0/0, which is not annotated in the SampleIndex,
            // so the index is partial.
            partialIndex = true;
        } else if (isValidParam(query, SAMPLE_DE_NOVO)) {
            onlyDeNovo = true;
            ParsedQuery<String> sampleDeNovo = splitValue(query, SAMPLE_DE_NOVO);
            mendelianErrorSet = new HashSet<>(sampleDeNovo.getValues());
            queryOperation = sampleDeNovo.getOperation();
            for (String s : mendelianErrorSet) {
                // Return any genotype
                samplesMap.put(s, mainGenotypes);
            }
            query.remove(SAMPLE_DE_NOVO.key());
        //} else if (isValidParam(query, FILE)) {
            // Add FILEs filter ?
        } else {
            throw new IllegalStateException("Unable to query SamplesIndex");
        }

        boolean partialFilesIndex = false;
        if (!negatedGenotypesSamples.isEmpty() || !parentsInQuery.isEmpty()) {
            Set<Integer> sampleFiles = new HashSet<>(samplesMap.size());
            for (String sample : samplesMap.keySet()) {
                Integer sampleId = metadataManager.getSampleId(studyId, sample);
                sampleFiles.addAll(metadataManager.getSampleMetadata(studyId, sampleId).getFiles());
            }

            // If the file of any other sample is not between the files of the samples in the query, mark as partial
            for (String sample : negatedGenotypesSamples) {
                Integer sampleId = metadataManager.getSampleId(studyId, sample);
                for (Integer file : metadataManager.getSampleMetadata(studyId, sampleId).getFiles()) {
                    if (!sampleFiles.contains(file)) {
                        partialFilesIndex = true;
                        break;
                    }
                }
            }
            // If the file of any parent is not between the files of the samples in the query, mark as partial
            for (String sample : parentsInQuery) {
                Integer sampleId = metadataManager.getSampleId(studyId, sample);
                for (Integer file : metadataManager.getSampleMetadata(studyId, sampleId).getFiles()) {
                    if (!sampleFiles.contains(file)) {
                        partialFilesIndex = true;
                        break;
                    }
                }
            }

        }
        Map<String, Values<SampleFileIndexQuery>> fileIndexMap = new HashMap<>(samplesMap.size());
        for (String sample : samplesMap.keySet()) {
            Values<SampleFileIndexQuery> fileIndexQuery =
                    parseFilesQuery(query, studyId, sample, multiFileSamples.contains(sample), partialFilesIndex);

            fileIndexMap.put(sample, fileIndexQuery);
        }

        int sampleIndexVersion = defaultStudy.getSampleIndexConfigurationLatest().getVersion();
        boolean allSamplesAnnotated = true;
        if (negatedGenotypesSamples.isEmpty()) {
            for (String sample : samplesMap.keySet()) {
                Integer sampleId = metadataManager.getSampleId(studyId, sample);
                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
                if (!SampleIndexDBAdaptor.getSampleIndexAnnotationStatus(sampleMetadata, sampleIndexVersion)
                        .equals(TaskMetadata.Status.READY)) {
                    allSamplesAnnotated = false;
                    break;
                }
            }
        } else {
            allSamplesAnnotated = false;
        }

        boolean completeIndex = allSamplesAnnotated && !partialIndex;
        SampleAnnotationIndexQuery annotationIndexQuery = parseAnnotationIndexQuery(query, completeIndex);
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

        // Extract regions
        List<Region> regions = new ArrayList<>();
        if (isValidParam(query, REGION)) {
            regions.addAll(Region.parseRegions(query.getString(REGION.key()), true));
            query.remove(REGION.key());
        }

        if (isValidParam(query, ANNOT_GENE_REGIONS)) {
            regions.addAll(Region.parseRegions(query.getString(ANNOT_GENE_REGIONS.key()), true));
            if (isValidParam(query, ANNOT_CONSEQUENCE_TYPE) || isValidParam(query, ANNOT_BIOTYPE)) {
                query.put(ANNOT_GENE_REGIONS.key(), SKIP_GENE_REGIONS);
            } else {
                query.remove(ANNOT_GENE_REGIONS.key());
                query.remove(GENE.key());
            }
        }
        List<List<Region>> regionGroups = groupRegions(regions);

        return new SampleIndexQuery(schema, regionGroups, variantTypes, study, samplesMap, multiFileSamples, negatedSamples,
                fatherFilterMap, motherFilterMap,
                fileIndexMap, annotationIndexQuery, mendelianErrorSet, onlyDeNovo, queryOperation);
    }

    /**
     * Merge and group regions by chunk start.
     * Concurrent groups of regions are merged into one single group.
     * Resulting groups are sorted using {@link VariantQueryUtils#REGION_COMPARATOR}.
     * Each group will be translated to a SCAN.
     *
     * @param regions List of regions to group
     * @return Grouped regions
     */
    public static List<List<Region>> groupRegions(List<Region> regions) {
        regions = mergeRegions(regions);
        List<List<Region>> regionGroups = new ArrayList<>(regions
                .stream()
                .collect(Collectors.groupingBy(r -> r.getChromosome() + "_" + SampleIndexSchema.getChunkStart(r.getStart())))
                .values());

        if (!regionGroups.isEmpty()) {
            regionGroups.forEach(l -> l.sort(REGION_COMPARATOR));
            regionGroups.sort(Comparator.comparing(l -> l.get(0), REGION_COMPARATOR));

            //Merge consecutive groups
            Iterator<List<Region>> iterator = regionGroups.iterator();
            List<Region> prevGroup = iterator.next();
            while (iterator.hasNext()) {
                List<Region> group = iterator.next();
                Region prevRegion = prevGroup.get(prevGroup.size() - 1);
                Region region = group.get(0);
                // Merge if the distance between groups is less than 1 batch size
                // TODO: This rule could be changed to reduce the number of small queries, even if more data is fetched.
                if (region.getChromosome().equals(prevRegion.getChromosome())
                        && Math.abs(region.getStart() - prevRegion.getEnd()) < SampleIndexSchema.BATCH_SIZE) {
                    // Merge groups
                    prevGroup.addAll(group);
                    iterator.remove();
                } else {
                    prevGroup = group;
                }
            }
        }
        return regionGroups;
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
     * @param gtMap Genotype filter map
     * @param queryOperation Query operation
     * @param parentsMap Parents map
     * @return Set with all children from the query
     */
    protected Set<String> findChildren(Map<String, List<String>> gtMap, QueryOperation queryOperation,
                                              Map<String, List<String>> parentsMap) {
        Set<String> childrenSet = new HashSet<>(parentsMap.size());
        for (Map.Entry<String, List<String>> entry : parentsMap.entrySet()) {
            String child = entry.getKey();
            List<String> parents = entry.getValue();

            if (hasNegatedGenotypeFilter(queryOperation, gtMap.get(child))) {
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

    protected static boolean[] buildParentGtFilter(List<String> parentGts, boolean includeDiscrepancies, boolean parentInSeparatedFile) {
        boolean[] filter = new boolean[GenotypeCodec.NUM_CODES]; // all false by default
        for (String gt : parentGts) {
            filter[GenotypeCodec.encode(gt)] = true;
        }
        if (includeDiscrepancies) {
            filter[GenotypeCodec.DISCREPANCY_SIMPLE] = true;
            filter[GenotypeCodec.DISCREPANCY_ANY] = true;
        }
        if (!parentInSeparatedFile) {
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


    protected Values<SampleFileIndexQuery> parseFilesQuery(Query query, int studyId, String sample,
                                                         boolean multiFileSample, boolean partialFilesIndex) {
        return parseFilesQuery(query, sample, multiFileSample, partialFilesIndex, s -> {
            Integer sampleId = metadataManager.getSampleId(studyId, s);
            List<Integer> fileIds = metadataManager.getFileIdsFromSampleId(studyId, sampleId);
            List<String> fileNames = new ArrayList<>(fileIds.size());
            for (Integer fileId : fileIds) {
                fileNames.add(metadataManager.getFileName(studyId, fileId));
            }
            return fileNames;
        });
    }

    protected Values<SampleFileIndexQuery> parseFilesQuery(Query query, String sample, boolean multiFileSample, boolean partialFilesIndex,
                                                         Function<String, List<String>> filesFromSample) {
        ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> fileDataParsedQuery = parseFileData(query);
        List<String> filesFromFileData = fileDataParsedQuery.getValues(KeyValues::getKey);

        boolean splitFileDataQuery = false;
        if (fileDataParsedQuery.getOperation() == QueryOperation.AND) {
            splitFileDataQuery = true;
        } else if (fileDataParsedQuery.getOperation() == QueryOperation.OR && multiFileSample) {
            List<String> files = filesFromSample.apply(sample);
            if (files.containsAll(filesFromFileData)) {
                // All samples from the query are from the same sample (aka multi-query sample)
                splitFileDataQuery = true;
            }
        }
        if (splitFileDataQuery) {
            List<SampleFileIndexQuery> fileIndexQueries = new ArrayList<>(filesFromFileData.size());
            boolean fileDataCovered = true;
            for (String fileFromFileData : filesFromFileData) {
                Query subQuery = new Query(query);
//                    subQuery.remove(FILE.key());
                subQuery.put(FILE_DATA.key(), fileDataParsedQuery.getValue(kv -> kv.getKey().equals(fileFromFileData)).toQuery());
                fileIndexQueries.add(parseFileQuery(subQuery, sample, multiFileSample, partialFilesIndex, filesFromSample));
                if (isValidParam(subQuery, FILE_DATA)) {
                    // This subquery did not remove the fileData, so it's not fully covered. Can't remove the fileData filter.
                    fileDataCovered = false;
                }
            }
            if (fileDataCovered && !partialFilesIndex) {
                query.remove(FILE_DATA.key());
            }
            return new Values<>(fileDataParsedQuery.getOperation(), fileIndexQueries);
        }
        return new Values<>(null,
                Collections.singletonList(parseFileQuery(query, sample, multiFileSample, partialFilesIndex, filesFromSample)));
    }

    protected SampleFileIndexQuery parseFileQuery(Query query, String sample, boolean multiFileSample, boolean partialFilesIndex,
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
            List<String> filesFromQuery;
            if (isValidParam(query, FILE)) {
                ParsedQuery<String> filesQuery = splitValue(query, FILE);
                filesFromQuery = filesQuery.getValues();
            } else if (isValidParam(query, FILE_DATA)) {
                ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> fileData = parseFileData(query);
                filesFromQuery = fileData.getValues(KeyValues::getKey);
            } else {
                filesFromQuery = null;
            }
            if (filesFromQuery != null) {
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
                            IndexFieldFilter indexFieldFilter = fileDataIndexField.buildFilter(keyOpValue);
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

    protected SampleAnnotationIndexQuery parseAnnotationIndexQuery(Query query) {
        return parseAnnotationIndexQuery(query, false);
    }

    /**
     * Builds the SampleAnnotationIndexQuery given a VariantQuery.
     *
     * @param query Input VariantQuery. If the index is complete, covered filters could be removed from here.
     * @param completeIndex Indicates if the annotation index is complete for the samples in the query.
     *                      Otherwise, the index can only be used as a hint, and should be completed with further filtering.
     * @return SampleAnnotationIndexQuery
     */
    protected SampleAnnotationIndexQuery parseAnnotationIndexQuery(Query query, boolean completeIndex) {
        byte annotationIndex = 0;
        byte biotypeMask = 0;
        short consequenceTypeMask = 0;
        byte clinicalMask = 0;

        Boolean intergenic = null;

        if (!isValidParam(query, REGION)) {
            ParsedVariantQuery.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);
            if (!variantQueryXref.getGenes().isEmpty()
                    && variantQueryXref.getIds().isEmpty()
                    && variantQueryXref.getOtherXrefs().isEmpty()
                    && variantQueryXref.getVariants().isEmpty()) {
                // If only filtering by genes, is not intergenic.
                intergenic = false;
            }
        }

        BiotypeConsquenceTypeFlagCombination combination = BiotypeConsquenceTypeFlagCombination.fromQuery(query);
        boolean btCovered = false;
        boolean ctCovered = false;

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
            }
            boolean ctFilterCoveredBySummary = false;
            boolean ctBtCombinationCoveredBySummary = false;
            if (LOF_SET.containsAll(soNames)) {
                ctFilterCoveredBySummary = soNames.size() == LOF_SET.size();
                annotationIndex |= LOF_MASK;
                // If all present, remove consequenceType filter
                if (completeIndex && LOF_SET.size() == soNames.size()) {
                    // Ensure not filtering by gene, and not combining with other params
                    if (!isValidParam(query, GENE) && simpleCombination(combination)) {
                        query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                    }
                }
            }
            if (LOF_EXTENDED_SET.containsAll(soNames)) {
                boolean proteinCodingOnly = query.getString(ANNOT_BIOTYPE.key()).equals(VariantAnnotationConstants.PROTEIN_CODING);
                ctFilterCoveredBySummary = soNames.size() == LOF_EXTENDED_SET.size();
                annotationIndex |= LOF_EXTENDED_MASK;
                // If all present, remove consequenceType filter
                if (LOF_EXTENDED_SET.size() == soNames.size()) {
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
                    annotationIndex |= LOFE_PROTEIN_CODING_MASK;
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
            boolean useCtIndexFilter = !ctFilterCoveredBySummary || (!ctBtCombinationCoveredBySummary && combination.isBiotype());
            if (useCtIndexFilter) {
                ctCovered = completeIndex;
                for (String soName : soNames) {
                    short mask = getMaskFromSoName(soName);
                    if (mask == IndexUtils.EMPTY_MASK) {
                        // If any element is not in the index, do not use this filter
                        consequenceTypeMask = IndexUtils.EMPTY_MASK;
                        ctCovered = false;
                        break;
                    }
                    consequenceTypeMask |= mask;
                    // Some CT filter values are not precise, so the query is not covered.
                    ctCovered &= !isImpreciseCtMask(mask);
                }
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

            boolean useBtIndexFilter = !biotypeFilterCoveredBySummary || combination.isConsequenceType();
            if (useBtIndexFilter) {
                btCovered = completeIndex;
                for (String biotype : biotypes) {
                    byte mask = getMaskFromBiotype(biotype);
                    if (mask == IndexUtils.EMPTY_MASK) {
                        // If any element is not in the index, do not use this filter
                        biotypeMask = IndexUtils.EMPTY_MASK;
                        btCovered = false;
                        break;
                    }
                    biotypeMask |= mask;
                    // Some CT filter values are not precise, so the query is not covered.
                    btCovered &= !isImpreciseBtMask(mask);
                }
                // Biotype filter is covered by index
                if (btCovered) {
                    if (!isValidParam(query, GENE) && simpleCombination(combination)) {
                        query.remove(ANNOT_BIOTYPE.key());
                    }
                }
            }
        }
        if (completeIndex && btCovered && ctCovered && !isValidParam(query, GENE)
                && combination.equals(BiotypeConsquenceTypeFlagCombination.BIOTYPE_CT)) {
            query.remove(ANNOT_BIOTYPE.key());
            query.remove(ANNOT_CONSEQUENCE_TYPE.key());
        }

        // If filter by proteinSubstitution, without filter << or >>, add ProteinCodingMask
        String proteinSubstitution = query.getString(ANNOT_PROTEIN_SUBSTITUTION.key());
        if (StringUtils.isNotEmpty(proteinSubstitution)
                && !proteinSubstitution.contains("<<")
                && !proteinSubstitution.contains(">>")) {
            annotationIndex |= LOF_EXTENDED_MASK;
        }

        if (isValidParam(query, ANNOT_CLINICAL_SIGNIFICANCE)) {
            annotationIndex |= CLINICAL_MASK;
            boolean clinicalCovered = true;
            for (String clinical : query.getAsStringList(ANNOT_CLINICAL_SIGNIFICANCE.key())) {
                switch (ClinicalSignificance.valueOf(clinical)) {
                    case likely_benign:
                    case benign:
                        // These two values are covered by the same bit, so, they are not covered.
                        clinicalCovered = false;
                        clinicalMask |= CLINICAL_BENIGN_LIKELY_BENIGN_MASK;
                        break;
                    case VUS:
                    case uncertain_significance:
                        // These two values are synonymous
                        clinicalMask |= CLINICAL_UNCERTAIN_SIGNIFICANCE_MASK;
                        break;
                    case likely_pathogenic:
                        clinicalMask |= CLINICAL_LIKELY_PATHOGENIC_MASK;
                        break;
                    case pathogenic:
                        clinicalMask |= CLINICAL_PATHOGENIC_MASK;
                        break;
                    default:
                        clinicalCovered = false;
                        break;
                }
            }
            if (completeIndex && clinicalCovered) {
                query.remove(ANNOT_CLINICAL_SIGNIFICANCE.key());
            }
            if (!clinicalCovered) {
                // Not all values are covered by the index. Unable to filter using this index, as it may return less values than required.
                clinicalMask = 0;
            }
        }

        List<PopulationFrequencyQuery> popFreqQuery = new ArrayList<>();
        QueryOperation popFreqOp = QueryOperation.AND;
        boolean popFreqPartial = false;
        // TODO: This will skip filters ANNOT_POPULATION_REFERENCE_FREQUENCY and ANNOT_POPULATION_MINNOR_ALLELE_FREQUENCY
        if (isValidParam(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY)) {
            ParsedQuery<String> popFreqFilter = VariantQueryUtils.splitValue(query, VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY);
            popFreqOp = popFreqFilter.getOperation();

            Set<String> studyPops = new HashSet<>();
            Set<String> popFreqLessThan001 = new HashSet<>();
            List<String> filtersNotCoveredByPopFreqQuery = new ArrayList<>(popFreqFilter.getValues().size());

            for (String popFreq : popFreqFilter) {
                KeyOpValue<String, String> keyOpValue = VariantQueryUtils.parseKeyOpValue(popFreq);
                String studyPop = keyOpValue.getKey();
                studyPops.add(studyPop);
                double freqFilter = Double.valueOf(keyOpValue.getValue());
                if (keyOpValue.getOp().equals("<") || keyOpValue.getOp().equals("<<")) {
                    if (freqFilter <= POP_FREQ_THRESHOLD_001) {
                        popFreqLessThan001.add(studyPop);
                    }
                }

                boolean populationInSampleIndex = false;
                boolean populationFilterFullyCovered = false;
                int popFreqIdx = 0;
                for (PopulationFrequencyRange populationRange : schema.getConfiguration().getPopulationRanges()) {
                    if (populationRange.getKey().equals(studyPop)) {
                        populationInSampleIndex = true;
                        RangeQuery rangeQuery = getRangeQuery(keyOpValue.getOp(), freqFilter, populationRange.getThresholds(),
                                0, 1 + RangeIndexField.DELTA);

                        popFreqQuery.add(new PopulationFrequencyQuery(rangeQuery,
                                popFreqIdx, populationRange.getStudy(),
                                populationRange.getPopulation()));
                        populationFilterFullyCovered |= rangeQuery.isExactQuery();
                    }
                    popFreqIdx++;
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
                        popFreqQuery.clear();

                        // If the index is complete for all samples, remove the filter from main query
                        if (completeIndex) {
                            query.remove(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
                        }
                    }
                }
                if (popFreqPartial) {
                    // Can not use the index with partial OR queries.
                    popFreqQuery.clear();
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
            consequenceTypeMask = IndexUtils.EMPTY_MASK;
            biotypeMask = IndexUtils.EMPTY_MASK;
        }


        return new SampleAnnotationIndexQuery(new byte[]{annotationIndexMask, annotationIndex}, consequenceTypeMask, biotypeMask,
                clinicalMask, popFreqOp, popFreqQuery, popFreqPartial);
    }

    private boolean simpleCombination(BiotypeConsquenceTypeFlagCombination combination) {
        return combination.equals(BiotypeConsquenceTypeFlagCombination.BIOTYPE)
                || combination.equals(BiotypeConsquenceTypeFlagCombination.CT);
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
