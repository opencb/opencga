/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.hadoop.variant.converters;

import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.converters.Converter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.metadata.VariantStudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.samples.HBaseToSamplesDataConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseStudyConfigurationDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.SampleList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory.extractVariantFromVariantRowKey;

/**
 * Created on 20/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class HBaseToVariantConverter<T> implements Converter<T, Variant> {

    private static final String UNKNOWN_GENOTYPE = "?/?";
    protected final StudyConfigurationManager scm;
    protected final HBaseToVariantAnnotationConverter annotationConverter;
    protected final HBaseToVariantStatsConverter statsConverter;
    protected final HBaseToSamplesDataConverter samplesDataConverter;
    protected final GenomeHelper genomeHelper;
    protected final QueryOptions scmOptions = new QueryOptions(StudyConfigurationManager.READ_ONLY, true)
            .append(StudyConfigurationManager.CACHED, true);
    protected final Map<Integer, LinkedHashMap<String, Integer>> returnedSamplesPositionMap = new HashMap<>();
    protected final Logger logger = LoggerFactory.getLogger(HBaseToVariantConverter.class);

    protected List<String> returnedSamples = null;

    protected static boolean failOnWrongVariants = false; //FIXME
    protected boolean studyNameAsStudyId = false;
    protected boolean mutableSamplesPosition = true;
    protected boolean failOnEmptyVariants = false;
    protected boolean simpleGenotypes = false;
    protected boolean readFullSamplesData = true;
    protected Set<VariantField> variantFields = null;
    protected String unknownGenotype = UNKNOWN_GENOTYPE;
    protected List<String> expectedFormat;

    public HBaseToVariantConverter(VariantTableHelper variantTableHelper) throws IOException {
        this(variantTableHelper, new StudyConfigurationManager(
                new HBaseStudyConfigurationDBAdaptor(
                        variantTableHelper.getAnalysisTableAsString(), variantTableHelper.getConf(), new ObjectMap())));
    }

    public HBaseToVariantConverter(GenomeHelper genomeHelper, StudyConfigurationManager scm) {
        this.genomeHelper = genomeHelper;
        this.scm = scm;
        this.annotationConverter = new HBaseToVariantAnnotationConverter(genomeHelper);
        this.statsConverter = new HBaseToVariantStatsConverter(genomeHelper);
        this.samplesDataConverter = new HBaseToSamplesDataConverter(genomeHelper);
    }

    public static List<String> getFormat(StudyConfiguration studyConfiguration) {
        List<String> format;
        List<String> extraFields = studyConfiguration.getAttributes().getAsStringList(Options.EXTRA_GENOTYPE_FIELDS.key());
        if (extraFields.isEmpty()) {
            extraFields = Collections.singletonList(VariantMerger.GENOTYPE_FILTER_KEY);
        }

        // TODO: Allow exclude genotypes! Read from configuration
        boolean excludeGenotypes = false;
//        boolean excludeGenotypes = getStudyConfiguration().getAttributes()
//                .getBoolean(Options.EXCLUDE_GENOTYPES.key(), Options.EXCLUDE_GENOTYPES.defaultValue());

        if (excludeGenotypes) {
            format = extraFields;
        } else {
            format = new ArrayList<>(1 + extraFields.size());
            format.add(VariantMerger.GT_KEY);
            format.addAll(extraFields);
        }
        return format;
    }

    public HBaseToVariantConverter<T> setReturnedSamples(List<String> returnedSamples) {
        this.returnedSamples = returnedSamples;
        return this;
    }

    public HBaseToVariantConverter<T> setReturnedFields(Set<VariantField> fields) {
        variantFields = fields;
        annotationConverter.setReturnedFields(fields);
        return this;
    }

    public HBaseToVariantConverter<T> setStudyNameAsStudyId(boolean studyNameAsStudyId) {
        this.studyNameAsStudyId = studyNameAsStudyId;
        return this;
    }

    public HBaseToVariantConverter<T> setMutableSamplesPosition(boolean mutableSamplesPosition) {
        this.mutableSamplesPosition = mutableSamplesPosition;
        return this;
    }

    public HBaseToVariantConverter<T> setFailOnEmptyVariants(boolean failOnEmptyVariants) {
        this.failOnEmptyVariants = failOnEmptyVariants;
        return this;
    }

    public HBaseToVariantConverter<T> setSimpleGenotypes(boolean simpleGenotypes) {
        this.simpleGenotypes = simpleGenotypes;
        return this;
    }

    public HBaseToVariantConverter<T> setReadFullSamplesData(boolean readFullSamplesData) {
        this.readFullSamplesData = readFullSamplesData;
        return this;
    }

    public HBaseToVariantConverter<T> setUnknownGenotype(String unknownGenotype) {
        if (StringUtils.isEmpty(unknownGenotype)) {
            this.unknownGenotype = UNKNOWN_GENOTYPE;
        } else {
            this.unknownGenotype = unknownGenotype;
        }
        return this;
    }

    /**
     * Format of the converted variants. Discard other values.
     * @see org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils#getIncludeFormats
     * @param formats Formats for converted variants
     * @return this
     */
    public HBaseToVariantConverter<T> setFormats(List<String> formats) {
        this.expectedFormat = formats;
        return this;
    }

    public static HBaseToVariantConverter<Result> fromResult(VariantTableHelper helper) throws IOException {
        return new ResultToVariantConverter(helper);
    }

    public static HBaseToVariantConverter<Result> fromResult(GenomeHelper genomeHelper, StudyConfigurationManager scm) {
        return new ResultToVariantConverter(genomeHelper, scm);
    }

    public static HBaseToVariantConverter<ResultSet> fromResultSet(VariantTableHelper helper) throws IOException {
        return new ResultSetToVariantConverter(helper);
    }

    public static HBaseToVariantConverter<ResultSet> fromResultSet(GenomeHelper genomeHelper, StudyConfigurationManager scm) {
        return new ResultSetToVariantConverter(genomeHelper, scm);
    }

    public static HBaseToVariantConverter<VariantTableStudyRow> fromRow(VariantTableHelper helper) throws IOException {
        return new VariantTableStudyRowToVariantConverter(helper);
    }

    public static HBaseToVariantConverter<VariantTableStudyRow> fromRow(GenomeHelper genomeHelper, StudyConfigurationManager scm) {
        return new VariantTableStudyRowToVariantConverter(genomeHelper, scm);
    }

    protected Variant convert(T object, Variant variant,
                              Collection<Integer> studyIds, Map<Integer, VariantTableStudyRow> rowsMap,
                              Map<Integer, Map<Integer, List<String>>> fullSamplesData,
                              Map<Integer, Map<Integer, VariantStats>> stats, VariantAnnotation annotation) {
        if (annotation == null) {
            annotation = new VariantAnnotation();
            annotation.setConsequenceTypes(Collections.emptyList());
        }

        if (!rowsMap.isEmpty()) {
            studyIds = rowsMap.keySet();
        } else if (fullSamplesData != null && !fullSamplesData.isEmpty()) {
            studyIds = fullSamplesData.keySet();
        } else if (failOnEmptyVariants) {
            throw new IllegalStateException("No Studies supplied for row " + variant);
        } else {
            studyIds = Collections.emptySet();
        }

        for (Integer studyId : studyIds) {
            Map<String, String> attributesMap = new HashMap<>();
            QueryResult<StudyConfiguration> queryResult = scm.getStudyConfiguration(studyId, scmOptions);
            if (queryResult.getResult().isEmpty()) {
                throw new IllegalStateException("No study found for study ID: " + studyId);
            }
            StudyConfiguration studyConfiguration = queryResult.first();

            LinkedHashMap<String, Integer> returnedSamplesPosition = getReturnedSamplesPosition(studyConfiguration);
            if (mutableSamplesPosition) {
                returnedSamplesPosition = new LinkedHashMap<>(returnedSamplesPosition);
            }
//            Do not throw any exception. It may happen that the study is not loaded yet or no samples are required!
//            if (returnedSamplesPosition.isEmpty()) {
//                throw new IllegalStateException("No samples found for study!!!");
//            }

            BiMap<String, Integer> loadedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration);

            List<String> format;
            List<String> expectedFormat;
            if (fullSamplesData == null) {
                // Only GT and FT available.
                format = Arrays.asList(VariantMerger.GT_KEY, VariantMerger.GENOTYPE_FILTER_KEY);
            } else {
                format = getFormat(studyConfiguration);
            }

            int[] formatsMap;
            if (this.expectedFormat != null && !this.expectedFormat.equals(format)) {
                expectedFormat = this.expectedFormat;
                formatsMap = new int[expectedFormat.size()];
                for (int i = 0; i < expectedFormat.size(); i++) {
                    formatsMap[i] = format.indexOf(expectedFormat.get(i));
                }
            } else {
                formatsMap = null;
                expectedFormat = format;
            }

            int gtIdx = format.indexOf(VariantMerger.GT_KEY);
            int ftIdx = format.indexOf(VariantMerger.GENOTYPE_FILTER_KEY);

            int loadedSamplesSize = loadedSamples.size();

            // Load Secondary Index
            List<AlternateCoordinate> secAltArr = getAlternateCoordinates(variant, studyId,
                    studyConfiguration.getVariantMetadata(), format, rowsMap, fullSamplesData);

            Integer nSamples = returnedSamplesPosition.size();

            @SuppressWarnings("unchecked")
            List<String>[] samplesDataArray = new List[nSamples];
            BiMap<Integer, String> mapSampleIds = studyConfiguration.getSampleIds().inverse();

            // Read values from sample columns
            if (fullSamplesData != null) {
                Map<Integer, List<String>> studySampleData = fullSamplesData.getOrDefault(studyId, Collections.emptyMap());
                for (Entry<Integer, List<String>> entry : studySampleData.entrySet()) {
                    Integer sampleId = entry.getKey();
                    List<String> sampleData = entry.getValue();
                    String sampleName = mapSampleIds.get(sampleId);

                    if (sampleData.size() != format.size()) {
                        throw new IllegalStateException();
                    }
                    Integer samplePosition = returnedSamplesPosition.get(sampleName);
                    if (samplePosition != null) {
                        if (simpleGenotypes && gtIdx >= 0) {
                            String simpleGenotype = getSimpleGenotype(sampleData.get(gtIdx));
                            sampleData.set(gtIdx, simpleGenotype);
                        }
                        if (formatsMap != null) {
                            List<String> filteredSampleData = new ArrayList<>(formatsMap.length);
                            for (int i : formatsMap) {
                                if (i < 0) {
                                    filteredSampleData.add(VCFConstants.MISSING_VALUE_v4);
                                } else {
                                    filteredSampleData.add(sampleData.get(i));
                                }
                            }
                            sampleData = filteredSampleData;
                        }
                        samplesDataArray[samplePosition] = sampleData;
                    } else {
                        logger.warn("ResultSet containing unwanted samples data returnedSamples: "
                                + returnedSamplesPosition + "  sample: " + sampleName + " id: " + sampleId);
                    }
                }
                List<String> defaultSampleData = new ArrayList<>(expectedFormat.size());
                for (String f : expectedFormat) {
                    if (f.equals(VariantMerger.GT_KEY)) {
                        String defaultGenotype = getDefaultGenotype(studyConfiguration);
                        defaultSampleData.add(defaultGenotype); // Read from default genotype
                    } else {
                        defaultSampleData.add(VCFConstants.MISSING_VALUE_v4);
                    }
                }
                for (int i = 0; i < samplesDataArray.length; i++) {
                    if (samplesDataArray[i] == null) {
                        samplesDataArray[i] = defaultSampleData;
                    }
                }
            } else {
                VariantTableStudyRow row = rowsMap.get(studyId);
                calculatePassCallRates(row, attributesMap, loadedSamplesSize);
                Set<Integer> sampleWithVariant = new HashSet<>();
                for (String genotype : row.getGenotypes()) {
                    sampleWithVariant.addAll(row.getSampleIds(genotype));
                    if (genotype.equals(VariantTableStudyRow.OTHER)) {
                        continue; // skip OTHER -> see Complex type
                    }
                    for (Integer sampleId : row.getSampleIds(genotype)) {
                        String sampleName = mapSampleIds.get(sampleId);
                        Integer sampleIdx = returnedSamplesPosition.get(sampleName);
                        if (sampleIdx == null) {
                            continue;   //Sample may not be required. Ignore this sample.
                        }
                        List<String> lst = Arrays.asList(genotype, VariantMerger.PASS_VALUE);
                        samplesDataArray[sampleIdx] = lst;
                    }
                }

                // Load complex genotypes
                for (Entry<Integer, String> entry : row.getComplexVariant().getSampleToGenotypeMap().entrySet()) {
                    sampleWithVariant.add(entry.getKey());
                    Integer samplePosition = getSamplePosition(returnedSamplesPosition, mapSampleIds, entry.getKey());
                    if (samplePosition == null) {
                        continue;   //Sample may not be required. Ignore this sample.
                    }
                    String genotype = entry.getValue();
                    String returnedGenotype;
                    // FIXME: Decide what to do with lists of genotypes
                    if (simpleGenotypes) {
                        returnedGenotype = getSimpleGenotype(genotype);
                        logger.debug("Return simplified genotype: {} -> {}", genotype, returnedGenotype);
                    } else {
                        returnedGenotype = genotype;
                    }
                    samplesDataArray[samplePosition] = Arrays.asList(returnedGenotype, VariantMerger.PASS_VALUE);
                }

                // Fill gaps (with HOM_REF)
                int gapCounter = 0;
                for (int i = 0; i < samplesDataArray.length; i++) {
                    if (samplesDataArray[i] == null) {
                        ++gapCounter;
                        samplesDataArray[i] = Arrays.asList(VariantTableStudyRow.HOM_REF, VariantMerger.PASS_VALUE);
                    }
                }

                // Check homRef count
                int homRefCount = loadedSamplesSize;
                homRefCount -= sampleWithVariant.size();
                if (homRefCount != row.getHomRefCount()) {
                    String message = "Wrong number of HomRef samples for variant " + variant + ". Got " + homRefCount + ", expect "
                            + row.getHomRefCount() + ". Samples number: " + samplesDataArray.length + " , ";
                    message += "'" + VariantTableStudyRow.HOM_REF + "':" + row.getHomRefCount() + " , ";
                    for (String studyColumn : VariantTableStudyRow.GENOTYPE_COLUMNS) {
                        message += "'" + studyColumn + "':" + row.getSampleIds(studyColumn) + " , ";
                    }
                    wrongVariant(message);
                }

                // Set pass field
                int passCount = loadedSamplesSize;
                for (Entry<String, SampleList> entry : row.getComplexFilter().getFilterNonPass().entrySet()) {
                    String filterString = entry.getKey();
                    passCount -= entry.getValue().getSampleIdsCount();
                    for (Integer id : entry.getValue().getSampleIdsList()) {
                        Integer samplePosition = getSamplePosition(returnedSamplesPosition, mapSampleIds, id);
                        if (samplePosition == null) {
                            continue; // Sample may not be required. Ignore this sample.
                        }
                        samplesDataArray[samplePosition].set(ftIdx, filterString);
                    }
                }

                // Check pass count
                if (passCount != row.getPassCount()) {
                    String message = String.format(
                            "Error parsing variant %s. Pass count %s does not match filter fill count: %s using %s loaded samples.",
                            row.toString(), row.getPassCount(), passCount, loadedSamplesSize);
                    wrongVariant(message);
                }
            }


            List<List<String>> samplesData = Arrays.asList(samplesDataArray);

            StudyEntry studyEntry;
            if (studyNameAsStudyId) {
                studyEntry = new StudyEntry(studyConfiguration.getStudyName());
            } else {
                studyEntry = new StudyEntry(Integer.toString(studyConfiguration.getStudyId()));
            }
            studyEntry.setSortedSamplesPosition(returnedSamplesPosition);
            studyEntry.setSamplesData(samplesData);
            studyEntry.setFormat(expectedFormat);
            studyEntry.setFiles(Collections.singletonList(new FileEntry("", "", attributesMap)));
            studyEntry.setSecondaryAlternates(secAltArr);

            Map<Integer, VariantStats> convertedStatsMap = stats.get(studyConfiguration.getStudyId());
            if (convertedStatsMap != null) {
                Map<String, VariantStats> statsMap = new HashMap<>(convertedStatsMap.size());
                for (Entry<Integer, VariantStats> entry : convertedStatsMap.entrySet()) {
                    String cohortName = studyConfiguration.getCohortIds().inverse().get(entry.getKey());
                    statsMap.put(cohortName, entry.getValue());
                }
                studyEntry.setStats(statsMap);
            }

            variant.addStudyEntry(studyEntry);
        }
        variant.setAnnotation(annotation);
        if (StringUtils.isNotEmpty(annotation.getId())) {
            variant.setId(annotation.getId());
        } else {
            variant.setId(variant.toString());
        }
        if (failOnEmptyVariants && variant.getStudies().isEmpty()) {
            throw new IllegalStateException("No Studies registered for variant!!! " + variant);
        }
        return variant;
    }

    private String getDefaultGenotype(StudyConfiguration studyConfiguration) {
        String defaultGenotype;
        if (VariantStorageEngine.MergeMode.from(studyConfiguration.getAttributes()).equals(VariantStorageEngine.MergeMode.ADVANCED)) {
            defaultGenotype = "0/0";
        } else {
            defaultGenotype = unknownGenotype;
        }
        return defaultGenotype;
    }

    private List<AlternateCoordinate> getAlternateCoordinates(Variant variant, Integer studyId, VariantStudyMetadata variantMetadata,
                                                              List<String> format, Map<Integer, VariantTableStudyRow> rowsMap,
                                                              Map<Integer, Map<Integer, List<String>>> fullSamplesData) {
        List<AlternateCoordinate> secAltArr;
        if (rowsMap.containsKey(studyId)) {
            VariantTableStudyRow row = rowsMap.get(studyId);
            secAltArr = getAlternateCoordinates(variant, row);
        } else {
            secAltArr = samplesDataConverter.extractSecondaryAlternates(variant, variantMetadata, format, fullSamplesData.get(studyId));
        }
        return secAltArr;
    }

    protected List<AlternateCoordinate> getAlternateCoordinates(Variant variant, VariantTableStudyRow row) {
        List<AlternateCoordinate> secAltArr;
        List<VariantProto.AlternateCoordinate> secondaryAlternates = row.getComplexVariant().getSecondaryAlternatesList();
        int secondaryAlternatesCount = row.getComplexVariant().getSecondaryAlternatesCount();
        secAltArr = new ArrayList<>(secondaryAlternatesCount);
        if (secondaryAlternatesCount > 0) {
            for (VariantProto.AlternateCoordinate altCoordinate : secondaryAlternates) {
                VariantType type = VariantType.valueOf(altCoordinate.getType().name());
                String chr = StringUtils.isEmpty(altCoordinate.getChromosome())
                        ? variant.getChromosome() : altCoordinate.getChromosome();
                Integer start = altCoordinate.getStart() == 0 ? variant.getStart() : altCoordinate.getStart();
                Integer end = altCoordinate.getEnd() == 0 ? variant.getEnd() : altCoordinate.getEnd();
                String reference = StringUtils.isEmpty(altCoordinate.getReference()) ? "" : altCoordinate.getReference();
                String alternate = StringUtils.isEmpty(altCoordinate.getAlternate()) ? "" : altCoordinate.getAlternate();
                AlternateCoordinate alt = new AlternateCoordinate(chr, start, end, reference, alternate, type);
                secAltArr.add(alt);
            }
        }
        return secAltArr;
    }

    private void calculatePassCallRates(VariantTableStudyRow row, Map<String, String> attributesMap, int
            loadedSamplesSize) {
        attributesMap.put("PASS", row.getPassCount().toString());
        attributesMap.put("CALL", row.getCallCount().toString());
        double passRate = row.getPassCount().doubleValue() / loadedSamplesSize;
        double callRate = row.getCallCount().doubleValue() / loadedSamplesSize;
        double opr = passRate * callRate;
        attributesMap.put("PR", String.valueOf(passRate));
        attributesMap.put("CR", String.valueOf(callRate));
        attributesMap.put("OPR", String.valueOf(opr)); // OVERALL pass rate
        attributesMap.put("NS", String.valueOf(loadedSamplesSize)); // Number of Samples
    }

    private String getSimpleGenotype(String genotype) {
        int idx = genotype.indexOf(',');
        if (idx > 0) {
            return genotype.substring(0, idx);
        } else {
            return genotype;
        }
    }

    private void wrongVariant(String message) {
        if (failOnWrongVariants) {
            throw new IllegalStateException(message);
        } else {
            logger.warn(message);
        }
    }

    private Integer getSamplePosition(LinkedHashMap<String, Integer> returnedSamplesPosition, BiMap<Integer, String> mapSampleIds,
                                      Integer sampleId) {
        String sampleName = mapSampleIds.get(sampleId);
        return returnedSamplesPosition.get(sampleName);
    }

    /**
     * Creates a SORTED MAP with the required samples position.
     *
     * @param studyConfiguration Study Configuration
     * @return Sorted linked hash map
     */
    private LinkedHashMap<String, Integer> getReturnedSamplesPosition(StudyConfiguration studyConfiguration) {
        if (!returnedSamplesPositionMap.containsKey(studyConfiguration.getStudyId())) {
            LinkedHashMap<String, Integer> samplesPosition = StudyConfiguration.getReturnedSamplesPosition(studyConfiguration,
                    returnedSamples == null ? null : new LinkedHashSet<>(returnedSamples), StudyConfiguration::getIndexedSamplesPosition);
            returnedSamplesPositionMap.put(studyConfiguration.getStudyId(), samplesPosition);
        }
        return returnedSamplesPositionMap.get(studyConfiguration.getStudyId());
    }


    public static boolean isFailOnWrongVariants() {
        return failOnWrongVariants;
    }

    public static void setFailOnWrongVariants(boolean b) {
        failOnWrongVariants = b;
    }

    private static class VariantTableStudyRowToVariantConverter extends HBaseToVariantConverter<VariantTableStudyRow> {
        VariantTableStudyRowToVariantConverter(VariantTableHelper helper) throws IOException {
            super(helper);
        }

        VariantTableStudyRowToVariantConverter(GenomeHelper genomeHelper, StudyConfigurationManager scm) {
            super(genomeHelper, scm);
        }

        @Override
        public Variant convert(VariantTableStudyRow row) {
            return convert(row, new Variant(row.getChromosome(), row.getPos(), row.getRef(), row.getAlt()),
                    Collections.singletonList(row.getStudyId()),
                    Collections.singletonMap(row.getStudyId(), row), null, Collections.emptyMap(), null);
        }
    }

    private static class ResultSetToVariantConverter extends HBaseToVariantConverter<ResultSet> {
        ResultSetToVariantConverter(VariantTableHelper helper) throws IOException {
            super(helper);
        }

        ResultSetToVariantConverter(GenomeHelper genomeHelper, StudyConfigurationManager scm) {
            super(genomeHelper, scm);
        }

        @Override
        public Variant convert(ResultSet resultSet) {
            Variant variant = null;
            try {
                variant = new Variant(resultSet.getString(VariantPhoenixHelper.VariantColumn.CHROMOSOME.column()),
                        resultSet.getInt(VariantPhoenixHelper.VariantColumn.POSITION.column()),
                        resultSet.getString(VariantPhoenixHelper.VariantColumn.REFERENCE.column()),
                        resultSet.getString(VariantPhoenixHelper.VariantColumn.ALTERNATE.column())
                );
                String type = resultSet.getString(VariantPhoenixHelper.VariantColumn.TYPE.column());
                if (StringUtils.isNotBlank(type)) {
                    variant.setType(VariantType.valueOf(type));
                }

                Map<Integer, Map<Integer, VariantStats>> stats = statsConverter.convert(resultSet);
                VariantAnnotation annotation = annotationConverter.convert(resultSet);
                Map<Integer, VariantTableStudyRow> variantTableStudyRows = VariantTableStudyRow.parse(variant, resultSet, genomeHelper)
                        .stream().collect(Collectors.toMap(VariantTableStudyRow::getStudyId, r -> r));
                Map<Integer, Map<Integer, List<String>>> samplesData = readFullSamplesData ? samplesDataConverter.convert(resultSet) : null;
                return convert(resultSet, variant, null, variantTableStudyRows, samplesData, stats, annotation);
            } catch (RuntimeException | SQLException e) {
                logger.error("Fail to parse variant: " + variant);
                throw Throwables.propagate(e);
            }
        }
    }

    private static class ResultToVariantConverter extends HBaseToVariantConverter<Result> {
        ResultToVariantConverter(VariantTableHelper helper) throws IOException {
            super(helper);
        }

        ResultToVariantConverter(GenomeHelper genomeHelper, StudyConfigurationManager scm) {
            super(genomeHelper, scm);
        }

        @Override
        public Variant convert(Result result) {
            VariantAnnotation annotation = annotationConverter.convert(result);
            Map<Integer, Map<Integer, VariantStats>> stats = statsConverter.convert(result);
            Map<Integer, Map<Integer, List<String>>> samplesData = readFullSamplesData ? samplesDataConverter.convert(result) : null;
            Map<Integer, VariantTableStudyRow> rows = VariantTableStudyRow.parse(result, genomeHelper)
                    .stream().collect(Collectors.toMap(VariantTableStudyRow::getStudyId, r -> r));
            return convert(result, extractVariantFromVariantRowKey(result.getRow()), null, rows, samplesData, stats, annotation);
        }
    }
}
