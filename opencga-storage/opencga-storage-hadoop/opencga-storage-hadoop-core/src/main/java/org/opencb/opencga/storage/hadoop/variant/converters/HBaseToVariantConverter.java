/*
 * Copyright 2015-2016 OpenCB
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

import com.google.common.collect.BiMap;
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
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
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

import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory.extractVariantFromVariantRowKey;

/**
 * Created on 20/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToVariantConverter implements Converter<Result, Variant> {

    private final StudyConfigurationManager scm;
    private final HBaseToVariantAnnotationConverter annotationConverter;
    private final HBaseToVariantStatsConverter statsConverter;
    private final GenomeHelper genomeHelper;
    private final QueryOptions scmOptions = new QueryOptions(StudyConfigurationManager.READ_ONLY, true)
            .append(StudyConfigurationManager.CACHED, true);
    private final Map<Integer, LinkedHashMap<String, Integer>> returnedSamplesPositionMap = new HashMap<>();
    private final Logger logger = LoggerFactory.getLogger(HBaseToVariantConverter.class);

    private List<String> returnedSamples = null;

    private static boolean failOnWrongVariants = false; //FIXME
    private boolean studyNameAsStudyId = false;
    private boolean mutableSamplesPosition = true;
    private boolean failOnEmptyVariants = false;
    private boolean simpleGenotypes = false;
    private Set<VariantField> variantFields = null;

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
    }

    public HBaseToVariantConverter setReturnedSamples(List<String> returnedSamples) {
        this.returnedSamples = returnedSamples;
        return this;
    }

    public HBaseToVariantConverter setReturnedFields(Set<VariantField> fields) {
        variantFields = fields;
        annotationConverter.setReturnedFields(fields);
        return this;
    }

    public HBaseToVariantConverter setStudyNameAsStudyId(boolean studyNameAsStudyId) {
        this.studyNameAsStudyId = studyNameAsStudyId;
        return this;
    }

    public HBaseToVariantConverter setMutableSamplesPosition(boolean mutableSamplesPosition) {
        this.mutableSamplesPosition = mutableSamplesPosition;
        return this;
    }

    public HBaseToVariantConverter setFailOnEmptyVariants(boolean failOnEmptyVariants) {
        this.failOnEmptyVariants = failOnEmptyVariants;
        return this;
    }

    public HBaseToVariantConverter setSimpleGenotypes(boolean simpleGenotypes) {
        this.simpleGenotypes = simpleGenotypes;
        return this;
    }

    @Override
    public Variant convert(Result result) {
        VariantAnnotation annotation = annotationConverter.convert(result);
        Map<Integer, Map<Integer, VariantStats>> stats = statsConverter.convert(result);
        return convert(extractVariantFromVariantRowKey(result.getRow()), VariantTableStudyRow.parse(result, genomeHelper),
                stats, annotation);
    }

    public Variant convert(ResultSet resultSet) throws SQLException {
        Variant variant = new Variant(resultSet.getString(VariantPhoenixHelper.VariantColumn.CHROMOSOME.column()),
                resultSet.getInt(VariantPhoenixHelper.VariantColumn.POSITION.column()),
                resultSet.getString(VariantPhoenixHelper.VariantColumn.REFERENCE.column()),
                resultSet.getString(VariantPhoenixHelper.VariantColumn.ALTERNATE.column())
        );
        String type = resultSet.getString(VariantPhoenixHelper.VariantColumn.TYPE.column());
        if (StringUtils.isNotBlank(type)) {
            variant.setType(VariantType.valueOf(type));
        }
        try {
            Map<Integer, Map<Integer, VariantStats>> stats = statsConverter.convert(resultSet);
            VariantAnnotation annotation = annotationConverter.convert(resultSet);
            return convert(variant, VariantTableStudyRow.parse(variant, resultSet, genomeHelper), stats, annotation);
        } catch (RuntimeException e) {
            logger.error("Fail to parse variant: " + variant);
            throw e;
        }
    }

    public Variant convert(VariantTableStudyRow row) {
        return convert(new Variant(row.getChromosome(), row.getPos(), row.getRef(), row.getAlt()),
                Collections.singletonList(row), Collections.emptyMap(), null);

    }

    protected Variant convert(Variant variant, List<VariantTableStudyRow> rows, Map<Integer, Map<Integer, VariantStats>> stats,
                              VariantAnnotation annotation) {
        if (annotation == null) {
            annotation = new VariantAnnotation();
            annotation.setConsequenceTypes(Collections.emptyList());
        }
        if (failOnEmptyVariants && rows.isEmpty()) {
            throw new IllegalStateException("No Row columns supplied for row " + variant);
        }
        for (VariantTableStudyRow row : rows) {
            Map<String, String> attributesMap = new HashMap<>();
            Integer studyId = row.getStudyId();
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

            List<String> format = Arrays.asList(VariantMerger.GT_KEY, VariantMerger.GENOTYPE_FILTER_KEY);
            int gtIdx = format.indexOf(VariantMerger.GT_KEY);
            int ftIdx = format.indexOf(VariantMerger.GENOTYPE_FILTER_KEY);

            int loadedSamplesSize = loadedSamples.size();
            calculatePassCallRates(row, attributesMap, loadedSamplesSize);

            Integer nSamples = returnedSamplesPosition.size();

            @SuppressWarnings ("unchecked")
            List<String>[] samplesDataArray = new List[nSamples];

            Set<Integer> sampleWithVariant = new HashSet<>();
            BiMap<Integer, String> mapSampleIds = studyConfiguration.getSampleIds().inverse();
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

            // Load Secondary Index
            List<VariantProto.AlternateCoordinate> s2cgt = row.getComplexVariant().getSecondaryAlternatesList();
            int secondaryAlternatesCount = row.getComplexVariant().getSecondaryAlternatesCount();
            List<AlternateCoordinate> secAltArr = new ArrayList<AlternateCoordinate>(secondaryAlternatesCount);
            if (secondaryAlternatesCount > 0) {
                for (VariantProto.AlternateCoordinate altcoord : s2cgt) {
                    VariantType vart = VariantType.valueOf(altcoord.getType().name());
                    String chr = StringUtils.isEmpty(altcoord.getChromosome()) ? variant.getChromosome() : altcoord.getChromosome();
                    Integer start = altcoord.getStart() == 0 ? variant.getStart() : altcoord.getStart();
                    Integer end = altcoord.getEnd() == 0 ? variant.getEnd() : altcoord.getEnd();
                    String reference = StringUtils.isEmpty(altcoord.getReference()) ? "" : altcoord.getReference();
                    String alternate = StringUtils.isEmpty(altcoord.getAlternate()) ? "" : altcoord.getAlternate();
                    AlternateCoordinate alt = new AlternateCoordinate(chr, start, end, reference, alternate, vart);
                    secAltArr.add(alt);
                }
            }
            // Load complex genotypes
            for (Entry<Integer, String> entry : row.getComplexVariant().getSampleToGenotype().entrySet()) {
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

            List<List<String>> samplesData = Arrays.asList(samplesDataArray);

            StudyEntry studyEntry;
            if (studyNameAsStudyId) {
                studyEntry = new StudyEntry(studyConfiguration.getStudyName());
            } else {
                studyEntry = new StudyEntry(Integer.toString(studyConfiguration.getStudyId()));
            }
            studyEntry.setSortedSamplesPosition(returnedSamplesPosition);
            studyEntry.setSamplesData(samplesData);
            studyEntry.setFormat(format);
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
        if (genotype.contains(",")) {
            return genotype.split(",")[0];
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
        Integer samplePosition = returnedSamplesPosition.get(sampleName);
        return samplePosition;
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
                    returnedSamples == null ? null : new LinkedHashSet<>(returnedSamples), StudyConfiguration::getIndexedSamples);
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
}
