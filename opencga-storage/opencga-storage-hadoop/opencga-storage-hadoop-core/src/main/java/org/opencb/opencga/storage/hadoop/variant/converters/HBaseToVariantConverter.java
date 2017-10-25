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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.Converter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseStudyConfigurationDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory.extractVariantFromVariantRowKey;

/**
 * Created on 20/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class HBaseToVariantConverter<T> implements Converter<T, Variant> {

    protected final HBaseToVariantAnnotationConverter annotationConverter;
    protected final HBaseToVariantStatsConverter statsConverter;
    protected final HBaseToStudyEntryConverter studyEntryConverter;
    protected final GenomeHelper genomeHelper;
    protected final Logger logger = LoggerFactory.getLogger(HBaseToVariantConverter.class);

    protected static boolean failOnWrongVariants = false; //FIXME
    protected boolean failOnEmptyVariants = false;

    public HBaseToVariantConverter(VariantTableHelper variantTableHelper) throws IOException {
        this(variantTableHelper, new StudyConfigurationManager(
                new HBaseStudyConfigurationDBAdaptor(
                        variantTableHelper.getAnalysisTableAsString(), variantTableHelper.getConf(), new ObjectMap())));
    }

    public HBaseToVariantConverter(GenomeHelper genomeHelper, StudyConfigurationManager scm) {
        this.genomeHelper = genomeHelper;
        this.annotationConverter = new HBaseToVariantAnnotationConverter(genomeHelper);
        this.statsConverter = new HBaseToVariantStatsConverter(genomeHelper);
        this.studyEntryConverter = new HBaseToStudyEntryConverter(genomeHelper, scm);
    }

    /**
     * Get fixed format for the VARCHAR ARRAY sample columns.
     * @param studyConfiguration    StudyConfiguration
     * @return  List of fixed formats
     */
    public static List<String> getFixedFormat(StudyConfiguration studyConfiguration) {
        List<String> format;
        List<String> extraFields = studyConfiguration.getAttributes().getAsStringList(Options.EXTRA_GENOTYPE_FIELDS.key());
        if (extraFields.isEmpty()) {
            extraFields = Collections.singletonList(VariantMerger.GENOTYPE_FILTER_KEY);
        }

        boolean excludeGenotypes = studyConfiguration.getAttributes()
                .getBoolean(Options.EXCLUDE_GENOTYPES.key(), Options.EXCLUDE_GENOTYPES.defaultValue());

        if (excludeGenotypes) {
            format = extraFields;
        } else {
            format = new ArrayList<>(1 + extraFields.size());
            format.add(VariantMerger.GT_KEY);
            format.addAll(extraFields);
        }
        return format;
    }

    public static List<String> getFixedAttributes(StudyConfiguration studyConfiguration) {
        return studyConfiguration.getVariantHeader()
                .getComplexLines()
                .stream()
                .filter(line -> line.getKey().equalsIgnoreCase("INFO"))
                .map(VariantFileHeaderComplexLine::getId)
                .collect(Collectors.toList());
    }

    public HBaseToVariantConverter<T> setReturnedFields(Set<VariantField> fields) {
        annotationConverter.setReturnedFields(fields);
        return this;
    }

    public HBaseToVariantConverter<T> setStudyNameAsStudyId(boolean studyNameAsStudyId) {
        studyEntryConverter.setStudyNameAsStudyId(studyNameAsStudyId);
        return this;
    }

    public HBaseToVariantConverter<T> setMutableSamplesPosition(boolean mutableSamplesPosition) {
        studyEntryConverter.setMutableSamplesPosition(mutableSamplesPosition);
        return this;
    }

    public HBaseToVariantConverter<T> setFailOnEmptyVariants(boolean failOnEmptyVariants) {
        studyEntryConverter.setFailOnWrongVariants(failOnEmptyVariants);
        return this;
    }

    public HBaseToVariantConverter<T> setSimpleGenotypes(boolean simpleGenotypes) {
        studyEntryConverter.setSimpleGenotypes(simpleGenotypes);
        return this;
    }

    public HBaseToVariantConverter<T> setUnknownGenotype(String unknownGenotype) {
        studyEntryConverter.setUnknownGenotype(unknownGenotype);
        return this;
    }

    public HBaseToVariantConverter<T> setSelectVariantElements(VariantQueryUtils.SelectVariantElements selectVariantElements) {
        studyEntryConverter.setSelectVariantElements(selectVariantElements);
        annotationConverter.setReturnedFields(selectVariantElements.getFields());
        return this;
    }

    public static boolean isFailOnWrongVariants() {
        return failOnWrongVariants;
    }

    public static void setFailOnWrongVariants(boolean b) {
        failOnWrongVariants = b;
    }


    /**
     * Format of the converted variants. Discard other values.
     * @see org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils#getIncludeFormats
     * @param formats Formats for converted variants
     * @return this
     */
    public HBaseToVariantConverter<T> setFormats(List<String> formats) {
        studyEntryConverter.setFormats(formats);
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

    protected Variant convert(Variant variant, Map<Integer, StudyEntry> studies, Map<Integer, Map<Integer, VariantStats>> stats,
                              VariantAnnotation annotation) {
        if (annotation == null) {
            annotation = new VariantAnnotation();
            annotation.setConsequenceTypes(Collections.emptyList());
        }

        for (Integer studyId : studies.keySet()) {
            StudyEntry studyEntry = studies.get(studyId);
            stats.getOrDefault(studyId, Collections.emptyMap()).forEach((cohortId, variantStats) -> {
                studyEntry.setStats(cohortId.toString(), variantStats);
            });
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

    private void wrongVariant(String message) {
        if (failOnWrongVariants) {
            throw new IllegalStateException(message);
        } else {
            logger.warn(message);
        }
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
            Variant variant = new Variant(row.getChromosome(), row.getPos(), row.getRef(), row.getAlt());
            StudyEntry studyEntry = studyEntryConverter.convert(variant, row);
            return convert(variant, Collections.singletonMap(row.getStudyId(), studyEntry), Collections.emptyMap(), null);
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

                Map<Integer, StudyEntry> samplesData = studyEntryConverter.convert(resultSet);
                return convert(variant, samplesData, stats, annotation);
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
            Map<Integer, StudyEntry> studies = studyEntryConverter.convert(result);
            return convert(extractVariantFromVariantRowKey(result.getRow()), studies, stats, annotation);
        }
    }
}
