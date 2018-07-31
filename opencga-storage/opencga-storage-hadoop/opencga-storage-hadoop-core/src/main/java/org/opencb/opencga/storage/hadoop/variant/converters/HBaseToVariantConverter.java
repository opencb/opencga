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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.tools.Converter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
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

    public static final String MUTABLE_SAMPLES_POSITION = "mutableSamplesPosition";
    public static final String STUDY_NAME_AS_STUDY_ID = "studyNameAsStudyId";
    public static final String SIMPLE_GENOTYPES = "simpleGenotypes";

    protected final HBaseToVariantAnnotationConverter annotationConverter;
    protected final HBaseToStudyEntryConverter studyEntryConverter;
    protected final GenomeHelper genomeHelper;
    protected final Logger logger = LoggerFactory.getLogger(HBaseToVariantConverter.class);

    protected static boolean failOnWrongVariants = false; //FIXME
    protected boolean failOnEmptyVariants = false;
    protected VariantQueryUtils.SelectVariantElements selectVariantElements;

    public HBaseToVariantConverter(VariantTableHelper variantTableHelper) throws IOException {
        this(variantTableHelper, new StudyConfigurationManager(new HBaseVariantStorageMetadataDBAdaptorFactory(variantTableHelper)));
    }

    public HBaseToVariantConverter(GenomeHelper genomeHelper, StudyConfigurationManager scm) {
        this.genomeHelper = genomeHelper;
        this.annotationConverter = new HBaseToVariantAnnotationConverter(genomeHelper);
        HBaseToVariantStatsConverter statsConverter = new HBaseToVariantStatsConverter(genomeHelper);
        this.studyEntryConverter = new HBaseToStudyEntryConverter(genomeHelper.getColumnFamily(), scm, statsConverter);
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

    public HBaseToVariantConverter<T> setIncludeFields(Set<VariantField> fields) {
        annotationConverter.setIncludeFields(fields);
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
        this.selectVariantElements = selectVariantElements;
        studyEntryConverter.setSelectVariantElements(selectVariantElements);
        annotationConverter.setIncludeFields(selectVariantElements.getFields());
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

    protected Variant convert(Variant variant, Map<Integer, StudyEntry> studies,
                              VariantAnnotation annotation) {

        for (StudyEntry studyEntry : studies.values()) {
            variant.addStudyEntry(studyEntry);
        }
        variant.setAnnotation(annotation);
        if (annotation != null && StringUtils.isNotEmpty(annotation.getId())) {
            variant.setId(annotation.getId());
        } else {
            variant.setId(variant.toString());
        }
        if (failOnEmptyVariants && variant.getStudies().isEmpty()) {
            throw new IllegalStateException("No Studies registered for variant!!! " + variant);
        }
        return variant;
    }

    private void wrongVariant(String message) {
        if (failOnWrongVariants) {
            throw new IllegalStateException(message);
        } else {
            logger.warn(message);
        }
    }

    public HBaseToVariantConverter<T> configure(Configuration configuration) {
        if (StringUtils.isNotEmpty(configuration.get(MUTABLE_SAMPLES_POSITION))) {
            setMutableSamplesPosition(configuration.getBoolean(MUTABLE_SAMPLES_POSITION, true));
        }
        if (StringUtils.isNotEmpty(configuration.get(STUDY_NAME_AS_STUDY_ID))) {
            setStudyNameAsStudyId(configuration.getBoolean(STUDY_NAME_AS_STUDY_ID, false));
        }
        if (StringUtils.isNotEmpty(configuration.get(SIMPLE_GENOTYPES))) {
            setSimpleGenotypes(configuration.getBoolean(SIMPLE_GENOTYPES, false));
        }
        setUnknownGenotype(configuration.get(VariantQueryParam.UNKNOWN_GENOTYPE.key()));

//                .setSelectVariantElements(select)
//                .setFormats(formats)

        return this;
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
            String chromosome = null;
            Integer start = null;
            String reference = null;
            String alternate = null;
            try {
                chromosome = resultSet.getString(VariantPhoenixHelper.VariantColumn.CHROMOSOME.column());
                start = resultSet.getInt(VariantPhoenixHelper.VariantColumn.POSITION.column());
                reference = resultSet.getString(VariantPhoenixHelper.VariantColumn.REFERENCE.column());
                alternate = resultSet.getString(VariantPhoenixHelper.VariantColumn.ALTERNATE.column());
                Variant variant = new Variant(chromosome, start, reference, alternate);
                String type = resultSet.getString(VariantPhoenixHelper.VariantColumn.TYPE.column());
                if (StringUtils.isNotBlank(type)) {
                    variant.setType(VariantType.valueOf(type));
                }

                VariantAnnotation annotation = annotationConverter.convert(resultSet);

                Map<Integer, StudyEntry> samplesData = studyEntryConverter.convert(resultSet);
                return convert(variant, samplesData, annotation);
            } catch (RuntimeException | SQLException e) {
                logger.error("Fail to parse variant: " + chromosome + ':' + start + ':' + reference + ':' + alternate);
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
            Variant variant = extractVariantFromVariantRowKey(result.getRow());
            try {
                VariantAnnotation annotation = annotationConverter.convert(result);
                Map<Integer, StudyEntry> studies;
                if (selectVariantElements != null && selectVariantElements.getStudies().isEmpty()) {
                    studies = Collections.emptyMap();
                } else {
                    studies = studyEntryConverter.convert(result);
                }
                return convert(variant, studies, annotation);
            } catch (RuntimeException e) {
                throw new IllegalStateException("Fail to parse variant: " + variant, e);
            }
        }
    }
}
