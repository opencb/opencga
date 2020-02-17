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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantFileHeader;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.tools.Converter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryFields;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.SEARCH_INDEX_LAST_TIMESTAMP;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory.extractVariantFromResultSet;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory.extractVariantFromVariantRowKey;

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
    protected final Logger logger = LoggerFactory.getLogger(HBaseToVariantConverter.class);

    protected static boolean failOnWrongVariants = false; //FIXME
    protected boolean failOnEmptyVariants = false;
    protected VariantQueryFields selectVariantElements;

    public HBaseToVariantConverter(VariantTableHelper variantTableHelper) throws IOException {
        this(new VariantStorageMetadataManager(new HBaseVariantStorageMetadataDBAdaptorFactory(variantTableHelper)));
    }

    public HBaseToVariantConverter(VariantStorageMetadataManager scm) {
        long ts = scm.getProjectMetadata().getAttributes().getLong(SEARCH_INDEX_LAST_TIMESTAMP.key());
        this.annotationConverter = new HBaseToVariantAnnotationConverter(ts)
                .setAnnotationIds(scm.getProjectMetadata().getAnnotation());
        HBaseToVariantStatsConverter statsConverter = new HBaseToVariantStatsConverter();
        this.studyEntryConverter = new HBaseToStudyEntryConverter(scm, statsConverter);
    }

    /**
     * Get fixed format for the VARCHAR ARRAY sample columns.
     * @param attributes study attributes
     * @return  List of fixed formats
     */
    public static List<String> getFixedFormat(ObjectMap attributes) {
        List<String> format;
        List<String> extraFields = attributes.getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key());
        if (extraFields.isEmpty()) {
            extraFields = Collections.singletonList(VariantMerger.GENOTYPE_FILTER_KEY);
        }

        boolean excludeGenotypes = attributes
                .getBoolean(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), VariantStorageOptions.EXCLUDE_GENOTYPES.defaultValue());

        if (excludeGenotypes) {
            format = extraFields;
        } else {
            format = new ArrayList<>(1 + extraFields.size());
            format.add(VariantMerger.GT_KEY);
            format.addAll(extraFields);
        }
        return format;
    }

    public static List<String> getFixedAttributes(StudyMetadata studyMetadata) {
        return getFixedAttributes(studyMetadata.getVariantHeader());
    }

    public static List<String> getFixedAttributes(VariantFileHeader variantHeader) {
        return variantHeader
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

    public HBaseToVariantConverter<T> setIncludeIndexStatus(boolean includeIndexStatus) {
        annotationConverter.setIncludeIndexStatus(includeIndexStatus);
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

    public HBaseToVariantConverter<T> setSelectVariantElements(VariantQueryFields selectVariantElements) {
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

    public static HBaseToVariantConverter<Result> fromResult(VariantStorageMetadataManager scm) {
        return new ResultToVariantConverter(scm);
    }

    public static HBaseToVariantConverter<ResultSet> fromResultSet(VariantTableHelper helper) throws IOException {
        return new ResultSetToVariantConverter(helper);
    }

    public static HBaseToVariantConverter<ResultSet> fromResultSet(VariantStorageMetadataManager scm) {
        return new ResultSetToVariantConverter(scm);
    }

    protected Variant convert(Variant variant, Map<Integer, StudyEntry> studies,
                              VariantAnnotation annotation) {

        for (StudyEntry studyEntry : studies.values()) {
            variant.addStudyEntry(studyEntry);
        }
        variant.setAnnotation(annotation);
        variant.setId(variant.toString());

        Set<String> names = new HashSet<>();
        for (StudyEntry studyEntry : studies.values()) {
            List<FileEntry> files = studyEntry.getFiles();
            if (files != null) {
                for (FileEntry fileEntry : files) {
                    String id = fileEntry.getAttributes().get(StudyEntry.VCF_ID);
                    if (id != null) {
                        names.add(id);
                    }
                }
            }
        }
        variant.setNames(new ArrayList<>(names));

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

        ResultSetToVariantConverter(VariantStorageMetadataManager scm) {
            super(scm);
        }

        @Override
        public Variant convert(ResultSet resultSet) {
            Variant variant = extractVariantFromResultSet(resultSet);
            try {
                String type = resultSet.getString(VariantPhoenixHelper.VariantColumn.TYPE.column());
                if (StringUtils.isNotBlank(type)) {
                    variant.setType(VariantType.valueOf(type));
                }

                VariantAnnotation annotation = annotationConverter.convert(resultSet);

                Map<Integer, StudyEntry> samplesData = studyEntryConverter.convert(resultSet);
                return convert(variant, samplesData, annotation);
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

        ResultToVariantConverter(VariantStorageMetadataManager scm) {
            super(scm);
        }

        @Override
        public Variant convert(Result result) {
            Variant variant = extractVariantFromVariantRowKey(result.getRow());
            try {
                Cell cell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixHelper.VariantColumn.TYPE.bytes());
                if (cell != null && cell.getValueLength() > 0) {
                    String string = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    variant.setType(VariantType.valueOf(string));
                }

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
