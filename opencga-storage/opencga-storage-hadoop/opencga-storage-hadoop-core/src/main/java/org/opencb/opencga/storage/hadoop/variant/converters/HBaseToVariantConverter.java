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
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
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

    protected final HBaseToVariantAnnotationConverter annotationConverter;
    protected final HBaseToStudyEntryConverter studyEntryConverter;
    protected final Logger logger = LoggerFactory.getLogger(HBaseToVariantConverter.class);

    protected static boolean failOnWrongVariants = false; //FIXME
    protected HBaseVariantConverterConfiguration configuration;

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
     * @param studyMetadata study metadata
     * @return  List of fixed formats
     */
    public static List<String> getFixedFormat(StudyMetadata studyMetadata) {
        List<String> format;
        List<String> extraFields = studyMetadata.getAttributes().getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key());
        if (extraFields.isEmpty()) {
            extraFields = Collections.singletonList(VariantMerger.GENOTYPE_FILTER_KEY);
        }

        boolean excludeGenotypes = studyMetadata.getAttributes()
                .getBoolean(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), VariantStorageOptions.EXCLUDE_GENOTYPES.defaultValue());

        if (excludeGenotypes) {
            format = new ArrayList<>(extraFields);
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

    public HBaseToVariantConverter<T> configure(HBaseVariantConverterConfiguration configuration) {
        this.configuration = configuration;
        studyEntryConverter.configure(configuration);
        if (configuration.getProjection() != null) {
            annotationConverter.setIncludeFields(configuration.getProjection().getFields());
        }
        annotationConverter.setIncludeIndexStatus(configuration.getIncludeIndexStatus());
        return this;
    }

    public HBaseToVariantConverter<T> configure(Configuration configuration) {
        return configure(HBaseVariantConverterConfiguration.builder(configuration).build());
    }

    public static boolean isFailOnWrongVariants() {
        return failOnWrongVariants;
    }

    public static void setFailOnWrongVariants(boolean b) {
        failOnWrongVariants = b;
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
                    String id = fileEntry.getData().get(StudyEntry.VCF_ID);
                    if (id != null) {
                        names.add(id);
                    }
                }
            }
        }
        variant.setNames(new ArrayList<>(names));

        if (configuration.getFailOnEmptyVariants() && variant.getStudies().isEmpty()) {
            throw new IllegalStateException("No Studies registered for variant!!! " + variant);
        }
        return variant;
    }

    private void wrongVariant(String message) {
        if (configuration.getFailOnWrongVariants()) {
            throw new IllegalStateException(message);
        } else {
            logger.warn(message);
        }
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
                if (configuration.getProjection() != null && configuration.getProjection().getStudyIds().isEmpty()) {
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
