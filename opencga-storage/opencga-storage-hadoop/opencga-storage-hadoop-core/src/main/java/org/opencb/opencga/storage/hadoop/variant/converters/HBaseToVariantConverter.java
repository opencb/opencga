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
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory.extractVariantFromResult;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory.extractVariantFromResultSet;

/**
 * Created on 20/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class HBaseToVariantConverter<T> implements Converter<T, Variant> {

    protected final HBaseToVariantAnnotationConverter annotationConverter;
    protected final HBaseToStudyEntryConverter studyEntryConverter;
    protected final Logger logger = LoggerFactory.getLogger(HBaseToVariantConverter.class);

    protected HBaseVariantConverterConfiguration configuration;

    public HBaseToVariantConverter(VariantStorageMetadataManager scm) {
        this.annotationConverter = new HBaseToVariantAnnotationConverter()
                .setAnnotationIds(scm.getProjectMetadata().getAnnotation());
        HBaseToVariantStatsConverter statsConverter = new HBaseToVariantStatsConverter();
        this.studyEntryConverter = new HBaseToStudyEntryConverter(scm, statsConverter);
    }

    public HBaseToVariantConverter<T> configure(HBaseVariantConverterConfiguration configuration) {
        this.configuration = configuration;
        studyEntryConverter.configure(configuration);
        if (configuration.getProjection() != null) {
            annotationConverter.setIncludeFields(configuration.getProjection().getFields());
        }
        if (configuration.getSearchIndexCreationTs() >= 0) {
            annotationConverter.setIncludeIndexStatus(configuration.getSearchIndexCreationTs(), configuration.getSearchIndexUpdateTs());
        }
        return this;
    }

    public HBaseToVariantConverter<T> configure(Configuration configuration) {
        return configure(HBaseVariantConverterConfiguration.builder(configuration).build());
    }

    public static HBaseToVariantConverter<Result> fromResult(VariantStorageMetadataManager scm) {
        return new ResultToVariantConverter(scm);
    }

    public static HBaseToVariantConverter<ResultSet> fromResultSet(VariantStorageMetadataManager scm) {
        return new ResultSetToVariantConverter(scm);
    }

    public static HBaseToVariantConverter<VariantRow> fromVariantRow(VariantStorageMetadataManager vsmm, Set<String> scanColumns) {
        return new VariantRowToVariantConverter(vsmm, scanColumns);
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

    private static class ResultSetToVariantConverter extends HBaseToVariantConverter<ResultSet> {
        ResultSetToVariantConverter(VariantStorageMetadataManager scm) {
            super(scm);
        }

        @Override
        public Variant convert(ResultSet resultSet) {
            Variant variant = extractVariantFromResultSet(resultSet);
            try {
                String type = resultSet.getString(VariantPhoenixSchema.VariantColumn.TYPE.column());
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
        ResultToVariantConverter(VariantStorageMetadataManager scm) {
            super(scm);
        }

        @Override
        public Variant convert(Result result) {
            Variant variant = extractVariantFromResult(result);
            try {
                Cell cell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.TYPE.bytes());
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

    private static class VariantRowToVariantConverter extends HBaseToVariantConverter<VariantRow> {
        private final Set<String> scanColumns;

        VariantRowToVariantConverter(VariantStorageMetadataManager scm, Set<String> scanColumns) {
            super(scm);
            this.scanColumns = scanColumns;
        }

        @Override
        public Variant convert(VariantRow variantRow) {
            if (scanColumns != null) {
                variantRow = variantRow.withColumnsFilter(scanColumns);
            }
            Variant variant = variantRow.getVariant();
            try {
                variant.setType(variantRow.getType());
                VariantAnnotation annotation = variantRow.getVariantAnnotation(annotationConverter);
                Map<Integer, StudyEntry> studies;
                if (configuration.getProjection() != null && configuration.getProjection().getStudyIds().isEmpty()) {
                    studies = Collections.emptyMap();
                } else {
                    studies = studyEntryConverter.convert(variantRow);
                }
                return convert(variant, studies, annotation);
            } catch (RuntimeException e) {
                throw new IllegalStateException("Fail to parse variant: " + variant, e);
            }
        }
    }
}
