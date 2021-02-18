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

package org.opencb.opencga.storage.hadoop.variant.converters.annotation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.EthnicCategory;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantAnnotationMixin;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;

/**
 * Created on 03/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToVariantAnnotationConverter extends AbstractPhoenixConverter implements Converter<Result, VariantAnnotation> {

    private final ObjectMapper objectMapper;
    private final byte[] columnFamily;
    private final long ts;
    private byte[] annotationColumn = VariantPhoenixSchema.VariantColumn.FULL_ANNOTATION.bytes();
    private String annotationColumnStr = Bytes.toString(annotationColumn);
    private String defaultAnnotationId = null;
    private Map<Integer, String> annotationIds;
    private boolean includeIndexStatus;

    public HBaseToVariantAnnotationConverter() {
        this(-1);
    }

    public HBaseToVariantAnnotationConverter(long ts) {
        this(GenomeHelper.COLUMN_FAMILY_BYTES, ts);
    }

    public HBaseToVariantAnnotationConverter(byte[] columnFamily, long ts) {
        super(columnFamily);
        this.columnFamily = columnFamily;
        this.ts = ts;
        objectMapper = new ObjectMapper();
        objectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
    }

    public HBaseToVariantAnnotationConverter setAnnotationColumn(byte[] annotationColumn, String name) {
        this.annotationColumn = annotationColumn;
        annotationColumnStr = Bytes.toString(annotationColumn);
        if (name.equals(VariantAnnotationManager.CURRENT)) {
            // If CURRENT, check the annotation ID column
            defaultAnnotationId = null;
        } else {
            defaultAnnotationId = name;
        }
        return this;
    }

    public HBaseToVariantAnnotationConverter setAnnotationIds(ProjectMetadata.VariantAnnotationSets annotations) {
        Map<Integer, String> map = annotations.getSaved()
                .stream()
                .collect(Collectors.toMap(
                        ProjectMetadata.VariantAnnotationMetadata::getId,
                        ProjectMetadata.VariantAnnotationMetadata::getName));
        if (annotations.getCurrent() != null) {
            map.put(annotations.getCurrent().getId(), annotations.getCurrent().getName());
        }
        this.annotationIds = map;
        return this;
    }

    public HBaseToVariantAnnotationConverter setIncludeFields(Set<VariantField> allIncludeFields) {
        List<String> list = new ArrayList<>();
        if (allIncludeFields != null) {
            for (VariantField annotationField : VariantField.values()) {
                if (annotationField.getParent() == VariantField.ANNOTATION && !allIncludeFields.contains(annotationField)) {
                    list.add(annotationField.fieldName().replace(VariantField.ANNOTATION.fieldName() + '.', ""));
                }
            }
        }
        String[] excludedAnnotationFields = list.toArray(new String[list.size()]);
        objectMapper.setAnnotationIntrospector(
                new JacksonAnnotationIntrospector() {

                    @Override
                    public JsonIgnoreProperties.Value findPropertyIgnorals(Annotated ac) {
                        JsonIgnoreProperties.Value propertyIgnorals = super.findPropertyIgnorals(ac);
                        if (!ac.getRawType().equals(VariantAnnotation.class)) {
                            // Not a VariantAnnotation class. Return propertiesToIgnore as is.
                            return propertyIgnorals;
                        }
                        return JsonIgnoreProperties.Value.merge(propertyIgnorals,
                                JsonIgnoreProperties.Value.forIgnoredProperties(excludedAnnotationFields));
                    }

                    @Override
                    public String[] findPropertiesToIgnore(Annotated ac, boolean forSerialization) {
                        String[] propertiesToIgnore = super.findPropertiesToIgnore(ac, forSerialization);
                        if (!ac.getRawType().equals(VariantAnnotation.class)) {
                            // Not a VariantAnnotation class. Return propertiesToIgnore as is.
                            return propertiesToIgnore;
                        } else if (ArrayUtils.isNotEmpty(propertiesToIgnore)) {
                            // If there is any property to ignore, merge them
                            List<String> list = new ArrayList<>();
                            Collections.addAll(list, excludedAnnotationFields);
                            Collections.addAll(list, propertiesToIgnore);
                            return list.toArray(new String[list.size()]);
                        } else {
                            return excludedAnnotationFields;
                        }
                    }
                });
        return this;
    }

    public void setIncludeIndexStatus(boolean includeIndexStatus) {
        this.includeIndexStatus = includeIndexStatus;
    }

    @Override
    public VariantAnnotation convert(Result result) {
        VariantAnnotation variantAnnotation = null;

        Cell cell = result.getColumnLatestCell(columnFamily, annotationColumn);
        if (cell != null && cell.getValueLength() > 0) {
            byte[] valueArray = cell.getValueArray();
            int valueOffset = cell.getValueOffset();
            int valueLength = cell.getValueLength();
            variantAnnotation = convert(valueArray, valueOffset, valueLength);
        }
        List<Integer> releases = new ArrayList<>();
        for (byte[] bytes : result.getFamilyMap(columnFamily).tailMap(VariantPhoenixSchema.RELEASE_PREFIX_BYTES).keySet()) {
            if (Bytes.startsWith(bytes, VariantPhoenixSchema.RELEASE_PREFIX_BYTES)) {
                releases.add(getRelease(Bytes.toString(bytes)));
            }
        }

        String annotationId = this.defaultAnnotationId;
        if (defaultAnnotationId == null && annotationIds != null) {
            // Read the annotation Id from ANNOTATION_ID column
            byte[] annotationIdBytes = result.getFamilyMap(columnFamily).get(VariantPhoenixSchema.VariantColumn.ANNOTATION_ID.bytes());
            if (annotationIdBytes != null && annotationIdBytes.length > 0) {
                int annotationIdNum = ((Integer) PInteger.INSTANCE.toObject(annotationIdBytes));
                annotationId = annotationIds.get(annotationIdNum);
            }
        }

        Cell studyCell = result.getColumnLatestCell(columnFamily, VariantColumn.INDEX_STUDIES.bytes());
        Cell notSyncCell = result.getColumnLatestCell(columnFamily, VariantColumn.INDEX_NOT_SYNC.bytes());
        Cell unknownCell = result.getColumnLatestCell(columnFamily, VariantColumn.INDEX_UNKNOWN.bytes());

        List<Integer> studies;
        if (studyCell != null && studyCell.getValueLength() != 0) {
            studies = toList((PhoenixArray) VariantColumn.INDEX_STUDIES.getPDataType()
                    .toObject(studyCell.getValueArray(), studyCell.getValueOffset(), studyCell.getValueLength()));
        } else {
            studies = null;
        }

        VariantStorageEngine.SyncStatus syncStatus;
        if (includeIndexStatus) {
            syncStatus = HadoopVariantSearchIndexUtils.getSyncStatus(ts, studyCell, notSyncCell, unknownCell);
        } else {
            syncStatus = null;
        }
        return post(variantAnnotation, releases, syncStatus, studies, annotationId);
    }

    public VariantAnnotation convert(ResultSet resultSet) {
        VariantAnnotation variantAnnotation = null;

        Integer column = findColumn(resultSet, annotationColumnStr);
        if (column != null) {
            try {
                byte[] value = resultSet.getBytes(column);
                if (value != null) {
                    variantAnnotation = convert(value, 0, value.length);
                }
            } catch (SQLException e) {
                // This should never happen!
                throw new IllegalStateException(e);
            }
        }


        List<Integer> releases = new ArrayList<>();
        List<Integer> studies;
        VariantStorageEngine.SyncStatus syncStatus;
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            boolean hasIndex = false;
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                if (columnName.startsWith(VariantPhoenixSchema.RELEASE_PREFIX)) {
                    if (resultSet.getBoolean(i)) {
                        releases.add(getRelease(columnName));
                    }
                } else if (columnName.equals(VariantColumn.INDEX_NOT_SYNC.column())) {
                    hasIndex = true;
                }
            }

            if (includeIndexStatus && hasIndex) {
                Array studiesValue = resultSet.getArray(VariantColumn.INDEX_STUDIES.column());
                if (studiesValue != null) {
                    studies = toList((PhoenixArray) studiesValue);
                } else {
                    studies = null;
                }

                boolean noSync = resultSet.getBoolean(VariantColumn.INDEX_NOT_SYNC.column());
                boolean unknown = resultSet.getBoolean(VariantColumn.INDEX_UNKNOWN.column());

                syncStatus = HadoopVariantSearchIndexUtils.getSyncStatus(noSync, unknown, studies);
            } else {
                studies = null;
                syncStatus = null;
            }

        } catch (SQLException e) {
            // This should never happen!
            throw new IllegalStateException(e);
        }

        String annotationId = this.defaultAnnotationId;
        if (defaultAnnotationId == null && annotationIds != null) {
            try {
                Integer annotationIdColumnIdx = findColumn(resultSet, VariantColumn.ANNOTATION_ID.column());
                if (annotationIdColumnIdx != null) {
                    int annotationIdNum = resultSet.getInt(VariantPhoenixSchema.VariantColumn.ANNOTATION_ID.column());
                    annotationId = annotationIds.get(annotationIdNum);
                }
            } catch (SQLException e) {
                throw VariantQueryException.internalException(e);
            }
        }

        return post(variantAnnotation, releases, syncStatus, studies, annotationId);
    }

    public VariantAnnotation convert(ImmutableBytesWritable bytes) {
        return convert(bytes.get(), bytes.getOffset(), bytes.getLength());
    }

    public VariantAnnotation convert(byte[] valueArray, int valueOffset, int valueLength) {
        VariantAnnotation variantAnnotation;
        try {
            if (valueArray[valueOffset] == '{') {
                // Value looks to be uncompressed. Try to parse. If fails, try to decompress and parse.
                try {
                    variantAnnotation = objectMapper.readValue(valueArray, valueOffset, valueLength,
                            VariantAnnotation.class);
                } catch (IOException e) {
                    try {
                        byte[] destination = new byte[valueLength];
                        System.arraycopy(valueArray, valueOffset, destination, 0, valueLength);
                        byte[] value = CompressionUtils.decompress(destination);
                        variantAnnotation = objectMapper.readValue(value, VariantAnnotation.class);
                    } catch (Exception e1) {
                        e1.addSuppressed(e);
                        throw e1;
                    }
                }
            } else {
                // Value is compressed. Decompress and parse
                byte[] destination = new byte[valueLength];
                System.arraycopy(valueArray, valueOffset, destination, 0, valueLength);
                byte[] value = CompressionUtils.decompress(destination);
                variantAnnotation = objectMapper.readValue(value, VariantAnnotation.class);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataFormatException e) {
            throw new IllegalStateException(e);
        }
        return variantAnnotation;
    }

    public Integer findColumn(ResultSet resultSet, String column) {
        try {
            return resultSet.findColumn(column);
        } catch (SQLException e) {
            //Column not found
            return null;
        }
    }

    public Integer getRelease(String columnName) {
        return Integer.valueOf(columnName.substring(VariantPhoenixSchema.RELEASE_PREFIX.length()));
    }

    private VariantAnnotation post(VariantAnnotation variantAnnotation, List<Integer> releases, VariantStorageEngine.SyncStatus syncStatus,
                                   List<Integer> studies, String annotationId) {
        boolean hasRelease = releases != null && !releases.isEmpty();
        boolean hasIndex = syncStatus != null || studies != null;
        boolean hasAnnotationId = StringUtils.isNotEmpty(annotationId);

        if (variantAnnotation == null) {
            if (!hasIndex && !hasRelease) {
                return null;
            } else {
                variantAnnotation = new VariantAnnotation();
            }
        }

        if (variantAnnotation.getConsequenceTypes() == null) {
            variantAnnotation.setConsequenceTypes(Collections.emptyList());
        }

        if (variantAnnotation.getTraitAssociation() == null) {
            variantAnnotation.setTraitAssociation(Collections.emptyList());
        } else {
            for (EvidenceEntry evidenceEntry : variantAnnotation.getTraitAssociation()) {
                if (evidenceEntry.getSubmissions() == null) {
                    evidenceEntry.setSubmissions(Collections.emptyList());
                }
                if (evidenceEntry.getHeritableTraits() == null) {
                    evidenceEntry.setHeritableTraits(Collections.emptyList());
                }
                if (evidenceEntry.getEthnicity() == null) {
                    evidenceEntry.setEthnicity(EthnicCategory.Z);
                }
                if (evidenceEntry.getAdditionalProperties() == null) {
                    evidenceEntry.setAdditionalProperties(Collections.emptyList());
                }
                if (evidenceEntry.getBibliography() == null) {
                    evidenceEntry.setBibliography(Collections.emptyList());
                }
                if (evidenceEntry.getGenomicFeatures() == null) {
                    evidenceEntry.setGenomicFeatures(Collections.emptyList());
                }
            }
        }

        AdditionalAttribute additionalAttribute = null;
        if (hasAnnotationId || hasRelease || hasIndex) {
            if (variantAnnotation.getAdditionalAttributes() == null) {
                variantAnnotation.setAdditionalAttributes(new HashMap<>());
            }
            if (variantAnnotation.getAdditionalAttributes().containsKey(GROUP_NAME.key())) {
                additionalAttribute = variantAnnotation.getAdditionalAttributes().get(GROUP_NAME.key());
            } else {
                additionalAttribute = new AdditionalAttribute(new HashMap<>());
                variantAnnotation.getAdditionalAttributes().put(GROUP_NAME.key(), additionalAttribute);
            }
        }
        if (hasRelease) {
            String release = releases.stream().min(Integer::compareTo).orElse(0).toString();
            additionalAttribute.getAttribute().put(VariantField.AdditionalAttributes.RELEASE.key(), release);
        }

        if (hasIndex) {
            if (syncStatus != null) {
                additionalAttribute.getAttribute().put(VariantField.AdditionalAttributes.INDEX_SYNCHRONIZATION.key(), syncStatus.key());
            }
            if (studies != null) {
                additionalAttribute.getAttribute().put(VariantField.AdditionalAttributes.INDEX_STUDIES.key(),
                        studies.stream().map(Object::toString).collect(Collectors.joining(",")));
            }
        }

        if (hasAnnotationId) {
            additionalAttribute.getAttribute().put(VariantField.AdditionalAttributes.ANNOTATION_ID.key(), annotationId);
        }
        return variantAnnotation;
    }
}
