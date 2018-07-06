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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.Converter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.annotation.converters.VariantTraitAssociationToEvidenceEntryConverter;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantAnnotationMixin;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

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
    private final VariantTraitAssociationToEvidenceEntryConverter traitAssociationConverter;

    public HBaseToVariantAnnotationConverter(GenomeHelper genomeHelper, long ts) {
        super(genomeHelper.getColumnFamily());
        columnFamily = genomeHelper.getColumnFamily();
        this.ts = ts;
        objectMapper = new ObjectMapper();
        objectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        traitAssociationConverter = new VariantTraitAssociationToEvidenceEntryConverter();
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

    @Override
    public VariantAnnotation convert(Result result) {
        VariantAnnotation variantAnnotation = null;

        byte[] value = result.getValue(columnFamily, VariantColumn.FULL_ANNOTATION.bytes());
        if (ArrayUtils.isNotEmpty(value)) {
            try {
                variantAnnotation = objectMapper.readValue(value, VariantAnnotation.class);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        List<Integer> releases = new ArrayList<>();
        for (byte[] bytes : result.getFamilyMap(columnFamily).tailMap(VariantPhoenixHelper.RELEASE_PREFIX_BYTES).keySet()) {
            if (Bytes.startsWith(bytes, VariantPhoenixHelper.RELEASE_PREFIX_BYTES)) {
                releases.add(getRelease(Bytes.toString(bytes)));
            }
        }
        byte[] studiesValue = result.getFamilyMap(columnFamily).get(VariantColumn.INDEX_STUDIES.bytes());

        List<Integer> studies;
        if (studiesValue != null) {
            studies = toList((PhoenixArray) VariantColumn.INDEX_STUDIES.getPDataType().toObject(studiesValue));
        } else {
            studies = null;
        }
        Cell notSyncCell = result.getColumnLatestCell(columnFamily, VariantColumn.INDEX_NOT_SYNC.bytes());
        Cell unknownCell = result.getColumnLatestCell(columnFamily, VariantColumn.INDEX_UNKNOWN.bytes());

        // Don't need to check the value. If present, only check the timestamp.
        boolean notSync = notSyncCell != null && notSyncCell.getTimestamp() > ts;
        // Arrays.equals(PBoolean.TRUE_BYTES, result.getFamilyMap(columnFamily).get(VariantColumn.INDEX_NOT_SYNC.bytes()))
        boolean unknown = unknownCell != null && unknownCell.getTimestamp() > ts;
        // Arrays.equals(PBoolean.TRUE_BYTES, result.getFamilyMap(columnFamily).get(VariantColumn.INDEX_UNKNOWN.bytes()))
        VariantSearchManager.SyncStatus syncStatus = HadoopVariantSearchIndexUtils.getSyncStatus(notSync, unknown, studies);

        return post(variantAnnotation, releases, syncStatus, studies);
    }

    public VariantAnnotation convert(ResultSet resultSet) {
        VariantAnnotation variantAnnotation = null;

        Integer column = findColumn(resultSet, VariantColumn.FULL_ANNOTATION);
        if (column != null) {
            try {
                    String value = resultSet.getString(column);
                    if (StringUtils.isNotEmpty(value)) {
                        try {
                            variantAnnotation = objectMapper.readValue(value, VariantAnnotation.class);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
            } catch (SQLException e) {
                // This should never happen!
                throw new IllegalStateException(e);
            }
        }


        List<Integer> releases = new ArrayList<>();
        List<Integer> studies;
        VariantSearchManager.SyncStatus syncStatus;
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            boolean hasIndex = false;
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                if (columnName.startsWith(VariantPhoenixHelper.RELEASE_PREFIX)) {
                    if (resultSet.getBoolean(i)) {
                        releases.add(getRelease(columnName));
                    }
                } else if (columnName.equals(VariantColumn.INDEX_NOT_SYNC.column())) {
                    hasIndex = true;
                }
            }

            if (hasIndex) {
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
        return post(variantAnnotation, releases, syncStatus, studies);
    }

    public Integer findColumn(ResultSet resultSet, PhoenixHelper.Column column) {
        try {
            return resultSet.findColumn(column.column());
        } catch (SQLException e) {
            //Column not found
            return null;
        }
    }

    public Integer getRelease(String columnName) {
        return Integer.valueOf(columnName.substring(VariantPhoenixHelper.RELEASE_PREFIX.length()));
    }

    private VariantAnnotation post(VariantAnnotation variantAnnotation, List<Integer> releases, VariantSearchManager.SyncStatus syncStatus,
                                   List<Integer> studies) {
        boolean hasRelease = releases != null && !releases.isEmpty();
        boolean hasIndex = syncStatus != null || studies != null;

        if (variantAnnotation == null) {
            if (!hasIndex && !hasRelease) {
                return null;
            } else {
                variantAnnotation = new VariantAnnotation();
            }
        }
        // If there are VariantTraitAssociation, and there are none TraitAssociations (EvidenceEntry), convert
        if (variantAnnotation.getVariantTraitAssociation() != null
                && CollectionUtils.isEmpty(variantAnnotation.getTraitAssociation())) {
            List<EvidenceEntry> evidenceEntries = traitAssociationConverter.convert(variantAnnotation.getVariantTraitAssociation());
            variantAnnotation.setTraitAssociation(evidenceEntries);
        }
        AdditionalAttribute additionalAttribute = null;
        if (hasRelease || hasIndex) {
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
        return variantAnnotation;
    }
}
