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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.EthnicCategory;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.Converter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.annotation.converters.VariantTraitAssociationToEvidenceEntryConverter;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantAnnotationMixin;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * Created on 03/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToVariantAnnotationConverter implements Converter<Result, VariantAnnotation> {

    private final ObjectMapper objectMapper;
    private final byte[] columnFamily;
    private final VariantTraitAssociationToEvidenceEntryConverter traitAssociationConverter;
    private byte[] annotationColumn = VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.bytes();

    public HBaseToVariantAnnotationConverter(GenomeHelper genomeHelper) {
        columnFamily = genomeHelper.getColumnFamily();
        objectMapper = new ObjectMapper();
        objectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        traitAssociationConverter = new VariantTraitAssociationToEvidenceEntryConverter();
    }

    public HBaseToVariantAnnotationConverter setAnnotationColumn(byte[] annotationColumn) {
        this.annotationColumn = annotationColumn;
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

        byte[] value = result.getValue(columnFamily, annotationColumn);
        if (ArrayUtils.isNotEmpty(value)) {
            try {
                variantAnnotation = objectMapper.readValue(value, VariantAnnotation.class);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        List<Integer> releases = new ArrayList<>();
        for (byte[] bytes : result.getFamilyMap(columnFamily).keySet()) {
            if (Bytes.startsWith(bytes, VariantPhoenixHelper.RELEASE_PREFIX_BYTES)) {
                releases.add(getRelease(Bytes.toString(bytes)));
            }
        }

        return post(variantAnnotation, releases);
    }

    public VariantAnnotation convert(ResultSet resultSet) {
        VariantAnnotation variantAnnotation = null;

        Integer column = findColumn(resultSet, VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION);
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
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                if (columnName.startsWith(VariantPhoenixHelper.RELEASE_PREFIX)) {
                    if (resultSet.getBoolean(i)) {
                        releases.add(getRelease(columnName));
                    }
                }
            }
        } catch (SQLException e) {
            // This should never happen!
            throw new IllegalStateException(e);
        }
        return post(variantAnnotation, releases);
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

    private VariantAnnotation post(VariantAnnotation variantAnnotation, List<Integer> releases) {
        if (variantAnnotation == null) {
            if (releases.isEmpty()) {
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

        // If there are VariantTraitAssociation, and there are none TraitAssociations (EvidenceEntry), convert
        if (variantAnnotation.getVariantTraitAssociation() != null
                && CollectionUtils.isEmpty(variantAnnotation.getTraitAssociation())) {
            List<EvidenceEntry> evidenceEntries = traitAssociationConverter.convert(variantAnnotation.getVariantTraitAssociation());
            variantAnnotation.setTraitAssociation(evidenceEntries);
        }
        if (releases != null && !releases.isEmpty()) {
            if (variantAnnotation.getAdditionalAttributes() == null) {
                variantAnnotation.setAdditionalAttributes(new HashMap<>());
            }
            String release = releases.stream().min(Integer::compareTo).orElse(0).toString();
            variantAnnotation.getAdditionalAttributes().put("opencga",
                    new AdditionalAttribute(Collections.singletonMap("release", release)));
        }

        return variantAnnotation;
    }
}
