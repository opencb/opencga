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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.variant.converters.Converter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantAnnotationMixin;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Created on 03/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToVariantAnnotationConverter implements Converter<Result, VariantAnnotation> {

    private final ObjectMapper objectMapper;
    private final byte[] columnFamily;

    public HBaseToVariantAnnotationConverter(GenomeHelper genomeHelper) {
        columnFamily = genomeHelper.getColumnFamily();
        objectMapper = new ObjectMapper();
        objectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
    }

    public HBaseToVariantAnnotationConverter setReturnedFields(Set<VariantField> allReturnedFields) {
        List<String> list = new ArrayList<>();
        if (allReturnedFields != null) {
            for (VariantField annotationField : VariantField.values()) {
                if (annotationField.getParent() == VariantField.ANNOTATION && !allReturnedFields.contains(annotationField)) {
                    list.add(annotationField.fieldName().replace(VariantField.ANNOTATION.fieldName() + '.', ""));
                }
            }
        }
        String[] returnedAnnotationFields = list.toArray(new String[list.size()]);
        objectMapper.setAnnotationIntrospector(
                new JacksonAnnotationIntrospector() {
                    @Override
                    public String[] findPropertiesToIgnore(Annotated ac, boolean forSerialization) {
                        String[] propertiesToIgnore = super.findPropertiesToIgnore(ac, forSerialization);
                        if (ArrayUtils.isNotEmpty(propertiesToIgnore)) {
                            List<String> list = new ArrayList<>();
                            Collections.addAll(list, returnedAnnotationFields);
                            Collections.addAll(list, propertiesToIgnore);
                            return list.toArray(new String[list.size()]);
                        } else {
                            return returnedAnnotationFields;
                        }
                    }
                });
        return this;
    }

    @Override
    public VariantAnnotation convert(Result result) {

        byte[] value = result.getValue(columnFamily, VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.bytes());
        if (ArrayUtils.isNotEmpty(value)) {
            try {
                return objectMapper.readValue(value, VariantAnnotation.class);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return null;
    }

    public VariantAnnotation convert(ResultSet resultSet) {
        int column;
        try {
            column = resultSet.findColumn(VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.column());
        } catch (SQLException e) {
            //Column not found
            return null;
        }
        try {
            String value = resultSet.getString(column);
            if (StringUtils.isNotEmpty(value)) {
                try {
                    return objectMapper.readValue(value, VariantAnnotation.class);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        } catch (SQLException e) {
            // This should never happen!
            throw new IllegalStateException(e);
        }
        return null;

    }
}
