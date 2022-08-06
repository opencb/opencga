/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.avro.VariantStats;
import org.opencb.opencga.core.models.PrivateUidMixin;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.mixins.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.common.mixins.GenotypeJsonMixin;
import org.opencb.opencga.core.models.common.mixins.VariantStatsJsonMixin;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyMixin;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileMixin;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualMixin;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.panel.PanelMixin;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.VariableSet;

import javax.ws.rs.ext.ContextResolver;
import java.io.IOException;

public class JacksonUtils {

    private static ObjectMapper defaultObjectMapper;
    private static ObjectMapper defaultNonNullObjectMapper;
    private static ObjectMapper externalOpencgaObjectMapper;
    private static ObjectMapper updateObjectMapper;
    private static ObjectMapper defaultYamlObjectMapper;
    private static ObjectMapper defaultNonNullYamlObjectMapper;
    private static ObjectMapper externalOpencgaYamlObjectMapper;
    private static ObjectMapper updateYamlObjectMapper;

    static {
        defaultObjectMapper = generateDefaultObjectMapper();
        defaultNonNullObjectMapper = generateDefaultNonNullObjectMapper();
        externalOpencgaObjectMapper = generateOpenCGAObjectMapper();
        updateObjectMapper = generateUpdateObjectMapper();

        defaultYamlObjectMapper = generateDefaultObjectMapper(new YAMLFactory());
        defaultNonNullYamlObjectMapper = generateDefaultNonNullObjectMapper(new YAMLFactory());
        externalOpencgaYamlObjectMapper = generateOpenCGAObjectMapper(new YAMLFactory());
        updateYamlObjectMapper = generateUpdateObjectMapper(new YAMLFactory());
    }

    private static ObjectMapper generateDefaultObjectMapper() {
        return generateDefaultObjectMapper(null);
    }

    private static ObjectMapper generateDefaultObjectMapper(JsonFactory jf) {
        ObjectMapper objectMapper = new ObjectMapper(jf);
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

        objectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        objectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        return objectMapper;
    }

    private static ObjectMapper generateUpdateObjectMapper() {
        return generateUpdateObjectMapper(null);
    }

    private static ObjectMapper generateUpdateObjectMapper(JsonFactory jf) {
        ObjectMapper objectMapper = generateDefaultObjectMapper(jf);
        objectMapper.addMixIn(Individual.class, IndividualMixin.class);
        objectMapper.addMixIn(Family.class, FamilyMixin.class);
        objectMapper.addMixIn(File.class, FileMixin.class);
        objectMapper.addMixIn(org.opencb.opencga.core.models.panel.Panel.class, PanelMixin.class);
        objectMapper.addMixIn(Project.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Study.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Sample.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Cohort.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Job.class, PrivateUidMixin.class);
        objectMapper.addMixIn(VariableSet.class, PrivateUidMixin.class);
        objectMapper.addMixIn(ClinicalAnalysis.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Interpretation.class, PrivateUidMixin.class);

        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        return objectMapper;
    }

    private static ObjectMapper generateDefaultNonNullObjectMapper() {
        return generateDefaultNonNullObjectMapper(null);
    }

    private static ObjectMapper generateDefaultNonNullObjectMapper(JsonFactory jf) {
        ObjectMapper objectMapper = generateDefaultObjectMapper(jf);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return objectMapper;
    }

    private static ObjectMapper generateOpenCGAObjectMapper() {
        return generateOpenCGAObjectMapper(null);
    }

    private static ObjectMapper generateOpenCGAObjectMapper(JsonFactory jf) {
        ObjectMapper objectMapper = generateDefaultObjectMapper(jf);
        objectMapper.addMixIn(Individual.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Family.class, PrivateUidMixin.class);
        objectMapper.addMixIn(File.class, PrivateUidMixin.class);
        objectMapper.addMixIn(org.opencb.opencga.core.models.panel.Panel.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Project.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Study.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Sample.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Cohort.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Job.class, PrivateUidMixin.class);
        objectMapper.addMixIn(VariableSet.class, PrivateUidMixin.class);
        objectMapper.addMixIn(ClinicalAnalysis.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Interpretation.class, PrivateUidMixin.class);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        return objectMapper;
    }

    public static ObjectMapper getDefaultObjectMapper() {
        return defaultObjectMapper;
    }

    public static ObjectMapper getDefaultNonNullObjectMapper() {
        return defaultNonNullObjectMapper;
    }

    public static ObjectMapper getExternalOpencgaObjectMapper() {
        return externalOpencgaObjectMapper;
    }

    public static ObjectMapper getUpdateObjectMapper() {
        return updateObjectMapper;
    }

    public static ObjectMapper getDefaultYamlObjectMapper() {
        return defaultYamlObjectMapper;
    }

    public static ObjectMapper getDefaultNonNullYamlObjectMapper() {
        return defaultNonNullYamlObjectMapper;
    }

    public static ObjectMapper getExternalOpencgaYamlObjectMapper() {
        return externalOpencgaYamlObjectMapper;
    }

    public static ObjectMapper getUpdateYamlObjectMapper() {
        return updateYamlObjectMapper;
    }

    public static class ObjectMapperProvider implements ContextResolver<ObjectMapper> {

        final ObjectMapper mapper;

        public ObjectMapperProvider() {
            mapper = JacksonUtils.getDefaultNonNullObjectMapper();
        }

        @Override
        public ObjectMapper getContext(Class<?> type) {
            return mapper;
        }
    }

    public static <T> T copy(T instance, Class<T> clazz) throws IOException {
        if (instance == null) {
            return null;
        }
        TokenBuffer tokenBuffer = new TokenBuffer(defaultObjectMapper, false);
        defaultObjectMapper.writeValue(tokenBuffer, instance);
        return defaultObjectMapper.readValue(tokenBuffer.asParser(), clazz);
    }

}
