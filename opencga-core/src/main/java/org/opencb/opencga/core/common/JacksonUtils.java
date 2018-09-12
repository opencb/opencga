package org.opencb.opencga.core.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.mixin.*;

public class JacksonUtils {

    public static ObjectMapper getDefaultObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        return objectMapper;
    }

    public static ObjectMapper getCompleteOpenCGAObjectMapper() {
        ObjectMapper objectMapper = getDefaultObjectMapper();
        objectMapper.addMixIn(Individual.class, IndividualMixin.class);
        objectMapper.addMixIn(Family.class, FamilyMixin.class);
        objectMapper.addMixIn(File.class, FileMixin.class);
        objectMapper.addMixIn(DiseasePanel.class, PanelMixin.class);
        objectMapper.addMixIn(Project.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Study.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Sample.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Cohort.class, PrivateUidMixin.class);
        objectMapper.addMixIn(Job.class, PrivateUidMixin.class);
        objectMapper.addMixIn(VariableSet.class, PrivateUidMixin.class);
        objectMapper.addMixIn(ClinicalAnalysis.class, PrivateUidMixin.class);

        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        return objectMapper;
    }

}
