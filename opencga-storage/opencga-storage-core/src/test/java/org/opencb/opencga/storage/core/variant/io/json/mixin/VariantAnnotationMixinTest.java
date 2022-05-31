package org.opencb.opencga.storage.core.variant.io.json.mixin;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class VariantAnnotationMixinTest {


    private ObjectMapper objectMapper;

    @Before
    public void setUp() throws Exception {
        objectMapper = new ObjectMapper();
        objectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        objectMapper.addMixIn(ConsequenceType.class, ConsequenceTypeMixin.class);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Test
    public void testRead() throws Exception {
        VariantAnnotation va = objectMapper.readerFor(VariantAnnotation.class)
                .readValue(new ObjectMap()
                        .append("consequenceTypes", Arrays.asList(new ObjectMap()
                                .append("geneId", "geneId")
                                .append("ensemblGeneId", "ensemblGeneId")
                                .append("ensemblTranscriptId", "ensemblTranscriptId")
                                .append("transcriptAnnotationFlags", Arrays.asList("a"))))
                        .toJson());

        assertEquals(Arrays.asList("a"), va.getConsequenceTypes().get(0).getTranscriptFlags());
        assertEquals("geneId", va.getConsequenceTypes().get(0).getGeneId());
        assertEquals("ensemblGeneId", va.getConsequenceTypes().get(0).getEnsemblGeneId());
        assertEquals("ensemblTranscriptId", va.getConsequenceTypes().get(0).getEnsemblTranscriptId());
        assertEquals("ensemblTranscriptId", va.getConsequenceTypes().get(0).getTranscriptId()); // Take default value
    }
}