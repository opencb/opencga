package org.opencb.opencga.storage.core.metadata.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.JacksonUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SampleVariantStatsMixinTest {

    private ObjectMapper mapper;

    @Before
    public void setUp() throws Exception {
        mapper = JacksonUtils.getDefaultObjectMapper()
                .addMixIn(SampleVariantStats.class, SampleVariantStatsMixin.class);
    }

    @Test
    public void testDeserialize() throws Exception {
        ObjectMap json = new ObjectMap();
        json.put("id", "mySample");
        HashMap<String, Map<String, Integer>> me = new HashMap<>();
        json.put("mendelianErrorCount", me);
        me.put("chr1", Collections.singletonMap("2", 12));
        System.out.println(json.toJson());

        SampleVariantStats expected = new SampleVariantStats();
        expected.setId("mySample");
        expected.setMendelianErrorCount(new HashMap<>(me));

        SampleVariantStats actual = mapper.readValue(json.toJson(), SampleVariantStats.class);

        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeAndMigrate() throws Exception {
        ObjectMap json = new ObjectMap();
        json.put("id", "mySample");
        Map<String, Integer> me = Collections.singletonMap("2", 12);
        json.put("mendelianErrorCount", me);
        System.out.println("actual = " + json.toJson());

        SampleVariantStats expected = new SampleVariantStats();
        expected.setId("mySample");
        expected.setMendelianErrorCount(Collections.singletonMap("ALL", me));
        System.out.println("expected = " + expected);

        SampleVariantStats actual = mapper.readValue(json.toJson(), SampleVariantStats.class);
        System.out.println("actual = " + actual);
        assertEquals(expected, actual);
    }


}