package org.opencb.opencga.storage.core.metadata.models;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SampleVariantStatsMixin {

    @JsonDeserialize(using = SampleVariantStatsMendelianErrorDeserializer.class)
    public void setMendelianErrorCount(Map<String, Map<String, Integer>> me) {
    }

    public static class SampleVariantStatsMendelianErrorDeserializer extends StdDeserializer<Map<String, Map<String, Integer>>> {

        private static MapType mapType;

        static {
            TypeFactory tf = TypeFactory.defaultInstance();
            MapType mapStrInt = tf.constructMapType(HashMap.class, tf.constructSimpleType(String.class, null),
                    tf.constructSimpleType(Integer.class, null));
            mapType = tf.constructMapType(HashMap.class, tf.constructSimpleType(String.class, null), mapStrInt);
        }

        protected SampleVariantStatsMendelianErrorDeserializer() {
            super(mapType);
        }

        protected SampleVariantStatsMendelianErrorDeserializer(Class<?> vc) {
            super(vc);
        }

        protected SampleVariantStatsMendelianErrorDeserializer(JavaType valueType) {
            super(valueType);
        }

        protected SampleVariantStatsMendelianErrorDeserializer(StdDeserializer<?> src) {
            super(src);
        }

        @Override
        public Map<String, Map<String, Integer>> deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            Map map = p.readValueAs(Map.class);
            if (map.isEmpty()) {
                return map;
            } else {
                Map.Entry<String, ?> next = (Map.Entry<String, ?>) map.entrySet().iterator().next();
                if (next.getValue() instanceof Integer) {
                    // Old map
                    HashMap<String, Map<String, Integer>> newCountMap = new HashMap<>();
                    newCountMap.put("ALL", map);
                    return newCountMap;
                } else {
                    return map;
                }
            }
        }
    }

}
