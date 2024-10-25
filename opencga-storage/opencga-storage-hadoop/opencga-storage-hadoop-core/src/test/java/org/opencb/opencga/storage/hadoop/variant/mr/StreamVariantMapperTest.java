package org.opencb.opencga.storage.hadoop.variant.mr;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import static org.junit.Assert.*;


@Category(ShortTests.class)
public class StreamVariantMapperTest {
    @Test
    public void buildOutputKeyPrefixSingleDigitChromosome() {
        String result = StreamVariantMapper.buildOutputKeyPrefix("1", 100);
        assertEquals("01|0000000100|", result);
    }

    @Test
    public void buildOutputKeyPrefixDoubleDigitChromosome() {
        String result = StreamVariantMapper.buildOutputKeyPrefix("22", 100);
        assertEquals("22|0000000100|", result);
    }

    @Test
    public void buildOutputKeyPrefixRandomChromosome() {
        String result = StreamVariantMapper.buildOutputKeyPrefix("1_KI270712v1_random", 100);
        assertEquals("01_KI270712v1_random|0000000100|", result);
    }

    @Test
    public void buildOutputKeyPrefixMTChromosome() {
        String result = StreamVariantMapper.buildOutputKeyPrefix("MT", 100);
        assertEquals("MT|0000000100|", result);
    }

    @Test
    public void buildOutputKeyPrefixXChromosome() {
        String result = StreamVariantMapper.buildOutputKeyPrefix("X", 100);
        assertEquals("X|0000000100|", result);
    }
}