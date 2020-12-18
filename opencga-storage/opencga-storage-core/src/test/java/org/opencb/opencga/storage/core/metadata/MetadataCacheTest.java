package org.opencb.opencga.storage.core.metadata;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class MetadataCacheTest {

    @Test
    public void testCache() {
        Map<String, List<Integer>> values = new HashMap<>();
        for (int i = 1; i <= 5; i++) {
            values.put("va_" + i, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
            values.put("vb_" + i, Arrays.asList(1, 2));
        }
        MetadataCache<String, List<Integer>> cache = new MetadataCache<>(
                (study, key) -> new ArrayList<>(values.get(key)),
                v -> v.size() > 5);

        List<Integer> expected = cache.get(0, "vb_1");
        List<Integer> actual = cache.get(0, "vb_2");
        Assert.assertEquals(expected, actual);
        Assert.assertNotSame(expected, actual);

        expected = cache.get(0, "va_1");
        actual = cache.get(0, "va_2");
        Assert.assertEquals(expected, actual);
        Assert.assertSame(expected, actual);

    }

}