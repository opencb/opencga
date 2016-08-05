package org.opencb.opencga.storage.hadoop.variant.index;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by mh719 on 02/08/2016.
 */
public class VariantTableMapperTest {
    @Test
    public void generateRegion() throws Exception {
        VariantTableMapper mapper = new VariantTableMapper();
        Set<Integer> integers = mapper.generateRegion(10, 20);
        assertEquals(new HashSet(Arrays.asList(10,11,12,13,14,15,16,17,18,19,20)),integers);
    }

    @Test
    public void generateRegionIndel() throws Exception {
        VariantTableMapper mapper = new VariantTableMapper();
        Set<Integer> integers = mapper.generateRegion(10, 9);
        assertEquals(new HashSet(Arrays.asList(10)),integers);
    }

}