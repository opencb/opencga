package org.opencb.opencga.storage.core.variant.query.projection;

import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.SAMPLE;

public class VariantQueryProjectionParserTest {

    @Test
    public void queryBySampleGenotype() throws Exception {
        Query query = new Query(STUDY.key(), "s1").append(SAMPLE.key(), "sample1:0/1,0|1,1|0;sample2:0/1,0|1,1|0;sample3:1/1,1|1");
        assertEquals(Arrays.asList("sample1", "sample2", "sample3"), VariantQueryProjectionParser.getIncludeSamplesList(query));
    }


}