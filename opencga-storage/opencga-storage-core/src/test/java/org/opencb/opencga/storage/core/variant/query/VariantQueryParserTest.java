package org.opencb.opencga.storage.core.variant.query;

import org.junit.Test;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.parseGenotypeFilter;

public class VariantQueryParserTest {

    @Test
    public void preProcessGenotypesFilter() {
        List<String> loadedGenotypes = Arrays.asList("0/1", "1/1",
                "0|1", "1|0", "1|1",
                "0/2", "0|2", "2|0",
                "1/2", "1|2", "2|1",
                "0/3", "0|3", "3|0",
                "1/3", "1|3", "3|1",
                "2/3", "2|3", "3|2",
                "0/4", "0/5", "0/6", "0/16",
                "1/4", "1/5", "1/6", "1/16"
        );

        assertEquals("S1:1/1,1|1", preProcessGenotypesFilter("S1:1/1", loadedGenotypes));
        assertEquals("S1:1/1,1|1,1/2,1|2,2|1,1/3,1|3,3|1,1/4,1/5,1/6,1/16", preProcessGenotypesFilter("S1:1/1,1/2", loadedGenotypes));
        assertEquals("S1:1/3,1|3,3|1,1/2,1|2,2|1,1/4,1/5,1/6,1/16", preProcessGenotypesFilter("S1:1/3", loadedGenotypes));
        assertEquals("S1:0/2,0|2,2|0,0/3,0|3,3|0,0/4,0/5,0/6,0/16", preProcessGenotypesFilter("S1:0/2", loadedGenotypes));
        assertEquals("S1:0/3,0|3,3|0,0/2,0|2,2|0,0/4,0/5,0/6,0/16", preProcessGenotypesFilter("S1:0/3", loadedGenotypes));
        assertEquals("S1:./2", preProcessGenotypesFilter("S1:./2", loadedGenotypes)); // check scape '.' in regex
    }

    protected String preProcessGenotypesFilter(String genotypeFilter, List<String> loadedGenotypes) {
        Map<Object, List<String>> map = new LinkedHashMap<>();
        VariantQueryUtils.QueryOperation op = parseGenotypeFilter(genotypeFilter, map);
        return VariantQueryParser.preProcessGenotypesFilter(map, op, loadedGenotypes);
    }
}