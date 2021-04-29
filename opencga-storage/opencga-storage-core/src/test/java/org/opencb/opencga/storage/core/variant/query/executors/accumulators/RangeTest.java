package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import org.junit.Test;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
public class RangeTest {

    @Test
    public void testParse() {
        checkParse("[1.0, 10.0)");
        checkParse("[1.0, 10.0]");
        checkParse("(1.0, 10.0]");
        checkParse("[10.0]");
        checkParse("(10.0]");
        checkParse("(10.0, inf]");
        checkParse("(-inf, inf]");
        checkParse("NA");
    }

    @Test
    public void buildFromIndex() {
        IndexFieldConfiguration configuration = new IndexFieldConfiguration(IndexFieldConfiguration.Source.FILE, "QUAL", new double[]{20d, 30d, 250d}, IndexFieldConfiguration.Type.RANGE_LT);
        List<Range<Double>> ranges = Range.buildRanges(configuration);
        assertEquals(Arrays.asList(
                Range.parse("NA"),
                Range.parse("(-inf, 20.0)"),
                Range.parse("[20.0, 30.0)"),
                Range.parse("[30.0, 250.0)"),
                Range.parse("[250.0, inf)")
        ), ranges);

        configuration = new IndexFieldConfiguration(IndexFieldConfiguration.Source.FILE, "QUAL", new double[]{20d, 30d, 250d}, IndexFieldConfiguration.Type.RANGE_GT);
        ranges = Range.buildRanges(configuration);
        assertEquals(Arrays.asList(
                Range.parse("NA"),
                Range.parse("(-inf, 20.0]"),
                Range.parse("(20.0, 30.0]"),
                Range.parse("(30.0, 250.0]"),
                Range.parse("(250.0, inf)")
        ), ranges);
    }

    protected void checkParse(String s) {
        assertEquals(s, Range.parse(s).toString());
    }

}