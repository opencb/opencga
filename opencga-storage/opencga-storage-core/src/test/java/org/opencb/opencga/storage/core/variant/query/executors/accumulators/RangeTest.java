package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import org.junit.Test;

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
    }

    protected void checkParse(String s) {
        assertEquals(s, Range.parse(s).toString());
    }

}