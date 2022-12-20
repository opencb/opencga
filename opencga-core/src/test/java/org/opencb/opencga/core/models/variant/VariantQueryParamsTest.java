package org.opencb.opencga.core.models.variant;

import org.junit.Test;

import static org.junit.Assert.*;

public class VariantQueryParamsTest {

    @Test
    public void test() {
        VariantQueryParams params = new VariantQueryParams();
        assertEquals(0, params.toObjectMap().size());
        assertEquals(0, params.toParams().size());
    }
}