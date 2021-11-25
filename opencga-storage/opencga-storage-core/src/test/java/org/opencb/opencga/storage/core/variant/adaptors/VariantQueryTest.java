package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.*;

public class VariantQueryTest {

    @Test
    public void test() throws Exception {

        VariantQuery variantQuery = new VariantQuery();
        for (VariantQueryParam param : VariantQueryParam.values()) {
            Method methodSet = VariantQuery.class.getMethod(param.key(), String.class);
            assertNotNull(methodSet);
            String value = RandomStringUtils.random(10);
            methodSet.invoke(variantQuery, value);
            assertEquals(value, variantQuery.get(param.key()));

            Method methodGet = VariantQuery.class.getMethod(param.key());
            assertNotNull(methodGet);
            Object got = methodGet.invoke(variantQuery);
            assertEquals(value, got);
        }

    }
}