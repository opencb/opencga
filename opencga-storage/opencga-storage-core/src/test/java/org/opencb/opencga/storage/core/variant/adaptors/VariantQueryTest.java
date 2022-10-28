package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryParam;

import java.lang.reflect.Array;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

public class VariantQueryTest {

    @Test
    public void test() throws Exception {

        VariantQuery variantQuery = new VariantQuery();
        for (VariantQueryParam param : VariantQueryParam.values()) {
//            System.out.println("param.key() = " + param.key());
//            System.out.println("param.type() = " + param.type());
            Method methodGet = getMethodSafe(param.key());
            assertNotNull(methodGet);
            Object expectedValue;
            Method methodSet;
            if (param.type() == QueryParam.Type.BOOLEAN || param.type() == QueryParam.Type.BOOLEAN_ARRAY) {
                methodSet = getMethodSafe(param.key(), Boolean.class);
                expectedValue = true;
            } else {
                expectedValue = RandomStringUtils.random(10);
                methodSet = getMethodSafe(param.key(), String.class);
            }
            assertNotNull(methodSet);
            methodSet.invoke(variantQuery, expectedValue);
            assertEquals(expectedValue, variantQuery.get(param.key()));

            Object got = methodGet.invoke(variantQuery);
            assertEquals(expectedValue, got);
        }

    }

    private Method getMethodSafe(String name, Class<?>... parameterTypes) {
        try {
            return VariantQuery.class.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }
}