package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(ShortTests.class)
public class VariantQueryTest {

    @Test
    public void test() throws Exception {

        VariantQuery variantQuery = new VariantQuery();
        for (VariantQueryParam param : VariantQueryParam.values()) {
//            System.out.println("param.key() = " + param.key());
//            System.out.println("param.type() = " + param.type());
            Method methodGet = getMethodSafe(param.key());
            assertNotNull(param.key(), methodGet);
            Object expectedValue;
            Method methodSet;
            if (param.type() == QueryParam.Type.BOOLEAN || param.type() == QueryParam.Type.BOOLEAN_ARRAY) {
                methodSet = getMethodSafe(param.key(), boolean.class);
                expectedValue = true;
            } else if (param.type() == QueryParam.Type.INTEGER || param.type() == QueryParam.Type.INTEGER_ARRAY) {
                methodSet = getMethodSafe(param.key(), int.class);
                expectedValue = 42;
                if (methodSet == null) {
                    methodSet = getMethodSafe(param.key(), Integer.class);
                }
            } else {
                expectedValue = RandomStringUtils.random(10);
                methodSet = getMethodSafe(param.key(), String.class);
            }
            assertNotNull(param.key(), methodSet);
            methodSet.invoke(variantQuery, expectedValue);
            assertEquals(param.key(), expectedValue, variantQuery.get(param.key()));

            Object got = methodGet.invoke(variantQuery);
            assertEquals(param.key(), expectedValue, got);
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