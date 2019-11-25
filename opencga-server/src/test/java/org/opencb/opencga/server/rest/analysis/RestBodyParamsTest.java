package org.opencb.opencga.server.rest.analysis;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RestBodyParamsTest {

    private MyRestBodyParams p = new MyRestBodyParams();;

    public static class MyRestBodyParams extends RestBodyParams {
        public String myKey = "asdf";
        public String myKey2;
        public boolean myBoolean;
        public boolean myBooleanTrue = true;
        public Boolean myBooleanNullable;
        public Boolean myBooleanNullableTrue = true;
        public int myInteger;
        public Integer myIntegerNullable;
        private String myPrivateString = "private!";
    }

    @Test
    public void testToParams() throws IOException {
        Map<String, Object> params = p.toParams();

        assertEquals("asdf", params.get("myKey"));
        assertNull(params.get("myKey2"));
        assertEquals("", params.get("myBooleanTrue"));
        assertEquals("true", params.get("myBooleanNullableTrue"));
        assertNull(params.get("myBoolean"));
        assertNull(params.get("myBooleanNullable"));
        assertEquals("0", params.get("myInteger"));
        assertNull(params.get("myIntegerNullable"));
    }

    @Test
    public void testToParamsExtra() throws IOException {
        Map<String, Object> params = p.toParams(new ObjectMap()
                .append("otherParam", "value").append("myKey", "overwrite"));

        assertEquals("overwrite", params.get("myKey"));
        assertNull(params.get("myKey2"));
        assertEquals("", params.get("myBooleanTrue"));
        assertEquals("true", params.get("myBooleanNullableTrue"));
        assertNull(params.get("myBoolean"));
        assertNull(params.get("myBooleanNullable"));
        assertEquals("0", params.get("myInteger"));
        assertNull(params.get("myIntegerNullable"));
        assertEquals("value", params.get("-DotherParam"));
    }

    @Test
    public void testToParamsDynamic() throws IOException {
        p.dynamicParams = new HashMap<>();
        p.dynamicParams.put("otherParam", "value");
        p.dynamicParams.put("myKey", "overwrite");
        Map<String, Object> params = p.toParams();

        assertEquals(5, params.size());
        assertEquals("overwrite", params.get("myKey"));
        assertNull(params.get("myKey2"));
        assertEquals("", params.get("myBooleanTrue"));
        assertEquals("true", params.get("myBooleanNullableTrue"));
        assertNull(params.get("myBoolean"));
        assertNull(params.get("myBooleanNullable"));
        assertEquals("0", params.get("myInteger"));
        assertNull(params.get("myIntegerNullable"));
        assertEquals("value", params.get("-DotherParam"));
    }

    @Test
    public void testToObjectMap() throws IOException {
        p.dynamicParams = new HashMap<>();
        p.dynamicParams.put("otherParam", "value");
        p.dynamicParams.put("myKey", "overwrite");
        ObjectMap params = p.toObjectMap();

        assertEquals(6, params.size());
        assertEquals("asdf", params.get("myKey"));
        assertNull(params.get("myKey2"));
        assertEquals(true, params.get("myBooleanTrue"));
        assertEquals(true, params.get("myBooleanNullableTrue"));
        assertEquals(false, params.get("myBoolean"));
        assertNull(params.get("myBooleanNullable"));
        assertEquals(0, params.get("myInteger"));
        assertNull(params.get("myIntegerNullable"));
        assertEquals(p.dynamicParams, params.get("dynamicParams"));
    }
}