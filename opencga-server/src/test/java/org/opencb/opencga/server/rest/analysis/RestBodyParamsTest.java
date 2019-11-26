package org.opencb.opencga.server.rest.analysis;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RestBodyParamsTest {

    private MyRestBodyParams p = new MyRestBodyParams();;
    private MyRestBodyWithDynamicParams pd = new MyRestBodyWithDynamicParams();;

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

    public static class MyRestBodyWithDynamicParams extends MyRestBodyParams {
        public Map<String, String> dynamicParams;
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
    public void testToParamsDynamic() throws IOException {
        pd.dynamicParams = new HashMap<>();
        pd.dynamicParams.put("otherParam", "value");
        pd.dynamicParams.put("myKey", "overwrite");
        Map<String, Object> params = pd.toParams();

        assertEquals(5, params.size());
        assertEquals("asdf", params.get("myKey"));
        assertNull(params.get("myKey2"));
        assertEquals("", params.get("myBooleanTrue"));
        assertEquals("true", params.get("myBooleanNullableTrue"));
        assertNull(params.get("myBoolean"));
        assertNull(params.get("myBooleanNullable"));
        assertEquals("0", params.get("myInteger"));
        assertNull(params.get("myIntegerNullable"));
        assertEquals(pd.dynamicParams, params.get("dynamicParams"));
        assertNotSame(pd.dynamicParams, params.get("dynamicParams"));
    }

    @Test
    public void testToObjectMap() throws IOException {
        pd.dynamicParams = new HashMap<>();
        pd.dynamicParams.put("otherParam", "value");
        pd.dynamicParams.put("myKey", "overwrite");
        ObjectMap params = pd.toObjectMap();

        assertEquals(6, params.size());
        assertEquals("asdf", params.get("myKey"));
        assertNull(params.get("myKey2"));
        assertEquals(true, params.get("myBooleanTrue"));
        assertEquals(true, params.get("myBooleanNullableTrue"));
        assertEquals(false, params.get("myBoolean"));
        assertNull(params.get("myBooleanNullable"));
        assertEquals(0, params.get("myInteger"));
        assertNull(params.get("myIntegerNullable"));
        assertEquals(pd.dynamicParams, params.get("dynamicParams"));
    }
}