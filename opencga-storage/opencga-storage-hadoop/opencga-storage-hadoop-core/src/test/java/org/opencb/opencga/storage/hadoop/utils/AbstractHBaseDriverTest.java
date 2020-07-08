package org.opencb.opencga.storage.hadoop.utils;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Arrays;

import static org.junit.Assert.*;

public class AbstractHBaseDriverTest {


    @Test
    public void testBuildArgs() {
        ObjectMap options = new ObjectMap();
        assertArrayEquals(new String[]{"myTable"}, AbstractHBaseDriver.buildArgs("myTable", options));

        options.put("key", "value");
        options.put("list", Arrays.asList(1, 2, 3));
        options.put("null", null);
        options.put("key with spaces", "value with spaces");
        options.put("boolean", "true");
        options.put("nested", new ObjectMap()
                .append("key1", "value1")
                .append("key2", "value2")
                .append("key3", new ObjectMap("deep", "nested"))
                .append("key4_void", new ObjectMap()));
        assertArrayEquals(new String[]{"myTable",
                        "key", "value",
                        "list", "1,2,3",
                        "key with spaces", "value with spaces",
                        "boolean", "true",
                        "nested.key1", "value1",
                        "nested.key2", "value2",
                        "nested.key3.deep", "nested"},
                AbstractHBaseDriver.buildArgs("myTable", options));
    }

}