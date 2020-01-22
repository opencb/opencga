package org.opencb.opencga.analysis.variant.samples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TreeQueryTest {

    @Test
    public void testParse() throws JsonProcessingException {
        assertEquals("key=value AND key3>50", new TreeQuery("key=value AND key3>50").toString());
        assertEquals("(key=value) AND (key3>50)", new TreeQuery("(key=value) AND (key3>50)").toString());
        assertEquals("(key=value) AND (key3>50)", new TreeQuery("  ( key=value   )   AND   ( key3>50  )").toString());
        assertEquals("((key=value) AND (key3>50)) OR (key5>=2323)", new TreeQuery("((key=value) AND (key3>50)) OR (key5>=2323)").toString());
        assertEquals("(NOT ((key=value) AND (key3>50))) OR (NOT (key5>=2323))", new TreeQuery("NOT ((key=value) AND (key3>50)) OR NOT (key5>=2323)").toString());

        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(new TreeQuery("((key=value) AND NOT (key3>50)) OR (key5>=2323)")));
    }
}