package org.opencb.opencga.analysis.variant.samples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TreeQueryTest {

    @Test
    public void testParse() throws JsonProcessingException {
        assertEquals("key:value key3:>50", TreeQuery.parse("key:value key3:>50").toString());
        assertEquals("(key:value) AND (key3:>50)", TreeQuery.parse("(key:value) AND (key3:>50)").toString());
        assertEquals("(key:value) AND (key3:>50)", TreeQuery.parse("  ( key:value   )   AND   ( key3:>50  )").toString());
        assertEquals("((key:value) AND (key3:>50)) OR (key5:>=2323)", TreeQuery.parse("((key:value) AND (key3:>50)) OR (key5:>=2323)").toString());

        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(TreeQuery.parse("((key:value) AND (key3:>50)) OR (key5:>=2323)")));
    }
}