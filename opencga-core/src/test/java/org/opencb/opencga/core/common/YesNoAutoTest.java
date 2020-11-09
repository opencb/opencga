package org.opencb.opencga.core.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class YesNoAutoTest {

    @Test
    public void testJsonCreator() throws Exception {
        assertEquals(YesNoAuto.NO, JacksonUtils.getDefaultObjectMapper().readValue("\"NO\"", YesNoAuto.class));
        assertEquals(YesNoAuto.NO, JacksonUtils.getDefaultObjectMapper().readValue("\"no\"", YesNoAuto.class));
        assertEquals(YesNoAuto.NO, JacksonUtils.getDefaultObjectMapper().readValue("\"No\"", YesNoAuto.class));
        assertEquals(YesNoAuto.NO, JacksonUtils.getDefaultObjectMapper().readValue("\"FALSE\"", YesNoAuto.class));
        assertEquals(YesNoAuto.YES, JacksonUtils.getDefaultObjectMapper().readValue("\"True\"", YesNoAuto.class));
        assertEquals(YesNoAuto.YES, JacksonUtils.getDefaultObjectMapper().readValue("\"YES\"", YesNoAuto.class));
        assertEquals(YesNoAuto.YES, JacksonUtils.getDefaultObjectMapper().readValue("\"yes\"", YesNoAuto.class));
        assertEquals(YesNoAuto.AUTO, JacksonUtils.getDefaultObjectMapper().readValue("\"\"", YesNoAuto.class));
        assertEquals(YesNoAuto.AUTO, JacksonUtils.getDefaultObjectMapper().readValue("\"auto\"", YesNoAuto.class));
    }
}