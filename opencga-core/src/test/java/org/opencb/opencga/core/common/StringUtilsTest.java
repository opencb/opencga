package org.opencb.opencga.core.common;

import org.junit.Test;
import org.opencb.commons.test.GenericTest;

import java.io.DataOutputStream;
import java.io.FileOutputStream;

public class StringUtilsTest extends GenericTest {

    @Test
    public void testRandomString() throws Exception {
        DataOutputStream os = new DataOutputStream(new FileOutputStream("/tmp/tokens"));

        for (int i = 0; i < 1000000; i++) {
            os.writeChars(StringUtils.randomString(10) + "\n");
        }
        os.flush();
        os.close();
    }
}