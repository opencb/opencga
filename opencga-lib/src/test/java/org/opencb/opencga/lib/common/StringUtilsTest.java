package org.opencb.opencga.lib.common;

import com.google.common.primitives.Bytes;
import org.junit.Test;
import org.opencb.commons.test.GenericTest;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;

import static org.junit.Assert.*;

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