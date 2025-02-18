/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.common;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.*;
import java.nio.file.Paths;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@Category(ShortTests.class)
public class IOUtilsTest {

    private static String inputFile = IOUtilsTest.class.getResource("/file.txt").getFile();

    @Test
    public void testHeadOffset() throws Exception {

        InputStream is = IOUtils.headOffset(Paths.get(inputFile), 0, 1);
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            System.out.println(inputLine);
        }
        in.close();

    }
    @Test
    public void testHeadOffset2() throws Exception {

        InputStream is = IOUtils.headOffset(Paths.get(inputFile), 2, 4);
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            System.out.println(inputLine);
        }
        in.close();

    }

    @Test
    public void testGrepFile() throws Exception {

//        InputStream is = IOUtils.grepFile(Paths.get(inputFile), "^#a.*", false, false);
        InputStream is = IOUtils.grepFile(Paths.get(inputFile), ".*", false, true);
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            System.out.println(inputLine);
        }
        in.close();

    }

    @Test
    public void fromHumanReadableToByte_ValidInput_ReturnsCorrectBytes() {
        assertEquals(1024, IOUtils.fromHumanReadableToByte("1Ki"));
        assertEquals(1024, IOUtils.fromHumanReadableToByte("1KiB"));
        assertEquals(1024, IOUtils.fromHumanReadableToByte("1.KiB"));
        assertEquals(1024, IOUtils.fromHumanReadableToByte("1.0KiB"));
        assertEquals(1024, IOUtils.fromHumanReadableToByte("1.0K", true));
        assertEquals(1024, IOUtils.fromHumanReadableToByte("1K", true));
        assertEquals(1024, IOUtils.fromHumanReadableToByte("1KB", true));

        assertEquals(1000, IOUtils.fromHumanReadableToByte("1K"));
        assertEquals(1000, IOUtils.fromHumanReadableToByte("1KB"));
    }

    @Test
    public void fromHumanReadableToByte_ValidInputWithBinaryUnits_ReturnsCorrectBytes() {
        assertEquals(1048576, IOUtils.fromHumanReadableToByte("1Mi"));
        assertEquals(1073741824, IOUtils.fromHumanReadableToByte("1Gi"));
        assertEquals(1099511627776L, IOUtils.fromHumanReadableToByte("1Ti"));
    }

    @Test
    public void fromHumanReadableToByte_InvalidInput_ThrowsNumberFormatException() {
        assertThrows(NumberFormatException.class, () -> IOUtils.fromHumanReadableToByte("1X"));
    }

    @Test
    public void fromHumanReadableToByte_NullInput_ThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> IOUtils.fromHumanReadableToByte(null));
    }
}