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

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;

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
}