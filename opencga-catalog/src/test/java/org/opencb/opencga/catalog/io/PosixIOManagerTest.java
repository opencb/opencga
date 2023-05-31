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

package org.opencb.opencga.catalog.io;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.file.FileContent;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(ShortTests.class)
public class PosixIOManagerTest {

    private static PosixIOManager posixIOManager;
    private static Path tmpOutdir;

    @BeforeClass
    public static void beforeClass() {
        tmpOutdir = Paths.get("target", "test-data", "opencga-" + PosixIOManagerTest.class.getSimpleName() + "-" + TimeUtils.getTimeMillis(),
                "sessions");
    }

    @Before
    public void before() throws Exception {
        if (Files.exists(tmpOutdir)) {
            IOUtils.deleteDirectory(tmpOutdir);
        }
        Files.createDirectories(tmpOutdir);
        posixIOManager = new PosixIOManager();
    }

    @Test
    public void testHead() throws Exception {
        Path path = tmpOutdir.resolve("20130606_g1k.ped");
        FileUtils.copyInputStreamToFile(this.getClass().getClassLoader().getResource("20130606_g1k.ped").openStream(), path.toFile());

        System.out.println(posixIOManager.head(path, 0, 10).getContent());
    }

    @Test(timeout = 10000)
    public void testTail() throws Exception {
        Path path = tmpOutdir.resolve("file_large_lines.txt");
        List<String> expected = new LinkedList<>();
        try (DataOutputStream os = new DataOutputStream(new FileOutputStream(path.toFile()))) {
            for (int i = 0; i < 20; i++) {
                String line = i + " -- " + StringUtils.repeat("A", 10000);
                expected.add(line);
                os.writeBytes(line);
                os.writeBytes("\n");
            }
        }

        List<String> actual = Arrays.asList(posixIOManager.tail(path, 10).getContent().split("\n"));
        assertEquals(expected.subList(expected.size() - 10, expected.size()), actual);

        actual = Arrays.asList(posixIOManager.tail(path, expected.size() * 2).getContent().split("\n"));

        assertEquals(expected, actual);
    }

    @Test(timeout = 10000)
    public void testTailSmall() throws Exception {
        Path path = tmpOutdir.resolve("small_file.txt");
        FileUtils.write(path.toFile(), "hello world!", Charset.defaultCharset());

        List<String> actual = Arrays.asList(posixIOManager.tail(path, 10).getContent().split("\n"));
        assertEquals(Collections.singletonList("hello world!"), actual);
    }

    @Test
    public void testGrep() throws Exception {
        Path path = tmpOutdir.resolve("20130606_g1k.ped");
        FileUtils.copyInputStreamToFile(this.getClass().getClassLoader().getResource("20130606_g1k.ped").openStream(), path.toFile());

        FileContent fileContent = posixIOManager.grep(path, "hG01880", 0, false);
        assertTrue(StringUtils.isEmpty(fileContent.getContent()));

        fileContent = posixIOManager.grep(path, "hG01880", 0, true);
        assertTrue(StringUtils.isNotEmpty(fileContent.getContent()));

        FileContent grep = posixIOManager.grep(path, "HG01880", 0, true);
        System.out.println(grep.getContent());
    }

}