/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.alignment.adaptors;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageOptions;
import org.opencb.opencga.storage.core.alignment.local.LocalAlignmentStorageEngine;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

/**
 * Created by pfurio on 26/10/16.
 */
public class DefaultAlignmentDBAdaptorTest {

    private static Path rootDir = null;

    public static Path getTmpRootDir() throws IOException {
        if (rootDir == null) {
            newRootDir();
        }
        return rootDir;
    }

    private static void newRootDir() throws IOException {
        rootDir = Paths.get("target/test-data", "junit-opencga-storage-" + TimeUtils.getTimeMillis() + "_"
                + RandomStringUtils.randomAlphabetic(3));
        Files.createDirectories(rootDir);
    }

    @Test
    public void index() throws Exception {
        URI resource = getClass().getResource("/HG00096.chrom20.small.bam").toURI();

        Path tmpRootDir = getTmpRootDir();
        File inputFile = tmpRootDir.resolve("HG00096.chrom20.small.bam").toFile();
        FileOutputStream fileOutputStream = new FileOutputStream(inputFile);
        Files.copy(Paths.get(resource.getPath()), fileOutputStream);

        AlignmentStorageEngine defaultAlignmentStorageManager = new LocalAlignmentStorageEngine();
        InputStream is = getClass().getClassLoader().getResourceAsStream("storage-configuration.yml");
        StorageConfiguration storageConfiguration = StorageConfiguration.load(is);
        storageConfiguration.getAlignment().put(AlignmentStorageOptions.BIG_WIG_WINDOWS_SIZE.key(), 1000);
        defaultAlignmentStorageManager.setConfiguration(storageConfiguration, "", "");

        defaultAlignmentStorageManager.index(Arrays.asList(inputFile.toURI()), getTmpRootDir().toUri(), true, true, true);

        assertTrue(Files.exists(Paths.get(inputFile.getPath())));
        assertTrue(Files.exists(tmpRootDir.resolve("HG00096.chrom20.small.bam.bai")));
        assertTrue(Files.exists(tmpRootDir.resolve("HG00096.chrom20.small.bam.stats")));
        assertTrue(Files.exists(tmpRootDir.resolve("HG00096.chrom20.small.bam.bw")));
    }

    //    @Test
//    public void iterator() throws Exception {
//        String inputPath = getClass().getResource("/HG00096.chrom20.small.bam").getPath();
//        AlignmentStorageEngine defaultAlignmentStorageManager =
//                new AlignmentStorageEngine(null, null, new StorageConfiguration(), Paths.get("/tmp"));
//        AlignmentIterator iterator = defaultAlignmentStorageManager.getDBAdaptor().iterator(inputPath);
//        while (iterator.hasNext()) {
//            System.out.println(iterator.next().toString());
//        }
//    }

}