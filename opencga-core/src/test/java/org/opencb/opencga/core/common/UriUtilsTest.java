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
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

/**
 * Created on 03/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(ShortTests.class)
public class UriUtilsTest {


    @Test
    public void testDirName() {
        assertEquals("dir/", UriUtils.dirName(URI.create("file://other/dir/file")));
        assertEquals("dir/", UriUtils.dirName(URI.create("/dir/file")));
        assertEquals("dir/", UriUtils.dirName(URI.create("dir/file")));
        assertEquals("/", UriUtils.dirName(URI.create("file:///file")));
        assertEquals("/", UriUtils.dirName(URI.create("/file")));
        assertEquals("/", UriUtils.dirName(URI.create("file")));
    }

    @Test
    public void testFileName() {
        assertEquals("file", UriUtils.fileName(URI.create("file:///dir/file")));
        assertEquals("file", UriUtils.fileName(URI.create("/dir/file")));
        assertEquals("file", UriUtils.fileName(URI.create("dir/file")));
        assertEquals("file", UriUtils.fileName(URI.create("file:///file")));
        assertEquals("file", UriUtils.fileName(URI.create("/file")));
        assertEquals("file", UriUtils.fileName(URI.create("file")));
        assertEquals("file#2", UriUtils.fileName(UriUtils.createUriSafe("s3:///other/dir/file%232")));
        assertEquals("file%3A2", UriUtils.fileName(UriUtils.createUriSafe("/other/dir/file%3A2")));

        assertEquals("", UriUtils.fileName(URI.create("dir/")));
        assertEquals("", UriUtils.fileName(URI.create("/dir/")));
        assertEquals("", UriUtils.fileName(URI.create("file:///dir/")));
    }

    @Test
    public void testBuildUri() throws URISyntaxException {
        // Do not escape anything
        assertEquals("file:///other/dir/file", UriUtils.createUri("file:///other/dir/file").toString());
        assertEquals("file:///other/dir/file:2", UriUtils.createUri("file:///other/dir/file:2").toString());
        assertEquals("http:///other/dir/file%3A2", UriUtils.createUri("http:///other/dir/file%3A2").toString());
        assertEquals("my.schema:///other/dir/file%3A2", UriUtils.createUri("my.schema:///other/dir/file%3A2").toString());
        assertEquals("s3:///other/dir/file%232", UriUtils.createUri("s3:///other/dir/file%232").toString());

        // Escape special characters
        // %
        assertEquals("file:///other/dir/file%253A2", UriUtils.createUri("/other/dir/file%3A2").toString());
        // ?
        assertEquals("file:///other/dir/file%3F2", UriUtils.createUri("/other/dir/file?2").toString());
        // #
        assertEquals("file:///other/dir/file%232", UriUtils.createUri("/other/dir/file#2").toString());
    }
}