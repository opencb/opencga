package org.opencb.opencga.core.common;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.*;

/**
 * Created on 03/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
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

        assertEquals("", UriUtils.fileName(URI.create("dir/")));
        assertEquals("", UriUtils.fileName(URI.create("/dir/")));
        assertEquals("", UriUtils.fileName(URI.create("file:///dir/")));
    }
}