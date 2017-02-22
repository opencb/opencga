package org.opencb.opencga.storage.core.variant.io;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.*;

/**
 * Created on 07/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantWriterFactoryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkOutputTest() throws Exception {
        assertEquals("path/myFile.json.gz", VariantWriterFactory.checkOutput("path/myFile", JSON_GZ));
        assertEquals("path/myFile.json.gz", VariantWriterFactory.checkOutput("path/myFile.json", JSON_GZ));
        assertEquals("path/myFile.json.gz", VariantWriterFactory.checkOutput("path/myFile.json.gz", JSON_GZ));
        assertEquals("path/myFile.json.gz", VariantWriterFactory.checkOutput("path/myFile.json.gz.", JSON_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile.", STATS_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile", STATS_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile.stats", STATS_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile.stats.", STATS_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile.stats.tsv", STATS_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile.stats.tsv.gz", STATS_GZ));
    }

    @Test
    public void checkBadOutputTest() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        VariantWriterFactory.checkOutput("path/", JSON_GZ);
    }
}