package org.opencb.opencga.storage.core.variant.index.sample.schema;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.OriginalCall;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.thirdparty.hbase.util.Bytes;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class FileDataSchemaTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testOriginalCallConverter() {
        testOriginalCallEncoding(new Variant("1:12345678:T:-"), new OriginalCall("1:12345678:TC:C", 0));
        testOriginalCallEncoding(new Variant("1:12000078:T:-"), new OriginalCall("1:12000078:TC:C", 0));
        testOriginalCallEncoding(new Variant("1:12345678:T:-"), new OriginalCall("1:12345678:TC:C,T", 2));
        testOriginalCallEncoding(new Variant("1:12345670:-:T"), new OriginalCall("1:12345678:TTTTTTTTTTTTTTTTTTC:C,TC", 2));
        testOriginalCallEncoding(new Variant("1:12345679:-:T"), new OriginalCall("1:12345670:CTTTTTTTTTTTTTTTTTT:C,CT", 2));
        testOriginalCallEncoding(new Variant("1:12345679:CCCTCCTCTGAGTCTTCCTCCCCTTCCCGTG:-"), new OriginalCall("1:12345670:ACCCTCCTCTGAGTCTTCCTCCCCTTCCCGTG:A", 2));
        testOriginalCallEncoding(new Variant("1:12345601-12345625:C:<DEL>"), new OriginalCall("1:12345600-12345625:A:<DEL>", 2));
        testOriginalCallEncoding(new Variant("1:12345679:CCCTCCTCTGAGTCTTCCTCCCCTTCCCGTG:-"), new OriginalCall("1:12345670:ACCCTCCTCTGAGTCTTCCTCCCCTTCCCGTG:A]chr1:1234]", 2));
        testOriginalCallEncoding(new Variant("1:12345679:CCCTCCTCTGAGTCTTCCTCCCCTTCCCGTG:-"), new OriginalCall("chr1:12345670:ACCCTCCTCTGAGTCTTCCTCCCCTTCCCGTG:A", 2));
    }

    private static void testOriginalCallEncoding(Variant variant, OriginalCall expected) {
        FileDataSchema.VariantOriginalCallToBytesConverter cpair = new FileDataSchema.VariantOriginalCallToBytesConverter();

        Pair<Variant, ByteBuffer> pair = cpair.to(Pair.of(variant, expected));
        System.out.println("Bytes2 length : " + pair.getValue().limit());
        System.out.println("bytes2  = " + pair.getValue() + " - " + Bytes.toStringBinary(pair.getValue()));
        OriginalCall actualFromPair = cpair.from(pair).getValue();
        System.out.println(actualFromPair);
        System.out.println("----");
        assertEquals(expected, actualFromPair);

    }


}