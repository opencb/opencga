package org.opencb.opencga.storage.core.variant.search;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;

import static org.junit.Assert.*;

public class VariantSearchToVariantConverterTest {

    private VariantSearchToVariantConverter converter;

    @Before
    public void setUp() throws Exception {
        converter = new VariantSearchToVariantConverter();
    }

    @Test
    public void test() throws Exception {
        Variant expectedVariant = Variant.newBuilder("chr1:1000:A:T")
                .setStudyId("1")
                .setFileId("5")
                .setFormat("GT", "AD", "DP")
                .addSample("S1", "0/0", "10,1", "11")
                .addSample("S2", "1/1", "1,2", "1")
                .setFilter("PASS")
                .setQuality(200.0)
                .addAttribute("k1", "v1")
                .build();

        Variant aux = Variant.newBuilder("chr1:1000:A:T")
                .setStudyId("2")
                .setFileId("6")
                .setFormat("GT", "AD", "DP", "AN")
                .addSample("S3", "1/0", "12,2", "22", "dole-se")
                .setFilter("PASS:Low")
                .setQuality(210.0)
                .addAttribute("k2", "v2")
                .addAttribute("k22", "v22")
                .build();

        expectedVariant.addStudyEntry(aux.getStudy("2"));

        VariantSearchModel variantSearchModel = converter.convertToStorageType(expectedVariant);
        System.out.println("variantSearchModel = " + variantSearchModel);

        Variant actualVariant = converter.convertToDataModelType(variantSearchModel);

        System.out.println(expectedVariant.toJson());
        System.out.println(actualVariant.toJson());

        assertEquals(expectedVariant.getStudies(), actualVariant.getStudies());
    }
}