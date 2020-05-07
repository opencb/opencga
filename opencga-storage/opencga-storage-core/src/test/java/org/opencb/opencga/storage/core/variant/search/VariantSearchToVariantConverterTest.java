package org.opencb.opencga.storage.core.variant.search;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantScore;

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
                .setSampleDataKeys("GT", "AD", "DP")
                .addSample("S1", "0/0", "10,1", "11")
                .addSample("S2", "1/1", "1,2", "1")
                .setFilter("PASS")
                .setQuality(200.0)
                .addFileData("k1", "v1")
                .build();
        expectedVariant.getStudies().get(0).getScores().add(new VariantScore("gwas1", "A", null, 3.4f, 0.002f));
        expectedVariant.getStudies().get(0).getScores().add(new VariantScore("gwas2", "A", "B", 1.23f, 0.1f));

        Variant aux = Variant.newBuilder("chr1:1000:A:T")
                .setStudyId("2")
                .setFileId("6")
                .setSampleDataKeys("GT", "AD", "DP", "AN")
                .addSample("S3", "1/0", "12,2", "22", "dole-se")
                .setFilter("PASS:Low")
                .setQuality(210.0)
                .addFileData("k2", "v2")
                .addFileData("k22", "v22")
                .build();

        expectedVariant.addStudyEntry(aux.getStudy("2"));

        VariantSearchModel variantSearchModel = converter.convertToStorageType(expectedVariant);
        System.out.println("variantSearchModel = " + variantSearchModel);

        Variant actualVariant = converter.convertToDataModelType(variantSearchModel);

        System.out.println();
        System.out.println(expectedVariant.toJson());
        System.out.println();
        System.out.println(actualVariant.toJson());

        assertEquals(expectedVariant.getStudies(), actualVariant.getStudies());
    }
}