package org.opencb.opencga.storage.variant;

import org.junit.Test;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.vcf4.io.VariantDBWriter;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantDataReader;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantVcfDataReader;
import org.opencb.commons.test.GenericTest;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by aaleman on 12/9/13.
 */
public class VariantIndexRunnerTest extends GenericTest {

    private String inputFile = "/home/aaleman/Documents/pruebas/index/small.vcf";
    private String outputFile = "/home/aaleman/Documents/pruebas/index/index.db";
    private String pedFile = "/home/aaleman/tmp/file.ped";

    @Test
    public void sqliteIndex() throws IOException {

        VariantStudy study = new VariantStudy("study1", "s1", "Study 1", Arrays.asList("Alejandro", "Cristina"), Arrays.asList(inputFile, pedFile));
        VariantDataReader reader = new VariantVcfDataReader(inputFile);
        VariantDBWriter writer = new VariantVcfSqliteWriter(outputFile);

        VariantIndexRunner runner = new VariantIndexRunner(study, reader, null, writer);

        runner.run();

    }
}
