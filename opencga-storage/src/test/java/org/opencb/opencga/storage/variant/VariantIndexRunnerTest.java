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
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class VariantIndexRunnerTest extends GenericTest {

    private String inputFile = getClass().getResource("/variant-test-file.vcf.gz").getFile();
    private String pedFile = getClass().getResource("/pedigree-test-file.ped").getFile();
    private String outputFile = "/tmp/sqliteIndexTest.db";

    @Test
    public void sqliteIndex() throws IOException {


        VariantStudy study = new VariantStudy("study1", "s1", "Study 1", Arrays.asList("Alejandro", "Cristina"), Arrays.asList(inputFile, pedFile));

        VariantDataReader reader = new VariantVcfDataReader(inputFile);
        VariantDBWriter writer = new VariantVcfSqliteWriter(outputFile);

        VariantIndexRunner runner = new VariantIndexRunner(study, reader, null, writer);

        runner.run();

    }
}
