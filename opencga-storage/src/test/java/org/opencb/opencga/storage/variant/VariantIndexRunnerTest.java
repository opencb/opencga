package org.opencb.opencga.storage.variant;

import org.junit.Test;
import org.opencb.commons.bioformats.variant.VariantSource;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantReader;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantVcfReader;
import org.opencb.commons.bioformats.variant.vcf4.io.writers.VariantWriter;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.SqliteCredentials;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class VariantIndexRunnerTest extends GenericTest {

    private String inputFile = getClass().getResource("/variant-test-file.vcf.gz").getFile();
    private String pedFile = getClass().getResource("/pedigree-test-file.ped").getFile();
    private String outputFile = "/tmp/sqliteIndexTest.db";
    private VariantSource study = new VariantSource("study1", "s1", "Study 1", Arrays.asList("Alejandro", "Cristina"), Arrays.asList(inputFile, pedFile));

    @Test
    public void sqliteIndex() throws IOException {

        VariantReader reader = new VariantVcfReader(inputFile);
        VariantWriter writer = new VariantVcfSqliteWriter(outputFile);

//        VariantIndexRunner runner = new VariantIndexRunner(study, reader, null, writer);

//        runner.run();

    }

    @Test
    public void sqliteIndexWithCredentials() throws IllegalOpenCGACredentialsException, IOException {

        VariantSource study = new VariantSource("study1", "s1", "Study 1", Arrays.asList("Alejandro", "Cristina"), Arrays.asList(inputFile, pedFile));

        Path path = Paths.get(outputFile);

        SqliteCredentials credentials = new SqliteCredentials(path);

        VariantReader reader = new VariantVcfReader(inputFile);
        VariantWriter writer = new VariantVcfSqliteWriter(credentials);

//        VariantIndexRunner runner = new VariantIndexRunner(study, reader, null, writer);
//
//        runner.run();
    }

    @Test(expected = IllegalOpenCGACredentialsException.class)
    public void sqliteIndexWithCredentialsFail() throws IllegalOpenCGACredentialsException, IOException {

        SqliteCredentials credentials = new SqliteCredentials(Paths.get("/tamp/file.test"));

    }

}
