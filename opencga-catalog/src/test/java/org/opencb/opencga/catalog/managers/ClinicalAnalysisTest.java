package org.opencb.opencga.catalog.managers;

import org.apache.commons.io.FileUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ClinicalAnalysisTest {

    private String sessionIdUser;
    private CatalogManager catalogManager;

    private File testFile;

    private String study;
    private Path clinicalAnalysesDir;

    @Before
    public void init() throws CatalogException, IOException {
        // line #1: username
        // line #2: password
        // line #3: study
        // line #4: path to the OpenCGA configuration file
        // line #5: path to the folder where the clinical analyses to load are located at
        testFile = Paths.get("/tmp/load-clinical-analyses.conf").toFile();
        Assume.assumeTrue(testFile.exists());

        List<String> lines = FileUtils.readLines(testFile);
        String user = lines.get(0);
        String pass = lines.get(1);
        study = lines.get(2);
        String confPath = lines.get(3);
        clinicalAnalysesDir = Paths.get(lines.get(4));

        Configuration configuration = Configuration.load(new FileInputStream(confPath));
        catalogManager = new CatalogManager(configuration);
        sessionIdUser = catalogManager.getUserManager().login(user, pass).getToken();
    }

    @Test
    public void loadClinicalAnalysesTest() throws CatalogException, IOException {
        Assume.assumeTrue(clinicalAnalysesDir.toFile().exists());
        for (File file : clinicalAnalysesDir.toFile().listFiles()) {
            System.out.println("Loading clinical analyses file: " + file.getAbsolutePath() + " ....");
            int numLoaded = catalogManager.getClinicalAnalysisManager().load(study, file.toPath(), sessionIdUser);
            System.out.println("\t\t.... " + numLoaded + " clinical analyses loaded from file " + file.getAbsolutePath());
        }
    }
}