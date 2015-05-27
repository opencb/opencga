package org.opencb.opencga.analysis.files;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.StorageManagerException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

public class FileMetadataReaderTest extends TestCase {

    public static final String PASSWORD = "asdf";
    private CatalogManager catalogManager;
    private String sessionIdUser;
    private Project project;
    private Study study;
    private File folder;
    private URI vcfFileUri;
    public static final String VCF_FILE_NAME = "1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz";
    public static final String BAM_FILE_NAME = "HG00096.chrom20.small.bam";
    private URI bamFileUri;

    @Before
    public void setUp() throws IOException, CatalogException {
        InputStream is = CatalogManagerTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);

        CatalogManagerTest.clearCatalog(properties);

        catalogManager = new CatalogManager(properties);

        catalogManager.createUser("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null);
        sessionIdUser = catalogManager.login("user", PASSWORD, "127.0.0.1").first().getString("sessionId");
        project = catalogManager.createProject("user", "Project about some genomes", "1000G", "", "ACME", null, sessionIdUser).first();
        study = catalogManager.createStudy(project.getId(), "Phase 1", "phase1", Study.Type.TRIO, "Done", sessionIdUser).first();
        folder = catalogManager.createFolder(study.getId(), Paths.get("data/vcf/"), true, null, sessionIdUser).first();
        Path vcfPath = Paths.get("/tmp", VCF_FILE_NAME);
        Files.copy(this.getClass().getClassLoader().getResourceAsStream(VCF_FILE_NAME), vcfPath, StandardCopyOption.REPLACE_EXISTING);
        vcfFileUri = vcfPath.toUri();

        Path bamPath = Paths.get("/tmp", BAM_FILE_NAME);
        Files.copy(this.getClass().getClassLoader().getResourceAsStream(BAM_FILE_NAME), bamPath, StandardCopyOption.REPLACE_EXISTING);
        bamFileUri = bamPath.toUri();
    }

    @Test
    public void testCreate() throws CatalogException {
        QueryResult<File> fileQueryResult = FileMetadataReader.get(catalogManager).
                create(study.getId(), vcfFileUri, folder.getPath() + VCF_FILE_NAME, "", false, null, sessionIdUser);

        File file = fileQueryResult.first();

        assertEquals(File.Status.STAGE, file.getStatus());
        assertEquals(File.Format.VCF, file.getFormat());
        assertEquals(File.Bioformat.VARIANT, file.getBioformat());
        assertNotNull(file.getAttributes().get("variantSource"));
        assertEquals(2504, file.getSampleIds().size());
        assertEquals(0, file.getDiskUsage());

        new CatalogFileUtils(catalogManager).upload(vcfFileUri, file, null, sessionIdUser, false, false, true, true, Integer.MAX_VALUE);
        file = catalogManager.getFile(file.getId(), sessionIdUser).first();

        assertEquals(File.Status.READY, file.getStatus());
        assertEquals(File.Format.VCF, file.getFormat());
        assertEquals(File.Bioformat.VARIANT, file.getBioformat());
        assertNotNull(file.getAttributes().get("variantSource"));
        assertEquals(2504, file.getSampleIds().size());
        assertTrue(file.getDiskUsage() > 0);
    }

    @Test
    public void testGetMetadataVcf()
            throws CatalogException, StorageManagerException {
        QueryResult<File> fileQueryResult = catalogManager.createFile(study.getId(), File.Format.PLAIN,
                File.Bioformat.NONE, folder.getPath() + VCF_FILE_NAME, "", false, -1, sessionIdUser);

        File file = fileQueryResult.first();

        assertEquals(File.Status.STAGE, file.getStatus());
        assertEquals(File.Format.PLAIN, file.getFormat());
        assertEquals(File.Bioformat.NONE, file.getBioformat());
        assertNull(file.getAttributes().get("variantSource"));
        assertEquals(0, file.getSampleIds().size());
        assertEquals(0, file.getDiskUsage());

        new CatalogFileUtils(catalogManager).
                upload(vcfFileUri, file, null, sessionIdUser, false, false, true, true, Integer.MAX_VALUE);
        file = FileMetadataReader.get(catalogManager).
                setMetadataInformation(file, null, null, sessionIdUser, false);

        assertEquals(File.Status.READY, file.getStatus());
        assertEquals(File.Format.VCF, file.getFormat());
        assertEquals(File.Bioformat.VARIANT, file.getBioformat());
        assertNotNull(file.getAttributes().get("variantSource"));
        assertEquals(2504, file.getSampleIds().size());
        assertTrue(file.getDiskUsage() > 0);
    }

    @Test
    public void testGetMetadata()
            throws CatalogException, StorageManagerException {
        QueryResult<File> fileQueryResult = catalogManager.createFile(study.getId(), File.Format.PLAIN,
                File.Bioformat.NONE, folder.getPath() + BAM_FILE_NAME, "", false, -1, sessionIdUser);

        File file = fileQueryResult.first();

        assertEquals(File.Status.STAGE, file.getStatus());
        assertEquals(File.Format.PLAIN, file.getFormat());
        assertEquals(File.Bioformat.NONE, file.getBioformat());
        assertNull(file.getAttributes().get("variantSource"));
        assertEquals(0, file.getSampleIds().size());
        assertEquals(0, file.getDiskUsage());

        new CatalogFileUtils(catalogManager).
                upload(bamFileUri, file, null, sessionIdUser, false, false, true, true, Integer.MAX_VALUE);
        file = FileMetadataReader.get(catalogManager).
                setMetadataInformation(file, null, null, sessionIdUser, false);

        assertEquals(File.Status.READY, file.getStatus());
//        assertEquals(File.Format.GZIP, file.getFormat());
        assertEquals(File.Bioformat.ALIGNMENT, file.getBioformat());
        assertNotNull(file.getAttributes().get("alignmentHeader"));
        assertEquals(1, file.getSampleIds().size());
        assertTrue(file.getDiskUsage() > 0);
        assertEquals("HG00096", catalogManager.getSample(file.getSampleIds().get(0), null, sessionIdUser).first().getName());
    }


}